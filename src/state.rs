use std::collections::{BTreeSet, HashMap};

use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::ids::{GroupId, UnitId};
use crate::proto;

/// Number of buffered messages per update stream before a slow subscriber
/// starts missing them. A subscriber that lags is aborted rather than
/// silently skipped, since the server can no longer tell it how much it
/// missed -- so this bounds how much a brief stall costs it (a resubscribe),
/// not whether it stays correct.
///
/// Sized off `benches/state.rs`'s `state_cache_group_updates_fanout`: an
/// unstaggered 50-group/12-unit-each tick (every group changed, so every one
/// broadcasts -- the worst case, since Arma actually staggers pushes across
/// the second) is ~50 events and completes in low single-digit milliseconds
/// even with 64 concurrently draining subscribers. 256 covers several such
/// ticks of complete stall -- roughly a couple of seconds of the slowest
/// subscriber not being polled at all -- before it's aborted instead of
/// silently missing data.
const BROADCAST_CAPACITY: usize = 256;

/// A broadcast group update, tagged with the cache's sequence number as of
/// the write that produced it. Internal only (never serialized) -- it's how
/// the service layer tells a fresh subscriber's snapshot apart from updates
/// that arrived after it, without a lock spanning both (see
/// `subscribe_group_updates`'s doc).
#[derive(Clone)]
pub(crate) struct GroupEvent {
    pub(crate) sequence: u64,
    pub(crate) response: proto::SubscribeGroupUpdatesResponse,
}

#[derive(Clone)]
pub(crate) struct SimulationEvent {
    pub(crate) sequence: u64,
    pub(crate) response: proto::SubscribeSimulationUpdatesResponse,
}

#[derive(Default)]
struct Inner {
    units: HashMap<UnitId, proto::Unit>,
    groups: HashMap<GroupId, proto::Group>,
    /// Which units currently belong to each group. Derived from `units`'
    /// `group_id` field and kept in sync on every write that touches
    /// membership -- it can always be rebuilt by scanning `units`, so it
    /// can't disagree with them, only fall behind if a write forgets to
    /// update it (every write below does). A `BTreeSet` so a join iterates
    /// in a deterministic (id-sorted) order.
    members: HashMap<GroupId, BTreeSet<UnitId>>,
    simulation_info: Option<proto::SimulationInfo>,
    simulation_state: Option<proto::SimulationStateUpdate>,
    locations: Vec<proto::Location>,
    /// Bumped on every group/simulation-state mutation (not units/info/
    /// locations, which aren't subscribable). See `GroupEvent`.
    sequence: u64,
}

impl Inner {
    fn next_sequence(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    /// The group's stored fields joined with its current members, or `None`
    /// if the group isn't cached (regardless of whether it has members).
    fn joined_group(&self, id: &GroupId) -> Option<proto::Group> {
        let mut group = self.groups.get(id)?.clone();
        group.units = self.member_units(id);
        Some(group)
    }

    fn member_units(&self, id: &GroupId) -> Vec<proto::Unit> {
        self.members
            .get(id)
            .into_iter()
            .flatten()
            .filter_map(|unit_id| self.units.get(unit_id))
            .cloned()
            .collect()
    }

    /// Detaches `unit_id` from `group_id`'s member set, dropping the set
    /// entirely once it's empty so an emptied-out group doesn't leak a
    /// forever-empty entry.
    fn unlink_member(&mut self, group_id: &GroupId, unit_id: &UnitId) {
        if let Some(members) = self.members.get_mut(group_id) {
            members.remove(unit_id);
            if members.is_empty() {
                self.members.remove(group_id);
            }
        }
    }
}

/// Normalized, in-memory snapshot of simulation state, fed by the engine
/// thread and read by the gRPC services. `Unit.group_id` is the only place
/// group membership is recorded; `Inner::members` is a derived index over
/// it, kept in sync by every write below, not a second source of truth.
///
/// Every broadcast is sent while still holding the write lock that produced
/// it (`broadcast::Sender::send` never blocks or runs foreign code), so
/// subscribers see events in exactly the order their mutations committed --
/// e.g. a `clear()` removal can't overtake a concurrent re-upsert.
pub struct StateCache {
    inner: RwLock<Inner>,
    simulation_updates: broadcast::Sender<SimulationEvent>,
    group_updates: broadcast::Sender<GroupEvent>,
}

impl Default for StateCache {
    fn default() -> Self {
        Self::new()
    }
}

impl StateCache {
    pub fn new() -> Self {
        let (simulation_updates, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (group_updates, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            inner: RwLock::new(Inner::default()),
            simulation_updates,
            group_updates,
        }
    }

    // --- writes (engine thread) ---

    /// Upserts a single unit without otherwise touching group membership.
    /// Re-broadcasts its (new) group, and its previous group if this moved
    /// it away from one, since both groups' joined views just changed.
    pub fn upsert_unit(&self, unit: proto::Unit) {
        let mut inner = self.inner.write();
        let id = UnitId::from(unit.id.as_str());
        let new_group = GroupId::from(unit.group_id.as_str());
        let old_group = inner
            .units
            .get(&id)
            .map(|existing| GroupId::from(existing.group_id.as_str()));

        let before_new = inner.joined_group(&new_group);
        let before_old = old_group
            .as_ref()
            .filter(|old| **old != new_group)
            .map(|old| (old.clone(), inner.joined_group(old)));

        if let Some((old, _)) = &before_old {
            inner.unlink_member(old, &id);
        }
        inner
            .members
            .entry(new_group.clone())
            .or_default()
            .insert(id.clone());
        inner.units.insert(id, unit);

        self.emit_if_changed(&mut inner, &new_group, before_new);
        if let Some((old, before)) = before_old {
            self.emit_if_changed(&mut inner, &old, before);
        }
    }

    /// Removes a unit and re-broadcasts the group it belonged to, since its
    /// joined view just lost a member.
    pub fn remove_unit(&self, id: &UnitId) -> Option<proto::Unit> {
        let mut inner = self.inner.write();
        // `before` must be captured while the unit is still in `units`:
        // `joined_group` derives membership from that map, so computing it
        // after removal would already reflect the unit gone on both sides
        // of the comparison, and emit_if_changed would see no change and
        // never tell subscribers it left.
        let group_id = inner
            .units
            .get(id)
            .map(|unit| GroupId::from(unit.group_id.as_str()));
        let before = group_id.as_ref().and_then(|id| inner.joined_group(id));
        let removed = inner.units.remove(id);
        if let Some(group_id) = group_id {
            inner.unlink_member(&group_id, id);
            self.emit_if_changed(&mut inner, &group_id, before);
        }
        removed
    }

    /// Upserts a group's own fields (side/readiness/task/waypoints) without
    /// touching its membership. `group.units` is ignored -- use
    /// [`Self::upsert_group_with_units`] to update both together.
    ///
    /// If `group` changes side relative to what was cached, first broadcasts
    /// a removal for the old id: a subscriber filtered to the old side has
    /// no other way to learn this group no longer matches its filter.
    pub fn upsert_group(&self, mut group: proto::Group) {
        group.units = Vec::new();
        let mut inner = self.inner.write();
        let id = GroupId::from(group.id.as_str());
        let before = inner.joined_group(&id);
        self.replace_group_fields(&mut inner, &id, group);
        self.emit_if_changed(&mut inner, &id, before);
    }

    /// Upserts a group's fields and atomically replaces its member set: any
    /// unit previously in this group but not in `units` is dropped, and a
    /// unit listed here that belonged to a different group is moved. Each
    /// unit's `group_id` is set to `group.id` regardless of what it arrived
    /// with -- the server, not the caller, decides membership for a unit
    /// embedded in a group's upsert.
    ///
    /// Broadcasts every group whose joined view changed: this group, plus
    /// any other group that lost a member to it.
    pub fn upsert_group_with_units(&self, mut group: proto::Group, mut units: Vec<proto::Unit>) {
        group.units = Vec::new();
        let mut inner = self.inner.write();
        let group_id = GroupId::from(group.id.as_str());
        for unit in &mut units {
            unit.group_id = group_id.to_string();
        }
        let new_ids: BTreeSet<UnitId> = units
            .iter()
            .map(|unit| UnitId::from(unit.id.as_str()))
            .collect();

        // Snapshot every other group that will lose a member reassigned
        // here, before any mutation, so it can be compared after.
        let mut other_before: HashMap<GroupId, Option<proto::Group>> = HashMap::new();
        for unit in &units {
            let id = UnitId::from(unit.id.as_str());
            if let Some(existing) = inner.units.get(&id) {
                let previous_group = GroupId::from(existing.group_id.as_str());
                if previous_group != group_id {
                    other_before
                        .entry(previous_group.clone())
                        .or_insert_with(|| inner.joined_group(&previous_group));
                }
            }
        }
        let before = inner.joined_group(&group_id);

        // Drop members no longer listed.
        let old_ids = inner.members.get(&group_id).cloned().unwrap_or_default();
        for id in old_ids.difference(&new_ids) {
            inner.units.remove(id);
        }

        // Detach every listed unit from wherever it used to belong (if
        // different) before relinking it here.
        for unit in &units {
            let id = UnitId::from(unit.id.as_str());
            if let Some(existing) = inner.units.get(&id) {
                let previous_group = GroupId::from(existing.group_id.as_str());
                if previous_group != group_id {
                    inner.unlink_member(&previous_group, &id);
                }
            }
        }

        inner.members.insert(group_id.clone(), new_ids);
        for unit in units {
            inner.units.insert(UnitId::from(unit.id.as_str()), unit);
        }

        self.replace_group_fields(&mut inner, &group_id, group);

        self.emit_if_changed(&mut inner, &group_id, before);
        for (other_id, before) in other_before {
            self.emit_if_changed(&mut inner, &other_id, before);
        }
    }

    /// Replaces a group's stored (unit-less) fields, broadcasting a removal
    /// first if this changed its side. Shared by [`Self::upsert_group`] and
    /// [`Self::upsert_group_with_units`]; doesn't broadcast the upsert
    /// itself -- callers do that via [`Self::emit_if_changed`] once their
    /// own membership changes are also applied.
    fn replace_group_fields(&self, inner: &mut Inner, id: &GroupId, group: proto::Group) {
        let previous_side = inner.groups.get(id).map(|existing| existing.side);
        inner.groups.insert(id.clone(), group.clone());
        if previous_side.is_some_and(|previous| previous != group.side) {
            let removal = GroupEvent {
                sequence: inner.next_sequence(),
                response: group_removed(group.id),
            };
            let _ = self.group_updates.send(removal);
        }
    }

    /// Sends a `GroupEvent` for `id` if its joined value now differs from
    /// `before` (its value just prior to the write in progress). `inner`
    /// must already reflect that write.
    fn emit_if_changed(&self, inner: &mut Inner, id: &GroupId, before: Option<proto::Group>) {
        let after = inner.joined_group(id);
        if after == before {
            return;
        }
        let response = match after {
            Some(group) => group_upserted(group),
            None => group_removed(id.to_string()),
        };
        let event = GroupEvent {
            sequence: inner.next_sequence(),
            response,
        };
        let _ = self.group_updates.send(event);
    }

    /// Removes a group and every unit that belonged to it, broadcasting a
    /// single removal. Returns the group as it was just before removal
    /// (including its units), or `None` if it wasn't cached.
    pub fn remove_group(&self, id: &GroupId) -> Option<proto::Group> {
        let mut inner = self.inner.write();
        if !inner.groups.contains_key(id) {
            return None;
        }
        let removed = inner.joined_group(id);
        inner.groups.remove(id);
        if let Some(members) = inner.members.remove(id) {
            for unit_id in members {
                inner.units.remove(&unit_id);
            }
        }
        let event = GroupEvent {
            sequence: inner.next_sequence(),
            response: group_removed(id.to_string()),
        };
        let _ = self.group_updates.send(event);
        removed
    }

    pub fn set_simulation_info(&self, info: proto::SimulationInfo) {
        self.inner.write().simulation_info = Some(info);
    }

    pub fn set_simulation_state(&self, state: proto::SimulationStateUpdate) {
        let mut inner = self.inner.write();
        inner.simulation_state = Some(state);
        let event = SimulationEvent {
            sequence: inner.next_sequence(),
            response: proto::SubscribeSimulationUpdatesResponse { state: Some(state) },
        };
        let _ = self.simulation_updates.send(event);
    }

    pub fn set_locations(&self, locations: Vec<proto::Location>) {
        self.inner.write().locations = locations;
    }

    /// Drops all cached state, broadcasting a removal for every group that
    /// was present and a `state: None` simulation update, so subscribers
    /// don't retain stale entries/time/weather across a reset. Does not
    /// affect existing subscriptions or in-flight commands — see
    /// [`crate::Dispatcher::cancel_all`] for that.
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        let ids: Vec<String> = inner.groups.keys().map(GroupId::to_string).collect();
        // Keep the sequence monotonic: subscribers discard events at or below
        // their snapshot's sequence, so resetting it would silently drop these
        // removals (and every update after them until the counter caught up).
        *inner = Inner {
            sequence: inner.sequence,
            ..Inner::default()
        };
        for id in ids {
            let event = GroupEvent {
                sequence: inner.next_sequence(),
                response: group_removed(id),
            };
            let _ = self.group_updates.send(event);
        }
        let sim_event = SimulationEvent {
            sequence: inner.next_sequence(),
            response: proto::SubscribeSimulationUpdatesResponse { state: None },
        };
        let _ = self.simulation_updates.send(sim_event);
    }

    // --- reads (gRPC services) ---

    pub fn get_unit(&self, id: &UnitId) -> Option<proto::Unit> {
        self.inner.read().units.get(id).cloned()
    }

    pub fn list_units(
        &self,
        side: Option<proto::Side>,
        group_id: Option<&GroupId>,
    ) -> Vec<proto::Unit> {
        let inner = self.inner.read();
        inner
            .units
            .values()
            .filter(|unit| group_id.is_none_or(|id| unit.group_id == id.as_str()))
            .filter(|unit| side.is_none_or(|side| Self::unit_side(&inner, unit) == Some(side)))
            .cloned()
            .collect()
    }

    fn unit_side(inner: &Inner, unit: &proto::Unit) -> Option<proto::Side> {
        inner
            .groups
            .get(&GroupId::from(unit.group_id.as_str()))
            .and_then(|group| proto::Side::try_from(group.side).ok())
    }

    pub fn get_group(&self, id: &GroupId) -> Option<proto::Group> {
        self.inner.read().joined_group(id)
    }

    pub fn list_groups(&self, side: Option<proto::Side>) -> Vec<proto::Group> {
        let inner = self.inner.read();
        inner
            .groups
            .iter()
            .filter(|(_, group)| {
                side.is_none_or(|side| proto::Side::try_from(group.side) == Ok(side))
            })
            .map(|(id, group)| {
                let mut group = group.clone();
                group.units = inner.member_units(id);
                group
            })
            .collect()
    }

    pub fn simulation_info(&self) -> Option<proto::SimulationInfo> {
        self.inner.read().simulation_info.clone()
    }

    pub fn simulation_state(&self) -> Option<proto::SimulationStateUpdate> {
        self.inner.read().simulation_state
    }

    pub fn list_locations(&self, side: Option<proto::Side>) -> Vec<proto::Location> {
        self.inner
            .read()
            .locations
            .iter()
            .filter(|location| {
                side.is_none_or(|side| proto::Side::try_from(location.owner) == Ok(side))
            })
            .cloned()
            .collect()
    }

    // --- subscriptions ---
    //
    // Each of these subscribes *before* reading the snapshot (so an update
    // landing in between isn't missed by both), and returns the snapshot's
    // sequence number alongside it. The service layer discards any received
    // event whose sequence is <= that number: without this, an update that
    // both landed in the just-created receiver's buffer *and* is already
    // reflected in the snapshot (read a moment later) would replay as a
    // spurious, out-of-order duplicate after the snapshot.

    /// Returns the current simulation state (if any), its sequence number,
    /// and a receiver for updates from this point on.
    pub(crate) fn subscribe_simulation_updates(
        &self,
    ) -> (
        Option<proto::SimulationStateUpdate>,
        u64,
        broadcast::Receiver<SimulationEvent>,
    ) {
        let receiver = self.simulation_updates.subscribe();
        let inner = self.inner.read();
        (inner.simulation_state, inner.sequence, receiver)
    }

    /// Returns the groups (joined with their units) currently matching
    /// `side`, the sequence number that snapshot was taken at, and a
    /// receiver for updates from this point on.
    pub(crate) fn subscribe_group_updates(
        &self,
        side: Option<proto::Side>,
    ) -> (Vec<proto::Group>, u64, broadcast::Receiver<GroupEvent>) {
        let receiver = self.group_updates.subscribe();
        let inner = self.inner.read();
        let groups = inner
            .groups
            .iter()
            .filter(|(_, group)| {
                side.is_none_or(|side| proto::Side::try_from(group.side) == Ok(side))
            })
            .map(|(id, group)| {
                let mut group = group.clone();
                group.units = inner.member_units(id);
                group
            })
            .collect();
        (groups, inner.sequence, receiver)
    }
}

fn group_upserted(group: proto::Group) -> proto::SubscribeGroupUpdatesResponse {
    proto::SubscribeGroupUpdatesResponse {
        event: Some(proto::subscribe_group_updates_response::Event::Upserted(
            group,
        )),
    }
}

fn group_removed(id: String) -> proto::SubscribeGroupUpdatesResponse {
    proto::SubscribeGroupUpdatesResponse {
        event: Some(proto::subscribe_group_updates_response::Event::RemovedId(
            id,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(id: &str, group_id: &str) -> proto::Unit {
        proto::Unit {
            id: id.to_string(),
            group_id: group_id.to_string(),
            category: 0,
            r#type: String::new(),
            state: None,
        }
    }

    fn group(id: &str, side: proto::Side) -> proto::Group {
        proto::Group {
            id: id.to_string(),
            side: side as i32,
            readiness: None,
            has_task: false,
            waypoints: Vec::new(),
            units: Vec::new(),
        }
    }

    fn unit_ids(group: &proto::Group) -> Vec<&str> {
        group.units.iter().map(|unit| unit.id.as_str()).collect()
    }

    #[test]
    fn upsert_and_get_unit() {
        let cache = StateCache::new();
        cache.upsert_unit(unit("u1", "g1"));
        assert_eq!(
            cache.get_unit(&UnitId::from("u1")).map(|u| u.id),
            Some("u1".to_string())
        );
        assert_eq!(cache.get_unit(&UnitId::from("missing")), None);
    }

    #[test]
    fn remove_unit_drops_it() {
        let cache = StateCache::new();
        cache.upsert_unit(unit("u1", "g1"));
        assert!(cache.remove_unit(&UnitId::from("u1")).is_some());
        assert_eq!(cache.get_unit(&UnitId::from("u1")), None);
        assert_eq!(cache.remove_unit(&UnitId::from("u1")), None);
    }

    #[test]
    fn remove_unit_broadcasts_its_group_without_it() {
        let cache = StateCache::new();
        cache.upsert_group_with_units(group("g1", proto::Side::Blufor), vec![unit("u1", "g1")]);
        // Subscribing after the setup upsert above: only the snapshot
        // Vec (unused here) reflects it, so the receiver starts empty.
        let mut receiver = cache.subscribe_group_updates(None).2;

        cache.remove_unit(&UnitId::from("u1"));

        let event = receiver
            .try_recv()
            .expect("remove_unit must re-broadcast its group");
        match event.response.event {
            Some(proto::subscribe_group_updates_response::Event::Upserted(group)) => {
                assert_eq!(unit_ids(&group), Vec::<&str>::new());
            }
            other => panic!("expected an upsert with the unit gone, got {other:?}"),
        }
    }

    #[test]
    fn unit_membership_moves_with_group_id() {
        let cache = StateCache::new();
        cache.upsert_unit(unit("u1", "g1"));
        assert_eq!(cache.list_units(None, Some(&GroupId::from("g1"))).len(), 1);
        assert_eq!(cache.list_units(None, Some(&GroupId::from("g2"))).len(), 0);

        // Re-upserting with a different group_id moves membership, since the
        // unit's group_id is the only place it's recorded.
        cache.upsert_unit(unit("u1", "g2"));
        assert_eq!(cache.list_units(None, Some(&GroupId::from("g1"))).len(), 0);
        assert_eq!(cache.list_units(None, Some(&GroupId::from("g2"))).len(), 1);
    }

    #[test]
    fn list_units_filters_by_side_via_group() {
        let cache = StateCache::new();
        cache.upsert_group(group("g1", proto::Side::Blufor));
        cache.upsert_group(group("g2", proto::Side::Opfor));
        cache.upsert_unit(unit("u1", "g1"));
        cache.upsert_unit(unit("u2", "g2"));

        let blufor = cache.list_units(Some(proto::Side::Blufor), None);
        assert_eq!(
            blufor.iter().map(|u| u.id.as_str()).collect::<Vec<_>>(),
            vec!["u1"]
        );

        assert_eq!(cache.list_units(None, None).len(), 2);
    }

    #[test]
    fn list_units_excludes_orphaned_units_when_side_filtered() {
        let cache = StateCache::new();
        cache.upsert_unit(unit("u1", "no-such-group"));
        assert_eq!(cache.list_units(Some(proto::Side::Blufor), None).len(), 0);
        assert_eq!(cache.list_units(None, None).len(), 1);
    }

    #[test]
    fn list_groups_filters_by_side() {
        let cache = StateCache::new();
        cache.upsert_group(group("g1", proto::Side::Blufor));
        cache.upsert_group(group("g2", proto::Side::Opfor));

        let blufor = cache.list_groups(Some(proto::Side::Blufor));
        assert_eq!(
            blufor.iter().map(|g| g.id.as_str()).collect::<Vec<_>>(),
            vec!["g1"]
        );
        assert_eq!(cache.list_groups(None).len(), 2);
    }

    #[test]
    fn remove_group_drops_it() {
        let cache = StateCache::new();
        cache.upsert_group(group("g1", proto::Side::Blufor));
        assert!(cache.remove_group(&GroupId::from("g1")).is_some());
        assert_eq!(cache.get_group(&GroupId::from("g1")), None);
    }

    #[test]
    fn remove_group_drops_its_units() {
        let cache = StateCache::new();
        cache.upsert_group_with_units(group("g1", proto::Side::Blufor), vec![unit("u1", "g1")]);
        let removed = cache.remove_group(&GroupId::from("g1")).expect("cached");
        assert_eq!(unit_ids(&removed), vec!["u1"]);
        assert_eq!(cache.get_unit(&UnitId::from("u1")), None);
        assert_eq!(cache.list_units(None, None).len(), 0);
    }

    #[test]
    fn upsert_group_with_units_joins_units_in_reads() {
        let cache = StateCache::new();
        cache.upsert_group_with_units(
            group("g1", proto::Side::Blufor),
            vec![unit("u1", "g1"), unit("u2", "g1")],
        );

        let group = cache.get_group(&GroupId::from("g1")).expect("cached");
        assert_eq!(unit_ids(&group), vec!["u1", "u2"]);

        let listed = cache.list_groups(None);
        assert_eq!(unit_ids(&listed[0]), vec!["u1", "u2"]);
    }

    #[test]
    fn upsert_group_with_units_drops_members_not_listed() {
        let cache = StateCache::new();
        cache.upsert_group_with_units(
            group("g1", proto::Side::Blufor),
            vec![unit("u1", "g1"), unit("u2", "g1")],
        );
        cache.upsert_group_with_units(group("g1", proto::Side::Blufor), vec![unit("u1", "g1")]);

        let group = cache.get_group(&GroupId::from("g1")).expect("cached");
        assert_eq!(unit_ids(&group), vec!["u1"]);
        // The dropped member is gone entirely, not just detached.
        assert_eq!(cache.get_unit(&UnitId::from("u2")), None);
    }

    #[test]
    fn upsert_group_with_units_moves_a_unit_from_another_group() {
        let cache = StateCache::new();
        cache.upsert_group_with_units(group("g1", proto::Side::Blufor), vec![unit("u1", "g1")]);
        cache.upsert_group_with_units(group("g2", proto::Side::Blufor), vec![unit("u1", "g2")]);

        let g1 = cache.get_group(&GroupId::from("g1")).expect("cached");
        let g2 = cache.get_group(&GroupId::from("g2")).expect("cached");
        assert_eq!(unit_ids(&g1), Vec::<&str>::new());
        assert_eq!(unit_ids(&g2), vec!["u1"]);
        assert_eq!(cache.get_unit(&UnitId::from("u1")).unwrap().group_id, "g2");
    }

    #[test]
    fn upsert_group_with_units_forces_the_units_group_id() {
        let cache = StateCache::new();
        // A unit claiming a different group_id than the one it's embedded
        // in is corrected, not trusted.
        cache.upsert_group_with_units(group("g1", proto::Side::Blufor), vec![unit("u1", "bogus")]);
        assert_eq!(cache.get_unit(&UnitId::from("u1")).unwrap().group_id, "g1");
    }

    #[test]
    fn upsert_group_with_units_is_a_noop_on_unchanged_input() {
        let cache = StateCache::new();
        let (_, sequence_before, _) = cache.subscribe_group_updates(None);
        cache.upsert_group_with_units(group("g1", proto::Side::Blufor), vec![unit("u1", "g1")]);
        let (_, sequence_after_first, _) = cache.subscribe_group_updates(None);
        assert!(sequence_after_first > sequence_before);

        cache.upsert_group_with_units(group("g1", proto::Side::Blufor), vec![unit("u1", "g1")]);
        let (_, sequence_after_repeat, _) = cache.subscribe_group_updates(None);
        assert_eq!(sequence_after_repeat, sequence_after_first);
    }

    #[test]
    fn side_change_still_emits_removal_then_upsert() {
        let cache = StateCache::new();
        let mut receiver = cache.subscribe_group_updates(None).2;
        cache.upsert_group(group("g1", proto::Side::Blufor));
        cache.upsert_group(group("g1", proto::Side::Opfor));

        use proto::subscribe_group_updates_response::Event;
        let first = receiver.try_recv().expect("upsert").response.event;
        let second = receiver.try_recv().expect("removal").response.event;
        let third = receiver.try_recv().expect("upsert").response.event;
        assert!(matches!(first, Some(Event::Upserted(_))));
        assert!(matches!(second, Some(Event::RemovedId(_))));
        match third {
            Some(Event::Upserted(group)) => {
                assert_eq!(proto::Side::try_from(group.side), Ok(proto::Side::Opfor))
            }
            other => panic!("expected an upsert, got {other:?}"),
        }
    }
}
