use std::collections::HashMap;

use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::ids::{GroupId, UnitId};
use crate::proto;

/// Number of buffered messages per update stream before a slow subscriber
/// starts missing them. Subscribers that lag skip forward to the latest
/// state rather than blocking writers or buffering unboundedly.
const BROADCAST_CAPACITY: usize = 64;

#[derive(Default)]
struct Inner {
    units: HashMap<UnitId, proto::Unit>,
    groups: HashMap<GroupId, proto::Group>,
    simulation_info: Option<proto::SimulationInfo>,
    simulation_state: Option<proto::SimulationStateUpdate>,
    locations: Vec<proto::Location>,
}

/// Normalized, in-memory snapshot of simulation state, fed by the engine
/// thread and read by the gRPC services. `Unit.group_id` is the only place
/// group membership is recorded — there is no separate group -> units index
/// to keep in sync, so membership can never disagree with itself.
pub struct StateCache {
    inner: RwLock<Inner>,
    simulation_updates: broadcast::Sender<proto::SubscribeSimulationUpdatesResponse>,
    group_updates: broadcast::Sender<proto::SubscribeGroupUpdatesResponse>,
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

    pub fn upsert_unit(&self, unit: proto::Unit) {
        let id = UnitId::from(unit.id.clone());
        self.inner.write().units.insert(id, unit);
    }

    pub fn remove_unit(&self, id: &UnitId) -> Option<proto::Unit> {
        self.inner.write().units.remove(id)
    }

    pub fn upsert_group(&self, group: proto::Group) {
        let id = GroupId::from(group.id.clone());
        self.inner.write().groups.insert(id, group.clone());
        // No subscribers is the common case (e.g. no client connected yet); ignore it.
        let _ = self
            .group_updates
            .send(proto::SubscribeGroupUpdatesResponse { group: Some(group) });
    }

    pub fn remove_group(&self, id: &GroupId) -> Option<proto::Group> {
        self.inner.write().groups.remove(id)
    }

    pub fn set_simulation_info(&self, info: proto::SimulationInfo) {
        self.inner.write().simulation_info = Some(info);
    }

    pub fn set_simulation_state(&self, state: proto::SimulationStateUpdate) {
        self.inner.write().simulation_state = Some(state);
        let _ = self
            .simulation_updates
            .send(proto::SubscribeSimulationUpdatesResponse { state: Some(state) });
    }

    pub fn set_locations(&self, locations: Vec<proto::Location>) {
        self.inner.write().locations = locations;
    }

    /// Drops all cached state. Does not affect existing subscriptions or
    /// in-flight commands — see [`crate::Dispatcher::cancel_all`] for that.
    pub fn clear(&self) {
        *self.inner.write() = Inner::default();
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
        self.inner.read().groups.get(id).cloned()
    }

    pub fn list_groups(&self, side: Option<proto::Side>) -> Vec<proto::Group> {
        self.inner
            .read()
            .groups
            .values()
            .filter(|group| side.is_none_or(|side| proto::Side::try_from(group.side) == Ok(side)))
            .cloned()
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

    pub fn subscribe_simulation_updates(
        &self,
    ) -> broadcast::Receiver<proto::SubscribeSimulationUpdatesResponse> {
        self.simulation_updates.subscribe()
    }

    pub fn subscribe_group_updates(
        &self,
    ) -> broadcast::Receiver<proto::SubscribeGroupUpdatesResponse> {
        self.group_updates.subscribe()
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
        }
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
    fn simulation_info_and_state_round_trip() {
        let cache = StateCache::new();
        assert_eq!(cache.simulation_info(), None);
        assert_eq!(cache.simulation_state(), None);

        let info = proto::SimulationInfo {
            world_name: "Altis".to_string(),
            ..Default::default()
        };
        cache.set_simulation_info(info.clone());
        assert_eq!(cache.simulation_info(), Some(info));

        let state = proto::SimulationStateUpdate {
            simulation_time: 42,
            ..Default::default()
        };
        cache.set_simulation_state(state);
        assert_eq!(cache.simulation_state(), Some(state));
    }

    #[test]
    fn locations_filter_by_owner_side() {
        let cache = StateCache::new();
        cache.set_locations(vec![
            proto::Location {
                id: "l1".to_string(),
                owner: proto::Side::Blufor as i32,
                ..Default::default()
            },
            proto::Location {
                id: "l2".to_string(),
                owner: proto::Side::Opfor as i32,
                ..Default::default()
            },
        ]);

        let blufor = cache.list_locations(Some(proto::Side::Blufor));
        assert_eq!(
            blufor.iter().map(|l| l.id.as_str()).collect::<Vec<_>>(),
            vec!["l1"]
        );
        assert_eq!(cache.list_locations(None).len(), 2);
    }

    #[test]
    fn clear_drops_everything() {
        let cache = StateCache::new();
        cache.upsert_unit(unit("u1", "g1"));
        cache.upsert_group(group("g1", proto::Side::Blufor));
        cache.set_simulation_info(proto::SimulationInfo::default());
        cache.set_locations(vec![proto::Location::default()]);

        cache.clear();

        assert_eq!(cache.list_units(None, None).len(), 0);
        assert_eq!(cache.list_groups(None).len(), 0);
        assert_eq!(cache.simulation_info(), None);
        assert_eq!(cache.list_locations(None).len(), 0);
    }

    #[tokio::test]
    async fn subscribe_group_updates_receives_upsert() {
        let cache = StateCache::new();
        let mut rx = cache.subscribe_group_updates();
        cache.upsert_group(group("g1", proto::Side::Blufor));
        let update = match rx.recv().await {
            Ok(update) => update,
            Err(err) => panic!("channel closed unexpectedly: {err}"),
        };
        assert_eq!(update.group.map(|g| g.id), Some("g1".to_string()));
    }

    #[tokio::test]
    async fn subscribe_simulation_updates_receives_state() {
        let cache = StateCache::new();
        let mut rx = cache.subscribe_simulation_updates();
        cache.set_simulation_state(proto::SimulationStateUpdate {
            simulation_time: 7,
            ..Default::default()
        });
        let update = match rx.recv().await {
            Ok(update) => update,
            Err(err) => panic!("channel closed unexpectedly: {err}"),
        };
        assert_eq!(update.state.map(|s| s.simulation_time), Some(7));
    }
}
