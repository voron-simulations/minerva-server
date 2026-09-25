use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::ReentrantMutex;
use tokio::sync::oneshot;
use tonic::Status;

use crate::ids::{CommandId, CommandIdAllocator, GroupId};
use crate::proto;

/// Domain form of [`proto::SendCommandRequest`], validated at the service
/// boundary so the rest of the crate never has to handle a missing oneof
/// or a missing required field.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Move {
        group_id: GroupId,
        position: proto::Position,
    },
    SearchAndDestroy {
        group_id: GroupId,
        position: proto::Position,
    },
    DefendZone {
        group_id: GroupId,
        zone_id: String,
        position: Option<proto::Position>,
    },
    Patrol {
        group_id: GroupId,
        waypoints: Vec<proto::Position>,
        loop_: bool,
    },
    Support {
        supporter_group_id: GroupId,
        supported_group_id: GroupId,
        support_type: String,
    },
}

impl Command {
    /// The group that must be dispatched to (and looked up in the cache
    /// before dispatching, for [`Command::Support`] this is the supporting
    /// group, since it is the one that receives the order).
    pub fn group_id(&self) -> &GroupId {
        match self {
            Command::Move { group_id, .. }
            | Command::SearchAndDestroy { group_id, .. }
            | Command::DefendZone { group_id, .. }
            | Command::Patrol { group_id, .. } => group_id,
            Command::Support {
                supporter_group_id, ..
            } => supporter_group_id,
        }
    }

    pub fn from_proto(request: proto::SendCommandRequest) -> Result<Self, Status> {
        use proto::send_command_request::Command as Oneof;

        let require_position = |position: Option<proto::Position>| {
            position.ok_or_else(|| Status::invalid_argument("position is required"))
        };
        let require_group_id = |group_id: String| -> Result<GroupId, Status> {
            if group_id.is_empty() {
                return Err(Status::invalid_argument("group_id is required"));
            }
            Ok(group_id.into())
        };

        match request
            .command
            .ok_or_else(|| Status::invalid_argument("command is required"))?
        {
            Oneof::Move(c) => Ok(Command::Move {
                group_id: require_group_id(c.group_id)?,
                position: require_position(c.position)?,
            }),
            Oneof::SearchAndDestroy(c) => Ok(Command::SearchAndDestroy {
                group_id: require_group_id(c.group_id)?,
                position: require_position(c.position)?,
            }),
            Oneof::DefendZone(c) => {
                if c.zone_id.is_empty() {
                    return Err(Status::invalid_argument("zone_id is required"));
                }
                Ok(Command::DefendZone {
                    group_id: require_group_id(c.group_id)?,
                    zone_id: c.zone_id,
                    position: c.position,
                })
            }
            Oneof::Patrol(c) => {
                if c.waypoints.is_empty() {
                    return Err(Status::invalid_argument("waypoints must not be empty"));
                }
                Ok(Command::Patrol {
                    group_id: require_group_id(c.group_id)?,
                    waypoints: c.waypoints,
                    loop_: c.r#loop,
                })
            }
            Oneof::Support(c) => {
                if c.supported_group_id.is_empty() {
                    return Err(Status::invalid_argument("supported_group_id is required"));
                }
                if c.support_type.is_empty() {
                    return Err(Status::invalid_argument("support_type is required"));
                }
                Ok(Command::Support {
                    supporter_group_id: require_group_id(c.supporter_group_id)?,
                    supported_group_id: c.supported_group_id.into(),
                    support_type: c.support_type,
                })
            }
        }
    }
}

/// Outcome of a dispatched command, reported back through
/// [`Dispatcher::complete`] once the engine acknowledges it (or through a
/// timeout/cancellation if it never does).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandOutcome {
    Success,
    Failure(String),
}

/// Implemented by engines (e.g. the Arma 3 plugin) to actually carry out a
/// dispatched command. Expected to return promptly; the real work happens
/// asynchronously, acknowledged later via [`Dispatcher::complete`].
pub trait CommandSink: Send + Sync {
    fn dispatch(&self, id: CommandId, command: &Command);
}

type Pending = ReentrantMutex<RefCell<HashMap<CommandId, oneshot::Sender<CommandOutcome>>>>;

/// Tracks commands sent to the engine until they are acknowledged. Owns the
/// single [`CommandSink`] commands are dispatched through.
pub struct Dispatcher {
    sink: Arc<dyn CommandSink>,
    timeout: Duration,
    ids: CommandIdAllocator,
    // A *reentrant* mutex, deliberately: `send()` holds this across the call
    // to `sink.dispatch()`, and a `CommandSink` that acknowledges
    // synchronously (calling back into `complete()`/`cancel_all()` from
    // the same thread, inside `dispatch()`) needs that reentrant call to
    // succeed rather than deadlock on a lock its own caller already holds.
    pending: Pending,
}

impl Dispatcher {
    pub fn new(sink: Arc<dyn CommandSink>, timeout: Duration) -> Self {
        Self {
            sink,
            timeout,
            ids: CommandIdAllocator::default(),
            pending: ReentrantMutex::new(RefCell::new(HashMap::new())),
        }
    }

    /// Dispatches `command` and waits for the engine to acknowledge it via
    /// [`Dispatcher::complete`], up to the configured timeout.
    pub async fn send(&self, command: Command) -> CommandOutcome {
        let id = self.ids.next();
        let (tx, rx) = oneshot::channel();
        let _guard;
        {
            // insert() and dispatch() share this critical section with
            // cancel_all()'s drain, so the two are strictly ordered rather
            // than interleaved: cancel_all() can no longer remove this
            // entry (reporting "cancelled" to our caller) in the gap after
            // it's inserted but before the engine was actually told about
            // it, which would leave the caller and the engine disagreeing
            // about whether the command happened.
            let pending = self.pending.lock();
            pending.borrow_mut().insert(id, tx);
            // Constructed before dispatch(), and while `pending` is still
            // held (the mutex is reentrant, so the guard's own drop can
            // re-lock it): if engine-owned `dispatch()` panics, unwinding
            // drops this guard and removes the entry, rather than leaking
            // it in `pending` forever.
            _guard = RemovePending {
                pending: &self.pending,
                id,
            };
            self.sink.dispatch(id, &command);
        }

        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(outcome)) => outcome,
            // Sender was dropped without completing, e.g. via cancel_all().
            Ok(Err(_)) => CommandOutcome::Failure("cancelled".to_string()),
            Err(_) => CommandOutcome::Failure("timeout".to_string()),
        }
    }

    /// Acknowledges a previously dispatched command. Returns `false` if `id`
    /// is not (or no longer) pending, e.g. it already timed out.
    pub fn complete(&self, id: CommandId, outcome: CommandOutcome) -> bool {
        match self.pending.lock().borrow_mut().remove(&id) {
            Some(tx) => {
                let _ = tx.send(outcome);
                true
            }
            None => false,
        }
    }

    /// Fails every pending command, e.g. on a simulation reset.
    pub fn cancel_all(&self) {
        for (_, tx) in self.pending.lock().borrow_mut().drain() {
            let _ = tx.send(CommandOutcome::Failure("cancelled".to_string()));
        }
    }
}

/// Removes `id` from `pending` when dropped, regardless of why `send()`'s
/// future stopped running (completed normally, timed out, or was dropped
/// before either happened).
struct RemovePending<'a> {
    pending: &'a Pending,
    id: CommandId,
}

impl Drop for RemovePending<'_> {
    fn drop(&mut self) {
        self.pending.lock().borrow_mut().remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;

    use super::*;

    fn move_command(group_id: &str) -> Command {
        Command::Move {
            group_id: GroupId::from(group_id),
            position: proto::Position::default(),
        }
    }

    fn oneof(command: proto::send_command_request::Command) -> proto::SendCommandRequest {
        proto::SendCommandRequest {
            command: Some(command),
        }
    }

    fn expect_err(result: Result<Command, Status>) -> Status {
        match result {
            Ok(_) => panic!("expected an error"),
            Err(err) => err,
        }
    }

    #[test]
    fn from_proto_rejects_missing_command() {
        let err = expect_err(Command::from_proto(proto::SendCommandRequest {
            command: None,
        }));
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn from_proto_rejects_empty_group_id() {
        let request = oneof(proto::send_command_request::Command::Move(
            proto::MoveCommand {
                position: Some(proto::Position::default()),
                group_id: String::new(),
            },
        ));
        assert_eq!(
            expect_err(Command::from_proto(request)).code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn from_proto_rejects_empty_support_type() {
        let request = oneof(proto::send_command_request::Command::Support(
            proto::SupportCommand {
                supporter_group_id: "g1".to_string(),
                supported_group_id: "g2".to_string(),
                support_type: String::new(),
            },
        ));
        assert_eq!(
            expect_err(Command::from_proto(request)).code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn from_proto_rejects_move_without_position() {
        let request = oneof(proto::send_command_request::Command::Move(
            proto::MoveCommand {
                position: None,
                group_id: "g1".to_string(),
            },
        ));
        assert_eq!(
            expect_err(Command::from_proto(request)).code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn from_proto_rejects_patrol_without_waypoints() {
        let request = oneof(proto::send_command_request::Command::Patrol(
            proto::PatrolCommand {
                group_id: "g1".to_string(),
                waypoints: Vec::new(),
                r#loop: false,
            },
        ));
        assert_eq!(
            expect_err(Command::from_proto(request)).code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn from_proto_accepts_valid_move() {
        let request = oneof(proto::send_command_request::Command::Move(
            proto::MoveCommand {
                position: Some(proto::Position {
                    x: 1.0,
                    y: 2.0,
                    z: 3.0,
                }),
                group_id: "g1".to_string(),
            },
        ));
        let command = match Command::from_proto(request) {
            Ok(command) => command,
            Err(err) => panic!("expected Ok, got {err}"),
        };
        assert_eq!(command.group_id(), &GroupId::from("g1"));
    }

    #[test]
    fn support_group_id_is_the_supporter() {
        let command = Command::Support {
            supporter_group_id: GroupId::from("supporter"),
            supported_group_id: GroupId::from("supported"),
            support_type: "resupply".to_string(),
        };
        assert_eq!(command.group_id(), &GroupId::from("supporter"));
    }

    struct NoopSink;
    impl CommandSink for NoopSink {
        fn dispatch(&self, _id: CommandId, _command: &Command) {}
    }

    /// Notifies a test of the [`CommandId`] a command was dispatched under,
    /// so the test can drive [`Dispatcher::complete`]/`cancel_all` for it
    /// concurrently with the still-pending `send`.
    struct NotifyingSink {
        id_tx: Mutex<Option<oneshot::Sender<CommandId>>>,
    }
    impl CommandSink for NotifyingSink {
        fn dispatch(&self, id: CommandId, _command: &Command) {
            if let Some(tx) = self.id_tx.lock().take() {
                let _ = tx.send(id);
            }
        }
    }

    /// Acknowledges every command synchronously, from inside `dispatch()`
    /// itself -- the scenario that deadlocked before `pending` became a
    /// `ReentrantMutex`: `complete()` needs the same lock `send()` was
    /// still holding while calling this.
    struct SynchronousAckSink {
        dispatcher: std::sync::OnceLock<std::sync::Weak<Dispatcher>>,
    }
    impl CommandSink for SynchronousAckSink {
        fn dispatch(&self, id: CommandId, _command: &Command) {
            if let Some(dispatcher) = self.dispatcher.get().and_then(std::sync::Weak::upgrade) {
                dispatcher.complete(id, CommandOutcome::Success);
            }
        }
    }

    #[tokio::test]
    async fn send_does_not_deadlock_on_a_synchronously_acking_sink() {
        let sink = Arc::new(SynchronousAckSink {
            dispatcher: std::sync::OnceLock::new(),
        });
        let dispatcher = Arc::new(Dispatcher::new(sink.clone(), Duration::from_secs(5)));
        let _ = sink.dispatcher.set(Arc::downgrade(&dispatcher));

        // A generous, test-only timeout distinct from the Dispatcher's own:
        // if the reentrant lock regresses, this fails the test instead of
        // hanging the whole suite.
        match tokio::time::timeout(Duration::from_secs(2), dispatcher.send(move_command("g1")))
            .await
        {
            Ok(outcome) => assert_eq!(outcome, CommandOutcome::Success),
            Err(_) => panic!("send() deadlocked (or took far too long) on a synchronous ack"),
        }
    }

    #[tokio::test]
    async fn send_resolves_on_complete() {
        let (id_tx, id_rx) = oneshot::channel();
        let sink = Arc::new(NotifyingSink {
            id_tx: Mutex::new(Some(id_tx)),
        });
        let dispatcher = Dispatcher::new(sink, Duration::from_secs(5));

        let (outcome, acked) = tokio::join!(dispatcher.send(move_command("g1")), async {
            match id_rx.await {
                Ok(id) => dispatcher.complete(id, CommandOutcome::Success),
                Err(_) => false,
            }
        });

        assert!(acked);
        assert_eq!(outcome, CommandOutcome::Success);
    }

    #[tokio::test]
    async fn send_times_out_without_ack() {
        tokio::time::pause();
        let dispatcher = Dispatcher::new(Arc::new(NoopSink), Duration::from_millis(20));
        let outcome = dispatcher.send(move_command("g1")).await;
        assert_eq!(outcome, CommandOutcome::Failure("timeout".to_string()));
    }

    #[tokio::test]
    async fn cancel_all_fails_pending_commands() {
        let (id_tx, id_rx) = oneshot::channel();
        let sink = Arc::new(NotifyingSink {
            id_tx: Mutex::new(Some(id_tx)),
        });
        let dispatcher = Dispatcher::new(sink, Duration::from_secs(5));

        let (outcome, _) = tokio::join!(dispatcher.send(move_command("g1")), async {
            let _ = id_rx.await;
            dispatcher.cancel_all();
        });

        assert_eq!(outcome, CommandOutcome::Failure("cancelled".to_string()));
    }

    #[test]
    fn complete_unknown_id_returns_false() {
        let dispatcher = Dispatcher::new(Arc::new(NoopSink), Duration::from_secs(5));
        assert!(!dispatcher.complete(CommandId(0), CommandOutcome::Success));
    }

    #[tokio::test]
    async fn send_removes_pending_entry_if_dropped_before_completion() {
        let (id_tx, id_rx) = oneshot::channel();
        let sink = Arc::new(NotifyingSink {
            id_tx: Mutex::new(Some(id_tx)),
        });
        let dispatcher = Arc::new(Dispatcher::new(sink, Duration::from_secs(5)));

        let d = dispatcher.clone();
        let handle = tokio::spawn(async move { d.send(move_command("g1")).await });
        // Wait for dispatch() to run, so we know the entry was inserted
        // before we cancel the future that's awaiting its ack.
        if id_rx.await.is_err() {
            panic!("sink was never invoked");
        }
        assert_eq!(dispatcher.pending.lock().borrow().len(), 1);

        handle.abort();
        let _ = handle.await;
        assert_eq!(dispatcher.pending.lock().borrow().len(), 0);
    }

    #[tokio::test]
    async fn complete_is_not_reusable_after_first_ack() {
        let (id_tx, id_rx) = oneshot::channel();
        let sink = Arc::new(NotifyingSink {
            id_tx: Mutex::new(Some(id_tx)),
        });
        let dispatcher = Arc::new(Dispatcher::new(sink, Duration::from_secs(5)));

        let d = dispatcher.clone();
        let send = tokio::spawn(async move { d.send(move_command("g1")).await });
        let id = match id_rx.await {
            Ok(id) => id,
            Err(_) => panic!("sink was never invoked"),
        };

        assert!(dispatcher.complete(id, CommandOutcome::Success));
        assert!(!dispatcher.complete(id, CommandOutcome::Success));

        match send.await {
            Ok(outcome) => assert_eq!(outcome, CommandOutcome::Success),
            Err(err) => panic!("send task panicked: {err}"),
        }
    }
}
