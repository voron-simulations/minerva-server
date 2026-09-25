//! End-to-end tests against a real [`ServerHandle`] over a real TCP socket,
//! exercising the gRPC surface the same way a client (or the Arma 3 plugin)
//! would.

use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use minerva_server::proto::command_service_client::CommandServiceClient;
use minerva_server::proto::group_service_client::GroupServiceClient;
use minerva_server::proto::location_service_client::LocationServiceClient;
use minerva_server::proto::send_command_request::Command as CommandOneof;
use minerva_server::proto::simulation_service_client::SimulationServiceClient;
use minerva_server::proto::unit_service_client::UnitServiceClient;
use minerva_server::proto::{
    CommandResult, GetSimulationInfoRequest, GetUnitRequest, Group, ListGroupsRequest,
    ListLocationsRequest, ListUnitsRequest, Location, MoveCommand, Position, SendCommandRequest,
    Side, SimulationInfo, SimulationStateUpdate, SubscribeGroupUpdatesRequest,
    SubscribeSimulationUpdatesRequest, Unit, UnitState,
};
use minerva_server::{Command, CommandId, CommandSink, ServerConfig, ServerHandle};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

fn expect<T, E: Debug>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(err) => panic!("unexpected error: {err:?}"),
    }
}

fn expect_some<T>(value: Option<T>) -> T {
    match value {
        Some(value) => value,
        None => panic!("expected Some, got None"),
    }
}

fn endpoint(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

/// Forwards every dispatched command to a channel a test can read from and
/// drive (ack, or just let it time out).
struct RecordingSink {
    tx: mpsc::UnboundedSender<(CommandId, Command)>,
}

impl CommandSink for RecordingSink {
    fn dispatch(&self, id: CommandId, command: &Command) {
        let _ = self.tx.send((id, command.clone()));
    }
}

fn spawn_server(
    command_timeout: Duration,
) -> (ServerHandle, mpsc::UnboundedReceiver<(CommandId, Command)>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let sink = Arc::new(RecordingSink { tx });
    let addr = expect("127.0.0.1:0".parse());
    let config = ServerConfig {
        addr,
        command_timeout,
    };
    (expect(ServerHandle::spawn(config, sink)), rx)
}

fn a_unit(id: &str, group_id: &str, position: Position) -> Unit {
    Unit {
        id: id.to_string(),
        group_id: group_id.to_string(),
        category: 0,
        r#type: "B_Soldier_F".to_string(),
        state: Some(UnitState {
            position: Some(position),
            ..Default::default()
        }),
    }
}

fn a_group(id: &str, side: Side) -> Group {
    Group {
        id: id.to_string(),
        side: side as i32,
        readiness: None,
        has_task: false,
        waypoints: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_unit_is_visible_via_get_and_list() {
    let (handle, _rx) = spawn_server(Duration::from_secs(5));
    let position = Position {
        x: 100.0,
        y: 200.0,
        z: 300.0,
    };
    handle.state().upsert_unit(a_unit("u1", "g1", position));

    let mut units = expect(UnitServiceClient::connect(endpoint(handle.local_addr())).await);

    let got = expect(
        units
            .get_unit(GetUnitRequest {
                id: "u1".to_string(),
            })
            .await,
    )
    .into_inner();
    let got_unit = expect_some(got.unit);
    assert_eq!(got_unit.state.and_then(|s| s.position), Some(position));

    let listed = expect(
        units
            .list_units(ListUnitsRequest {
                side: None,
                group_id: Some("g1".to_string()),
            })
            .await,
    )
    .into_inner();
    assert_eq!(listed.units.len(), 1);
    assert_eq!(listed.units[0].id, "u1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_filters_by_side() {
    let (handle, _rx) = spawn_server(Duration::from_secs(5));
    handle.state().upsert_group(a_group("g1", Side::Blufor));
    handle.state().upsert_group(a_group("g2", Side::Opfor));

    let mut groups = expect(GroupServiceClient::connect(endpoint(handle.local_addr())).await);

    let blufor = expect(
        groups
            .list_groups(ListGroupsRequest {
                side: Some(Side::Blufor as i32),
            })
            .await,
    )
    .into_inner();
    assert_eq!(
        blufor
            .groups
            .iter()
            .map(|g| g.id.as_str())
            .collect::<Vec<_>>(),
        vec!["g1"]
    );

    let all = expect(groups.list_groups(ListGroupsRequest { side: None }).await).into_inner();
    assert_eq!(all.groups.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_group_updates_yields_snapshot_then_update() {
    let (handle, _rx) = spawn_server(Duration::from_secs(5));
    handle.state().upsert_group(a_group("g1", Side::Blufor));

    let mut groups = expect(GroupServiceClient::connect(endpoint(handle.local_addr())).await);
    let mut stream = expect(
        groups
            .subscribe_group_updates(SubscribeGroupUpdatesRequest { side: None })
            .await,
    )
    .into_inner();

    let snapshot = expect_some(stream.next().await);
    let snapshot = expect(snapshot);
    assert_eq!(snapshot.group.map(|g| g.id), Some("g1".to_string()));

    handle.state().upsert_group(a_group("g2", Side::Opfor));
    let update = expect_some(stream.next().await);
    let update = expect(update);
    assert_eq!(update.group.map(|g| g.id), Some("g2".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_locations_filters_by_owner_side() {
    let (handle, _rx) = spawn_server(Duration::from_secs(5));
    handle.state().set_locations(vec![
        Location {
            id: "l1".to_string(),
            owner: Side::Blufor as i32,
            ..Default::default()
        },
        Location {
            id: "l2".to_string(),
            owner: Side::Opfor as i32,
            ..Default::default()
        },
    ]);

    let mut locations = expect(LocationServiceClient::connect(endpoint(handle.local_addr())).await);

    let blufor = expect(
        locations
            .list_locations(ListLocationsRequest {
                side: Some(Side::Blufor as i32),
            })
            .await,
    )
    .into_inner();
    assert_eq!(
        blufor
            .locations
            .iter()
            .map(|l| l.id.as_str())
            .collect::<Vec<_>>(),
        vec!["l1"]
    );

    let all = expect(
        locations
            .list_locations(ListLocationsRequest { side: None })
            .await,
    )
    .into_inner();
    assert_eq!(all.locations.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_simulation_info_and_subscribe_updates() {
    let (handle, _rx) = spawn_server(Duration::from_secs(5));
    handle.state().set_simulation_info(SimulationInfo {
        world_name: "Altis".to_string(),
        ..Default::default()
    });

    let mut simulation =
        expect(SimulationServiceClient::connect(endpoint(handle.local_addr())).await);

    let info = expect(
        simulation
            .get_simulation_info(GetSimulationInfoRequest {})
            .await,
    )
    .into_inner();
    assert_eq!(info.info.map(|i| i.world_name), Some("Altis".to_string()));

    handle.state().set_simulation_state(SimulationStateUpdate {
        simulation_time: 1,
        ..Default::default()
    });
    let mut stream = expect(
        simulation
            .subscribe_simulation_updates(SubscribeSimulationUpdatesRequest {})
            .await,
    )
    .into_inner();

    let snapshot = expect(expect_some(stream.next().await));
    assert_eq!(snapshot.state.map(|s| s.simulation_time), Some(1));

    handle.state().set_simulation_state(SimulationStateUpdate {
        simulation_time: 2,
        ..Default::default()
    });
    let update = expect(expect_some(stream.next().await));
    assert_eq!(update.state.map(|s| s.simulation_time), Some(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_command_acked_returns_success() {
    let (handle, mut rx) = spawn_server(Duration::from_secs(5));
    handle.state().upsert_group(a_group("g1", Side::Blufor));

    let mut commands = expect(CommandServiceClient::connect(endpoint(handle.local_addr())).await);
    let request = SendCommandRequest {
        command: Some(CommandOneof::Move(MoveCommand {
            position: Some(Position {
                x: 1.0,
                y: 2.0,
                z: 3.0,
            }),
            group_id: "g1".to_string(),
        })),
    };

    let dispatcher = handle.dispatcher().clone();
    let call = tokio::spawn(async move { commands.send_command(request).await });

    let (id, dispatched) = expect_some(rx.recv().await);
    assert_eq!(dispatched.group_id().as_str(), "g1");
    assert!(dispatcher.complete(id, minerva_server::CommandOutcome::Success));

    let response = expect(expect(call.await)).into_inner();
    assert_eq!(response.result, CommandResult::Success as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_command_without_ack_times_out() {
    let (handle, _rx) = spawn_server(Duration::from_millis(100));
    handle.state().upsert_group(a_group("g1", Side::Blufor));

    let mut commands = expect(CommandServiceClient::connect(endpoint(handle.local_addr())).await);
    let request = SendCommandRequest {
        command: Some(CommandOneof::Move(MoveCommand {
            position: Some(Position {
                x: 1.0,
                y: 2.0,
                z: 3.0,
            }),
            group_id: "g1".to_string(),
        })),
    };

    let response = expect(commands.send_command(request).await).into_inner();
    assert_eq!(response.result, CommandResult::Failure as i32);
    assert_eq!(response.reason, "timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_command_unknown_group_fails_without_dispatch() {
    let (handle, mut rx) = spawn_server(Duration::from_secs(5));

    let mut commands = expect(CommandServiceClient::connect(endpoint(handle.local_addr())).await);
    let request = SendCommandRequest {
        command: Some(CommandOneof::Move(MoveCommand {
            position: Some(Position {
                x: 1.0,
                y: 2.0,
                z: 3.0,
            }),
            group_id: "no-such-group".to_string(),
        })),
    };

    let response = expect(commands.send_command(request).await).into_inner();
    assert_eq!(response.result, CommandResult::Failure as i32);
    assert!(
        rx.try_recv().is_err(),
        "unknown group must not be dispatched to the sink"
    );
}
