use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::ids::GroupId;
use crate::proto;
use crate::services::{ResponseStream, parse_side};
use crate::state::{GroupEvent, StateCache};

/// Bound on the outgoing channel for a single subscriber; bounded so a slow
/// client applies backpressure rather than letting updates queue forever.
const SUBSCRIBER_CHANNEL_CAPACITY: usize = 16;

pub struct GroupServiceImpl {
    state: Arc<StateCache>,
}

impl GroupServiceImpl {
    pub fn new(state: Arc<StateCache>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl proto::group_service_server::GroupService for GroupServiceImpl {
    async fn list_groups(
        &self,
        request: Request<proto::ListGroupsRequest>,
    ) -> Result<Response<proto::ListGroupsResponse>, Status> {
        let side = parse_side(request.into_inner().side)?;
        let groups = self.state.list_groups(side);
        Ok(Response::new(proto::ListGroupsResponse { groups }))
    }

    async fn get_group(
        &self,
        request: Request<proto::GetGroupRequest>,
    ) -> Result<Response<proto::GetGroupResponse>, Status> {
        let id = request.into_inner().id;
        let group = self
            .state
            .get_group(&GroupId::from(id.as_str()))
            .ok_or_else(|| Status::not_found(format!("group {id} not found")))?;
        Ok(Response::new(proto::GetGroupResponse {
            group: Some(group),
        }))
    }

    type SubscribeGroupUpdatesStream = ResponseStream<proto::SubscribeGroupUpdatesResponse>;

    async fn subscribe_group_updates(
        &self,
        request: Request<proto::SubscribeGroupUpdatesRequest>,
    ) -> Result<Response<Self::SubscribeGroupUpdatesStream>, Status> {
        let side = parse_side(request.into_inner().side)?;
        let (snapshot, snapshot_sequence, updates) = self.state.subscribe_group_updates(side);

        let (tx, rx) = mpsc::channel(SUBSCRIBER_CHANNEL_CAPACITY);
        tokio::spawn(forward_group_updates(
            snapshot,
            snapshot_sequence,
            side,
            updates,
            tx,
        ));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

async fn forward_group_updates(
    snapshot: Vec<proto::Group>,
    snapshot_sequence: u64,
    side: Option<proto::Side>,
    mut updates: broadcast::Receiver<GroupEvent>,
    tx: mpsc::Sender<Result<proto::SubscribeGroupUpdatesResponse, Status>>,
) {
    use proto::subscribe_group_updates_response::Event;

    for group in snapshot {
        let response = proto::SubscribeGroupUpdatesResponse {
            event: Some(Event::Upserted(group)),
        };
        if tx.send(Ok(response)).await.is_err() {
            return;
        }
    }

    loop {
        // Without racing `tx.closed()` here, a disconnected client's task
        // (and its broadcast::Receiver) would linger until the next group
        // update happened to occur, since `updates.recv()` alone won't
        // notice a client that's gone away during an idle period.
        tokio::select! {
            () = tx.closed() => return,
            update = updates.recv() => match update {
                Ok(event) => {
                    // Already reflected in (or older than) the snapshot
                    // above: without this check, an update that landed in
                    // this receiver's buffer between it being created and
                    // the snapshot being read would replay here, showing
                    // the client newest (snapshot) -> older (this) state.
                    if event.sequence <= snapshot_sequence {
                        continue;
                    }
                    let update = event.response;
                    // A removal can't carry a side (the group's already
                    // gone), so it's forwarded unconditionally: a
                    // subscriber that never had this id just no-ops on it.
                    let matches_side = match &update.event {
                        Some(Event::Upserted(group)) => {
                            side.is_none_or(|side| proto::Side::try_from(group.side) == Ok(side))
                        }
                        Some(Event::RemovedId(_)) | None => true,
                    };
                    if matches_side && tx.send(Ok(update)).await.is_err() {
                        return;
                    }
                }
                // A slow subscriber can no longer be told what it missed
                // (the buffer already dropped it), so it's aborted rather
                // than silently resynced -- the contract's documented way
                // for a client to recover is to resubscribe and rebuild.
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let _ = tx
                        .send(Err(Status::aborted("subscriber lagged; resubscribe")))
                        .await;
                    return;
                }
                Err(broadcast::error::RecvError::Closed) => return,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_group(id: &str, side: proto::Side) -> proto::Group {
        proto::Group {
            id: id.to_string(),
            side: side as i32,
            readiness: None,
            has_task: false,
            waypoints: Vec::new(),
            units: Vec::new(),
        }
    }

    /// Drives `forward_group_updates` directly against a receiver that's
    /// already fallen behind, bypassing the gRPC transport entirely: proving
    /// this maps `Lagged` to an aborted response doesn't depend on how much
    /// backpressure the network happens to apply.
    #[tokio::test]
    async fn aborts_a_lagged_subscriber() {
        let cache = StateCache::new();
        let (_, _, updates) = cache.subscribe_group_updates(None);
        // Every side flip re-broadcasts (a removal plus an upsert), so this
        // overflows the broadcast buffer regardless of its exact capacity,
        // without `updates` ever being read.
        for i in 0..300 {
            let side = if i % 2 == 0 {
                proto::Side::Opfor
            } else {
                proto::Side::Blufor
            };
            cache.upsert_group(a_group("g1", side));
        }

        let (tx, mut rx) = mpsc::channel(1);
        forward_group_updates(Vec::new(), 0, None, updates, tx).await;

        let status = match rx.recv().await {
            Some(Err(status)) => status,
            other => panic!("expected an aborted response, got {other:?}"),
        };
        assert_eq!(status.code(), tonic::Code::Aborted);
    }
}
