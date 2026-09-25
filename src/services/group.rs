use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::ids::GroupId;
use crate::proto;
use crate::services::{ResponseStream, parse_side};
use crate::state::StateCache;

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
        let snapshot = self.state.list_groups(side);
        let updates = self.state.subscribe_group_updates();

        let (tx, rx) = mpsc::channel(SUBSCRIBER_CHANNEL_CAPACITY);
        tokio::spawn(forward_group_updates(snapshot, side, updates, tx));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

async fn forward_group_updates(
    snapshot: Vec<proto::Group>,
    side: Option<proto::Side>,
    mut updates: broadcast::Receiver<proto::SubscribeGroupUpdatesResponse>,
    tx: mpsc::Sender<Result<proto::SubscribeGroupUpdatesResponse, Status>>,
) {
    for group in snapshot {
        if tx
            .send(Ok(proto::SubscribeGroupUpdatesResponse {
                group: Some(group),
            }))
            .await
            .is_err()
        {
            return;
        }
    }

    loop {
        match updates.recv().await {
            Ok(update) => {
                let matches_side = side.is_none_or(|side| {
                    update
                        .group
                        .as_ref()
                        .is_some_and(|group| proto::Side::try_from(group.side) == Ok(side))
                });
                if matches_side && tx.send(Ok(update)).await.is_err() {
                    return;
                }
            }
            // A slow subscriber just misses what fell out of the buffer; keep going.
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}
