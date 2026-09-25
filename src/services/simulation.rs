use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::proto;
use crate::services::ResponseStream;
use crate::state::StateCache;

const SUBSCRIBER_CHANNEL_CAPACITY: usize = 16;

pub struct SimulationServiceImpl {
    state: Arc<StateCache>,
}

impl SimulationServiceImpl {
    pub fn new(state: Arc<StateCache>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl proto::simulation_service_server::SimulationService for SimulationServiceImpl {
    async fn get_simulation_info(
        &self,
        _request: Request<proto::GetSimulationInfoRequest>,
    ) -> Result<Response<proto::GetSimulationInfoResponse>, Status> {
        let info = self
            .state
            .simulation_info()
            .ok_or_else(|| Status::unavailable("simulation info not set"))?;
        Ok(Response::new(proto::GetSimulationInfoResponse {
            info: Some(info),
        }))
    }

    type SubscribeSimulationUpdatesStream =
        ResponseStream<proto::SubscribeSimulationUpdatesResponse>;

    async fn subscribe_simulation_updates(
        &self,
        _request: Request<proto::SubscribeSimulationUpdatesRequest>,
    ) -> Result<Response<Self::SubscribeSimulationUpdatesStream>, Status> {
        // Subscribe before reading the snapshot: an update that lands in
        // between would otherwise be missed by both.
        let updates = self.state.subscribe_simulation_updates();
        let snapshot = self.state.simulation_state();

        let (tx, rx) = mpsc::channel(SUBSCRIBER_CHANNEL_CAPACITY);
        tokio::spawn(forward_simulation_updates(snapshot, updates, tx));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

async fn forward_simulation_updates(
    snapshot: Option<proto::SimulationStateUpdate>,
    mut updates: broadcast::Receiver<proto::SubscribeSimulationUpdatesResponse>,
    tx: mpsc::Sender<Result<proto::SubscribeSimulationUpdatesResponse, Status>>,
) {
    if let Some(state) = snapshot
        && tx
            .send(Ok(proto::SubscribeSimulationUpdatesResponse {
                state: Some(state),
            }))
            .await
            .is_err()
    {
        return;
    }

    loop {
        // Without racing `tx.closed()` here, a disconnected client's task
        // (and its broadcast::Receiver) would linger until the next
        // simulation update happened to occur.
        tokio::select! {
            () = tx.closed() => return,
            update = updates.recv() => match update {
                Ok(update) => {
                    if tx.send(Ok(update)).await.is_err() {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
            },
        }
    }
}
