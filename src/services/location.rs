use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::proto;
use crate::services::parse_side;
use crate::state::StateCache;

pub struct LocationServiceImpl {
    state: Arc<StateCache>,
}

impl LocationServiceImpl {
    pub fn new(state: Arc<StateCache>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl proto::location_service_server::LocationService for LocationServiceImpl {
    async fn list_locations(
        &self,
        request: Request<proto::ListLocationsRequest>,
    ) -> Result<Response<proto::ListLocationsResponse>, Status> {
        let side = parse_side(request.into_inner().side)?;
        let locations = self.state.list_locations(side);
        Ok(Response::new(proto::ListLocationsResponse { locations }))
    }
}
