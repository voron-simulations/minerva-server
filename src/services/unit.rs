use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::ids::{GroupId, UnitId};
use crate::proto;
use crate::services::parse_side;
use crate::state::StateCache;

pub struct UnitServiceImpl {
    state: Arc<StateCache>,
}

impl UnitServiceImpl {
    pub fn new(state: Arc<StateCache>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl proto::unit_service_server::UnitService for UnitServiceImpl {
    async fn get_unit(
        &self,
        request: Request<proto::GetUnitRequest>,
    ) -> Result<Response<proto::GetUnitResponse>, Status> {
        let id = request.into_inner().id;
        let unit = self
            .state
            .get_unit(&UnitId::from(id.as_str()))
            .ok_or_else(|| Status::not_found(format!("unit {id} not found")))?;
        Ok(Response::new(proto::GetUnitResponse { unit: Some(unit) }))
    }

    async fn list_units(
        &self,
        request: Request<proto::ListUnitsRequest>,
    ) -> Result<Response<proto::ListUnitsResponse>, Status> {
        let request = request.into_inner();
        let side = parse_side(request.side)?;
        let group_id = request.group_id.map(GroupId::from);
        let units = self.state.list_units(side, group_id.as_ref());
        Ok(Response::new(proto::ListUnitsResponse { units }))
    }
}
