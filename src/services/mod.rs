mod command;
mod group;
mod location;
mod simulation;
mod unit;

pub(crate) use command::CommandServiceImpl;
pub(crate) use group::GroupServiceImpl;
pub(crate) use location::LocationServiceImpl;
pub(crate) use simulation::SimulationServiceImpl;
pub(crate) use unit::UnitServiceImpl;

use std::pin::Pin;

use tonic::Status;

use crate::proto;

/// Response stream type shared by the server-streaming RPCs.
pub(crate) type ResponseStream<T> =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send>>;

/// Decodes an `optional Side` request field, rejecting out-of-range values.
pub(crate) fn parse_side(side: Option<i32>) -> Result<Option<proto::Side>, Status> {
    side.map(proto::Side::try_from)
        .transpose()
        .map_err(|_| Status::invalid_argument("invalid side"))
}
