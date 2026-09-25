use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::commands::{Command, CommandOutcome, Dispatcher};
use crate::proto;
use crate::state::StateCache;

pub struct CommandServiceImpl {
    state: Arc<StateCache>,
    dispatcher: Arc<Dispatcher>,
}

impl CommandServiceImpl {
    pub fn new(state: Arc<StateCache>, dispatcher: Arc<Dispatcher>) -> Self {
        Self { state, dispatcher }
    }
}

#[tonic::async_trait]
impl proto::command_service_server::CommandService for CommandServiceImpl {
    async fn send_command(
        &self,
        request: Request<proto::SendCommandRequest>,
    ) -> Result<Response<proto::SendCommandResponse>, Status> {
        let command = Command::from_proto(request.into_inner())?;

        // Fail fast on an unknown group rather than dispatching a command the
        // engine has no group to act on.
        if self.state.get_group(command.group_id()).is_none() {
            return Ok(Response::new(proto::SendCommandResponse {
                result: proto::CommandResult::Failure as i32,
                reason: format!("unknown group {}", command.group_id()),
            }));
        }

        let (result, reason) = match self.dispatcher.send(command).await {
            CommandOutcome::Success => (proto::CommandResult::Success, String::new()),
            CommandOutcome::Failure(reason) => (proto::CommandResult::Failure, reason),
        };
        Ok(Response::new(proto::SendCommandResponse {
            result: result as i32,
            reason,
        }))
    }
}
