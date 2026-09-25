//! Minerva gRPC server: a state cache fed by a simulation engine, and
//! command dispatch back to it. See [`StateCache`], [`Dispatcher`] and
//! [`ServerHandle`].

pub mod proto {
    #![allow(clippy::all)]
    // minerva.v1.rs itself does `include!("minerva.v1.tonic.rs")` at its tail
    // (the neoeinstein-prost/tonic plugin pair links them that way), so
    // including it here pulls in both generated files.
    include!("gen/minerva/v1/minerva.v1.rs");
}

mod commands;
mod ids;
mod server;
mod services;
mod state;

pub use commands::{Command, CommandOutcome, CommandSink, Dispatcher};
pub use ids::{CommandId, GroupId, UnitId};
pub use server::{ServerConfig, ServerHandle, serve};
pub use state::StateCache;
