use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::commands::{CommandSink, Dispatcher};
use crate::proto::command_service_server::CommandServiceServer;
use crate::proto::group_service_server::GroupServiceServer;
use crate::proto::location_service_server::LocationServiceServer;
use crate::proto::simulation_service_server::SimulationServiceServer;
use crate::proto::unit_service_server::UnitServiceServer;
use crate::services::{
    CommandServiceImpl, GroupServiceImpl, LocationServiceImpl, SimulationServiceImpl,
    UnitServiceImpl,
};
use crate::state::StateCache;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub addr: SocketAddr,
    /// How long [`Dispatcher::send`] waits for the engine to acknowledge a
    /// command before reporting it as a failed "timeout".
    pub command_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], 50051)),
            command_timeout: Duration::from_secs(5),
        }
    }
}

/// How long [`ServerHandle::shutdown`] waits for tonic's graceful
/// connection drain before forcing the listener and any open connections
/// closed. Without this bound, a client that never closes its connection
/// (e.g. one sharing the same single-threaded runtime `shutdown` is called
/// from, which can starve the very task that would close it) would make
/// `shutdown` — and therefore `Drop` — hang forever.
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);

async fn serve_on(
    listener: TcpListener,
    state: Arc<StateCache>,
    dispatcher: Arc<Dispatcher>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let incoming = TcpListenerStream::new(listener);
    Server::builder()
        .add_service(CommandServiceServer::new(CommandServiceImpl::new(
            state.clone(),
            dispatcher.clone(),
        )))
        .add_service(GroupServiceServer::new(GroupServiceImpl::new(
            state.clone(),
        )))
        .add_service(LocationServiceServer::new(LocationServiceImpl::new(
            state.clone(),
        )))
        .add_service(SimulationServiceServer::new(SimulationServiceImpl::new(
            state.clone(),
        )))
        .add_service(UnitServiceServer::new(UnitServiceImpl::new(state)))
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await?;
    Ok(())
}

/// Runs the Minerva gRPC server on the current async runtime until
/// `shutdown` resolves. For embedding into an application that already owns
/// a tokio runtime; engine plugins normally use [`ServerHandle::spawn`]
/// instead, which owns its own runtime and thread.
pub async fn serve(
    config: ServerConfig,
    state: Arc<StateCache>,
    dispatcher: Arc<Dispatcher>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(config.addr).await?;
    serve_on(listener, state, dispatcher, shutdown).await
}

/// A running server on its own tokio runtime and OS thread. Shuts the server
/// down (and joins its thread) on [`ServerHandle::shutdown`] or when dropped.
pub struct ServerHandle {
    local_addr: SocketAddr,
    state: Arc<StateCache>,
    dispatcher: Arc<Dispatcher>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ServerHandle {
    /// Binds `config.addr` and starts serving on a dedicated thread. Returns
    /// once the socket is bound, so [`ServerHandle::local_addr`] reflects
    /// the actual port even when `config.addr` used port 0.
    ///
    /// Binding happens on the new thread rather than via `block_on` here, so
    /// this is safe to call from inside an existing tokio runtime (e.g. from
    /// an async test) as well as from plain synchronous code.
    pub fn spawn(config: ServerConfig, sink: Arc<dyn CommandSink>) -> anyhow::Result<Self> {
        let state = Arc::new(StateCache::new());
        let dispatcher = Arc::new(Dispatcher::new(sink, config.command_timeout));

        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<anyhow::Result<SocketAddr>>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let serve_state = state.clone();
        let serve_dispatcher = dispatcher.clone();
        let addr = config.addr;
        let thread = std::thread::Builder::new()
            .name("minerva-server".to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        let _ = ready_tx.send(Err(err.into()));
                        return;
                    }
                };
                runtime.block_on(async move {
                    let listener = match TcpListener::bind(addr)
                        .await
                        .and_then(|listener| Ok((listener.local_addr()?, listener)))
                    {
                        Ok((local_addr, listener)) => {
                            if ready_tx.send(Ok(local_addr)).is_err() {
                                return; // caller gave up waiting
                            }
                            listener
                        }
                        Err(err) => {
                            let _ = ready_tx.send(Err(err.into()));
                            return;
                        }
                    };
                    // `fired` lets both the graceful-shutdown trigger and the
                    // grace-period timer react to the same shutdown signal;
                    // a oneshot::Receiver can only be consumed by one of them.
                    let (fired_tx, fired_rx) = tokio::sync::watch::channel(false);
                    let mut forced_rx = fired_rx.clone();
                    tokio::spawn(async move {
                        let _ = shutdown_rx.await;
                        let _ = fired_tx.send(true);
                    });

                    let mut graceful_rx = fired_rx;
                    let graceful = async move {
                        let _ = graceful_rx.wait_for(|fired| *fired).await;
                    };
                    let forced = async move {
                        let _ = forced_rx.wait_for(|fired| *fired).await;
                        tokio::time::sleep(SHUTDOWN_GRACE_PERIOD).await;
                    };

                    tokio::select! {
                        result = serve_on(listener, serve_state, serve_dispatcher, graceful) => {
                            if let Err(err) = result {
                                tracing::error!(%err, "minerva-server: server task failed");
                            }
                        }
                        () = forced => {
                            tracing::warn!(
                                "minerva-server: shutdown grace period elapsed with connections \
                                 still open; forcing the listener closed"
                            );
                        }
                    }
                });
            })?;

        let local_addr = match ready_rx.recv() {
            Ok(result) => result?,
            Err(_) => anyhow::bail!("minerva-server: server thread exited before it was ready"),
        };

        Ok(Self {
            local_addr,
            state,
            dispatcher,
            shutdown_tx: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn state(&self) -> &Arc<StateCache> {
        &self.state
    }

    pub fn dispatcher(&self) -> &Arc<Dispatcher> {
        &self.dispatcher
    }

    /// Signals the server to stop and blocks until its thread exits.
    /// Idempotent: a second call is a no-op.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}
