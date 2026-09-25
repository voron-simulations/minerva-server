# minerva-server

Reusable Rust library implementing the Minerva gRPC server: a state cache fed by a
simulation engine, and command dispatch back to it. Built on the
[`minerva-protocol`](https://github.com/voron-simulations/minerva-protocol) protobuf
contract (package `minerva.v1`, published to the [Buf Schema
Registry](https://buf.build/voron-simulations/minerva)).

Consumed as a library by engine-specific plugins, e.g.
[`minerva-arma3`](https://github.com/voron-simulations/minerva-arma3).

See [`AGENTS.md`](AGENTS.md) for development workflow.
