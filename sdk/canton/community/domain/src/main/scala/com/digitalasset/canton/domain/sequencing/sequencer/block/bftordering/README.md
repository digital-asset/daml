# BFT ordering service

A sequencer driver that implements BFT ordering based on [ISS].

Currently, the Pekko actors-based architecture is present but the implementation is fake; in particular, the consensus
module reuses the reference driver's storage implementation that atomically appends blocks to a DB table.

Documentation can be found in the [docs](docs) folder and design documents in the [design](docs/design) subfolder.

[ISS]: https://arxiv.org/abs/2203.05681
