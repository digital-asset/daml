# Performance Driver

This project contains a Ledger application that can be used to load a ledger. We use
it in our integration tests, in particular the chaos and crash tests, but also for
our long running performance tooling.

The `scenarios` contains actual bootstrap scripts, which need to depend on enterprise/app, while the
drivers are a dependency of the enterprise/app test project.

Because of that it is split apart in two buckets, the `driver` and the `scenarios`.
