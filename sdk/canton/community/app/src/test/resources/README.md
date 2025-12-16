Port Allocation
===============

As different tests run in parallel, every test suite needs unique ports.

The chosen ports should be between 4001 and 32767,
as the other ports tend to be either reserved or will be randomly chosen for client connections:
https://en.wikipedia.org/wiki/Ephemeral_port

## Dynamically allocated ports

For tests that do not need a known static port, the `ConfigTransforms.globallyUniquePorts` configuration transform
can be used to allocate a unique port within a test run.
