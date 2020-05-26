# ide-debug-driver

`ide-debug-driver` can be used to automate an IDE session. This is
particularly useful for profiling where you often want to test
long-running sessions to ensure that there are no leaks.

Sessions are configured using a YAML file, see
[sample-config.yaml](sample-config.yaml) for an example.

You can then run `ide-debug-driver` as `ide-debug-driver -c sample-config.yaml`.
