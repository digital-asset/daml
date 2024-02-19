# Working with Go in Bazel

## Dependencies

Go dependencies are managed by [`go.mod`][go-deps] and [Gazelle][gazelle].
To add a new Go dependency

1. Run `go get` to add the dependency to `go.mod`. E.g.
    ```
    go get github.com/envoyproxy/protoc-gen-validate@v0.6.2
    ```
    This should update `go.mod` and `go.sum`.
2. Run Gazelle to import the dependencies to Bazel.
    ```
    bazel run //:gazelle-update-repos
    ```
    This should update `go_deps.bzl`.

[go-deps]: https://go.dev/doc/modules/managing-dependencies
[gazelle]: https://github.com/bazelbuild/bazel-gazelle
