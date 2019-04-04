{ system ? builtins.currentSystem }:
(import ./bazel.nix { inherit system; }).bazel-cc-toolchain
