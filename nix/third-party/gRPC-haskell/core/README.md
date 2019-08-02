Attribution
-----------

This library is a fork of https://github.com/aloiscochard/grpc-haskell that we
have extended and released under the same [`LICENSE`](./LICENSE)

Installation
------------

**The current version of this library requires gRPC version 1.2.0. Newer versions may work but have not been tested.**

Usage
-----

There is a tutorial [here](examples/tutorial/TUTORIAL.md)

Building and testing
--------------------

If you want to use `stack` on MacOS, you will need to have GRPC installed
already -- see the [Installing GRPC for `stack`](#stackgrpc) section below. Any
example build and test recipes below which use `stack` assume you have already
performed the steps described in that section.

Without `stack`, `nix-build release.nix -A grpc-haskell` will build and test the
whole thing and put the completed package into the nix store. `nix-shell` can be
used to give you a development environment where you can use the `cabal` and
`stack` toolchains for development and testing:

```bash
$ nix-shell release.nix -A grpc-haskell.env
[nix-shell]$ cabal configure --enable-tests && cabal build && cabal test
```

```bash
$ nix-shell release.nix -A grpc-haskell.env
[nix-shell]$ stack build --fast && stack test --fast
```

Note that, for `stack`, the `nix-shell` environment is only needed to run the
tests, because it uses some custom python tooling (for grpc interop
testing). You should still be able to `stack build` without using the
`nix-shell` environment at all.

NB: You can also instruct `stack` to run the tests inside the `nix-shell`
environment directly, via `stack --nix test --fast`. However, this will
frequently rebuild the custom ghc that is used in `release.nix`, so is not
recommended during develop-debug cycles (use the `cabal` path for that, or
iterate within a `nix-shell`).

Finally, since `stack` does not use `nix` for any Haskell package dependencies,
be sure to update repository references for dependent packages such as
`protobuf-wire` in both `nix/<pkg>.nix` AND in `stack.yaml`.

<a name="stackgrpc"></a>Installing GRPC for `stack` (MacOS)
-----------------------------------------------------------

If you want to use `stack` in a relatively natural and painless manner, you will
need a working installation of the GRPC C core libraries.

On MacOS, because of
issues [related](https://github.com/commercialhaskell/stack/issues/1161) to
System Integrity Protection, dependencies on the nix-built `grpc` don't seem to
work properly in the stack toolflow when `DYLD_LIBRARY_PATH` refers to the
`nix`-built `grpc` library in the nix store, so the library needs to be
installed somewhere that the loader can pick it up without `DYLD_LIBRARY_PATH`
set (e.g., in `/usr/local/lib`).

There are basically two methods to accomplish this:

1. Run `bin/install-macos-nix-grpc.sh`.

  This script will build the same version of `grpc` used in `release.nix`, which
  is what is used in the end-to-end system and in our CI testing flows. It then
  pretends to be an impoverished version of `brew` and installs symlinks from
  the nix store into `/usr/local/include/grpc` and
  `/usr/local/lib/libgrpc.dylib`. It should be run manually whenever the `grpc`
  dependency is updated. Note that it is intentionally destructive to any
  existing `brew` installs of `grpc`.

  Is it an ugly hack? Yes, but it's better than having either a rootless system
  to circumvent SIP or building atop a version of `grpc` which may differ from
  CI and production environments (which may be the case with `brew`-installed
  grpc).

  After running this script, you should be able to use `stack` normally, in
  combination with a `nix-shell` environment for running the tests:

  ```bash
  $ bin/install-macos-nix-grpc.sh
  $ stack build --fast
  $ stack --nix test --fast
  ```

1. Use `brew`

  If you don't want to hack the global pathing yourself using the above script,
  you can rely on homebrew to do this for you instead. However, you will need to
  specify the version of the `grpc` release that you want to use.

  ```bash
  $ brew tap grpc/grpc
  $ brew edit grpc
  $ brew install grpc
  ```

  Make sure you select a release version that is reasonably close to our grpc
  dependency, e.g.:

  ```
  url "https://github.com/grpc/grpc/archive/release-0_15_0.tar.gz"
  sha256 "d02235dff278869e94cb0dcb31cfea935693c6f87bd73f43d44147185e6becdd"
  ```

  or

  ```
  url "https://github.com/grpc/grpc/archive/v1.0.1.tar.gz"
  sha256 "efad782944da13d362aab9b81f001b7b8b1458794751de818e9848c47acd4b32"
  ```

Using the Library
-----------------

You must compile with `-threaded`, because we rely on being able to execute
Haskell while blocking on foreign calls to the gRPC library. If not using code
generation, the recommended place to start is in the
`Network.GRPC.HighLevel.Server.Unregistered` module, where `serverLoop` provides
a handler loop.
