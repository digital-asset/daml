# Haskell in Bazel

Finding your way around our Bazel build system from a Haskell developers' point of view might seem confusing at first. Going beyond just adding targets to `BUILD.bazel` files requires a more detailed understanding of the system:
  - Where rules come from;
  - How toolchains and external dependencies are defined;
  - Specifiying stock `bazel` command options.

For this, one needs awareness of four files at the root level of the Daml repository : `WORKSPACE`, `deps.bzl`, `BUILD` and `.bazelrc`.

## `.bazelrc` the Bazel configuration file

The `bazel` command accepts many options. To avoid having to specify them manually for every build they can be collected into a [`.bazelrc`](https://docs.bazel.build/versions/master/guide.html) file. The root of `daml.git` contains such a file. There doesn't seem to be anything in ours that is Haskell specific.

## `WORKSPACE`
The root of `daml.git` is a Bazel "workspace" : there exists a file `WORKSPACE`. In short, in a `WORKSPACE` we declare external packages and register toolchains. Visible in a `WORKSPACE` are the targets of the `BUILD.bazel` file at the same level as `WORKSPACE` and any `BUILD.bazel` files contained in sub-directories of the directory containing `WORKSPACE`.

Bazel extensions are loaded by a `load` statement. More or less the first couple of lines of our `WORKSPACE` reads:
```
load("//:deps.bzl", "daml_deps")
daml_deps()
```
Much of the contents of the `WORKSPACE` file have been factored out into `deps.bzl` so that other projects can share the definitions contained there. Looking into `deps.bzl` it begins:
```
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
```
This loads the contents of the files `http.bzl` and `git.bzl` from the external workspace [`bazel_tools`](https://github.com/bazelbuild/bazel/tree/master/tools) into the "environment". `bazel_tools` is an external workspace builtin to Bazel and provides rules for working with archives and git.

*[Note : Confusingly (?), `//bazel_tools` is a Daml package (a sub-directory of the root package directory containing a `BUILD.bazel` file). Don't confuse `@bazel_tools//..` with `//bazel_tools/..`]*.

Straight after the loading of those rules, `deps.bzl` reads,
```
http_archive(
  name = "rules_haskell",
  strip_prefix = 'rules_haskell-%s' % rules_haskell_version,
  urls = ["https://github.com/tweag/rules_haskell/archive/%s.tar.gz" % rules_haskell_version],
)
```
This defines the workspace [`rules_haskell`](https://github.com/tweag/rules_haskell) (we call this "`rules_haskell`" informally - in short, build rules for Haskell) as an external workspace that is downloaded via http. From here on we can refer to things in that workspace by prefixing them with `@rules_haskell` as in the next command from `WORKSPACE`,
```
load("@rules_haskell//haskell:repositories.bzl", "rules_haskell_dependencies")
```
which has the effect of making the macro `rules_haskell_dependencies` available in the environment which provides "all repositories necessary for `rules_haskell` to function":
```
rules_haskell_dependencies()
```
As mentioned earlier, targets of any `BUILD.bazel` file in a package are visible within `WORKSPACE`. In fact, its a rule that [toolchains](https://docs.bazel.build/versions/master/toolchains.html#defining-toolchains) can only be defined in `BUILD.bazel` files and registered in `WORKSPACE` files. [`register_toolchains`](https://docs.Bazel.build/versions/master/skylark/lib/globals.html#register_toolchains) registers a toolchain created with the `toolchain` rule so that it is available for toolchain resolution.
```
register_toolchains(
  "//:c2hs-toolchain",
)
```
Those toolchains are defined in `BUILD` (we'll skip listing their definitions here).

The GHC toolchain is registered within macros provided by `rules_haskell`:
```
haskell_register_ghc_nixpkgs(
    attribute_path = "ghc",
    build_file = "@io_tweag_rules_nixpkgs//nixpkgs:BUILD.pkg",
    compiler_flags = [ ... ],
    ...
    version = "8.6.5",
)
haskell_register_ghc_bindists(
    compiler_flags = common_ghc_flags,
    version = "8.6.5",
) if is_windows else None
```
On Linux and macOS we import GHC from nixpkgs, while on Windows we download an
official bindist.

Rules for importing nix packages are provided in the workspace `io_tweag_rules_nixpkgs`:
```
http_archive(
    name = "io_tweag_rules_nixpkgs",
    strip_prefix = "rules_nixpkgs-%s" % rules_nixpkgs_version,
    urls = ["https://github.com/tweag/rules_nixpkgs/archive/%s.tar.gz" % rules_nixpkgs_version],
)
load(
  "@io_tweag_rules_nixpkgs//nixpkgs:nixpkgs.bzl",
  "nixpkgs_local_repository", "nixpkgs_git_repository", "nixpkgs_package", "nixpkgs_cc_configure",
)
```

[`nixpkgs_local_repository`](https://github.com/tweag/rules_nixpkgs#nixpkgs_local_repository) creates an external repository representing the content of of a Nix package collection, based on Nix expressions stored in files in our `//nix` directory.
```
nixpkgs_local_repository(
    name = "nixpkgs",
    nix_file = "//nix:bazel-nixpkgs.nix",
)
nixpkgs_local_repository(
    name = 'dev_env_nix',
    nix_file = '//nix:default.nix',
)
```

[`nixpkgs_cc_configure`](https://github.com/tweag/rules_nixpkgs#nixpkgs_cc_configure) tells Bazel to use compilers and linkers from the Nix package collection for the CC toolchain (overriding auto-detection from the current `PATH`):
```
nixpkgs_cc_configure(
    nix_file = "//nix:bazel-cc-toolchain.nix",
    repositories = dev_env_nix_repos,
)
```
where,
```
dev_env_nix_repos = {
    "nixpkgs": "@nixpkgs",
    "damlSrc": "@dev_env_nix",
}
```

Finally, we use the `bazel-haskell-deps.bzl` file which is loaded from
`WORKSPACE` to define the set of Hackage packages that we want to import into
Bazel using the `stack_snapshot` macro.
```
stack_snapshot(
    name = "stackage",
    packages = [
        "aeson",
        "aeson-pretty",
        ...
    ],
    vendored_packages = {
        "grpc-haskell-core": "@grpc_haskell_core//:grpc-haskell-core",
        "proto3-suite": "@proto3-suite//:proto3-suite",
    },
    local_snapshot = "//:stack-snapshot.yaml",
    stack_snapshot_json = "//:stackage_snapshot.json",
    flags = {
        "integer-logarithms": ["-integer-gmp"],
        "text": ["integer-simple"],
        ...
    },
    tools = [
        "@alex",
        "@happy",
        ...
    ],
    deps = {
        "digest": ["@com_github_madler_zlib//:libz"],
        "zlib": ["@com_github_madler_zlib//:libz"],
    },
```
This will generate an external workspace called `@stackage` that exports all
the Hackage packages listed in `packages` or `vendored_packages`. We use a
custom stack snapshot defined in `stack-snapshot.yaml`. The items listed in the
`packages` attribute will be fetched using the `stack` tool as defined in the
custom snapshot and will be built using the `Cabal` library. Additionally, we
can provide custom Bazel build definitions for packages using the
`vendored_packages` attribute.

The packages are pinned by the Stackage snapshot, in this case a
`local_snapshot` and in the lock-file defined by `stack_snapshot_json`. If you
wish to update packages, then you need to change the `packages` and
`local_snapshot` attributes accordingly and afterwards execute the following
command on Unix and Windows to update the lock-files:
```
bazel run @stackage-unpinned//:pin
```

You can use the ad-hoc Windows machines as described in the [release
documentation][windows-ad-hoc] to get access to a Windows machine.

[windows-ad-hoc]: ./release/RELEASE.md#tips-for-windows-testing-in-an-ad-hoc-machine

The `flags` attribute can be used to override default Cabal flags. The `tools`
attribute defines Bazel targets for known Cabal tools, e.g. `alex`, `happy`, or
`c2hs`. Finally, the `deps` attribute can be used to define additional
dependencies to individual packages. E.g. the `zlib` Hackage packages depends
on the C library `libz`.

If you wish to override the version of a package that is fetch from Hackage, or
fetch it from a different source such as GitHub at a specific commit, then you
should modify the `stack-snapshot.yaml` file. If, additionally, you wish to
patch a package, e.g. to override Cabal version bounds, then you should define
a custom Bazel build and add the package to the `vendored_packages` attribute.

For example, to patch the `proto3-suite` package add the following snippet to
the `bazel-haskell-deps.bzl` file.
```
http_archive(
    name = "proto3_suite",
    build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "proto3-suite",
    version = "0.4.0.0",
    srcs = glob(["**"]),
    deps = packages["proto3-suite"].deps,
    visibility = ["//visibility:public"],
)
    """,
    patch_args = ["-p1"],
    patches = ["@com_github_digital_asset_daml//bazel_tools:haskell-proto3-suite.patch"],
    sha256 = "6a803b1655824e5bec2c518b39b6def438af26135d631b60c9b70bf3af5f0db2",
    strip_prefix = "proto3-suite-f5ca2bee361d518de5c60b9d05d0f54c5d2f22af",
    urls = ["https://github.com/awakesecurity/proto3-suite/archive/f5ca2bee361d518de5c60b9d05d0f54c5d2f22af.tar.gz"],
)
```
This will fetch the sources from GitHub at the specified revision and apply the
patch located in `bazel_tools/haskell-proto3-suite.patch` in the `daml`
repository.

## `BUILD`

At the root of the repository, alongside `WORKSPACE` there exists the top-level package definition file `BUILD`. The primary purpose of this `BUILD` file is to define toolchains (but it does a couple of other little things as well).

The directive
```
package(default_visibility = ["//visibility:public"])
```
sets the default visibility property globally for our targets as `public`. This means that our targets can freely be depended upon by other targets.

The `load` statments
```
load("@rules_haskell//haskell:defs.bzl",
  "haskell_toolchain", "haskell_toolchain_library",
)
load("@rules_haskell//haskell:c2hs.bzl",
  "c2hs_toolchain",
)
```
bring the macros `haskell_toolchain`, `haskell_toolchain_library`, and `c2hs_toolchain` into scope from `rules_haskell`.

`haskell_toolchain_library`:
- import a package that is prebuilt outside of Bazel

`haskell_toolchain`:
-  declare a GHC compiler toolchain

`c2hs_toolchain`:
- declare a Haskell `c2hs` toolchain

Lastly, there are some aliases defined here. For example,
```
alias(
  name = "damlc",
  actual = "//compiler/damlc"
)
```

## Editor integration

The `daml` repository is configured to support [`haskell-language-server`][hls]
with Bazel and the `da-hls` executable is provided by the `dev-env`. Take a look
at the [setup section][hls_setup] for example configurations for various
editors. `haskell-language-server` has to be built with the same `ghc` as the
project you're working on. Be sure to either point your editor to the
`dev-env`-provided `haskell-language-server` by absolute path, or make sure that
the `dev-env`-provided `haskell-language-server` is in `$PATH` for your editor.

Note, `hls` itself is built by Bazel and to load a target into the editor
some of its dependencies have to be built by Bazel. This means that start-up
may take some time if the required artifacts are not built or cached already.

Also note that the current setup works for modules in the bazel target
`//compiler/damlc:damlc` or in its dependencies. To work on other modules, it
should be enough to replace `//compiler/damlc:damlc` in `.hie-bios` with the
appropriate bazel target and restart the language server.

[hls]: https://github.com/haskell/haskell-language-server
[hls_setup]: https://haskell-language-server.readthedocs.io/en/latest/configuration.html#vs-code

## Further reading:

- ["Bazel User Guide"](https://github.com/digital-asset/daml/blob/main/BAZEL.md) (Daml specific)
- ["A Users's Guide to Bazel"](https://docs.bazel.build/versions/master/guide.html) (official documentation)
- [`rules_haskell` documentation](https://api.haskell.build/index.html) (core Haskell rules, Haddock support, Linting, Defining toolchains, Support for protocol buffers, Interop with `cc_*` rules, Workspace rules)
