# Haskell in Bazel

Finding your way around our Bazel build system from a Haskell developers' point of view might seem confusing at first. Going beyond just adding targets to `BUILD.bazel` files requires a more detailed understanding of the system:
  - Where rules come from;
  - How toolchains and external dependencies are defined;
  - Specifiying stock `bazel` command options.

For this, one needs awareness of four files at the root level of the DAML repository : `WORKSPACE`, `deps.bzl`, `BUILD` and `.bazelrc`.

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

*[Note : Confusingly (?), `//bazel_tools` is a DAML package (a sub-directory of the root package directory containing a `BUILD.bazel` file). Don't confuse `@bazel_tools//..` with `//bazel_tools/..`]*.

Straight after the loading of those rules, `deps.bzl` reads,
```
http_archive(
  name = "io_tweag_rules_haskell",
  strip_prefix = 'rules_haskell-%s' % rules_haskell_version,
  urls = ["https://github.com/tweag/rules_haskell/archive/%s.tar.gz" % rules_haskell_version],
)
```
This defines the workspace [`io_tweag_rules_haskell`](https://github.com/tweag/rules_haskell) (we call this "`rules_haskell`" informally - in short, build rules for Haskell) as an external workspace that is downloaded via http. From here on we can refer to things in that workspace by prefixing them with `@io_tweag_rules_haskell` as in the next command from `WORKSPACE`,
```
load("@io_tweag_rules_haskell//haskell:repositories.bzl", "haskell_repositories")
```
which has the effect of making the macro `haskell_repositories` available in the environment which provides "all repositories necessary for `rules_haskell` to function":
```
haskell_repositories()
```
As mentioned earlier, targets of any `BUILD.bazel` file in a package are visible within `WORKSPACE`. In fact, its a rule that [toolchains](https://docs.bazel.build/versions/master/toolchains.html#defining-toolchains) can only be defined in `BUILD.bazel` files and registered in `WORKSPACE` files. [`register_toolchains`](https://docs.Bazel.build/versions/master/skylark/lib/globals.html#register_toolchains) registers a toolchain created with the `toolchain` rule so that it is available for toolchain resolution.
```
register_toolchains(
  "//:ghc",
  "//:c2hs-toolchain",
)
```
Those toolchains are defined in `BUILD` (we'll skip listing their definitions here).

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

[`nixpkgs_package`](https://github.com/tweag/rules_nixpkgs#nixpkgs_package) provisions Bazel with our GHC toolchain from Nix:
```
nixpkgs_package(
  name = "ghc",
  attribute_path = "ghcStatic",
  nix_file = "//nix:default.nix",
  repositories = dev_env_nix_repos,
  build_file = "@ai_formation_hazel//:BUILD.ghc",
)
```

We see in the last macro invocation, forward reference to the [`ai_formation_hazel`]("https://github.com/DACH-NY/hazel) workspace. Here's its definition:
```
http_archive(
  name = "ai_formation_hazel",
  strip_prefix = "hazel-{}".format(hazel_version),
#  XXX: Switch to upstream once necessary changes are merged.
#  urls = ["https://github.com/formationai/hazel/archive/{}.tar.gz".format(hazel_version)],
  urls = ["https://github.com/DACH-NY/hazel/archive/{}.tar.gz".format(hazel_version)],
)
```
Hazel is a Bazel framework of build rules for third-party Haskell dependencies - it autogenerates Bazel rules from Cabal files. From the `@ai_formation_hazel` workspace we load
```
load("@ai_formation_hazel//:hazel.bzl", "hazel_repositories", "hazel_custom_package_hackage")
```
Immediately thereafter we load from the DAML ``//hazel`` "package":
```
load("//hazel:packages.bzl", "core_packages", "packages")
```
and from there we the DAML `//bazel_tools` project the `add_extra_packages` macro for packages on Hackage but not in Stackage:
```
load("//bazel_tools:haskell.bzl", "add_extra_packages")
```

The [`hazel_repositories`](https://github.com/DACH-NY/hazel#using-hazel-in-build-rules) macro creates a separate external dependency for each package. It downloads Cabal tarballs from Hackage and constructs build rules for compiling the components of each such package:
```
hazel_repositories(
  core_packages = core_packages (...),
  packages = add_extra_packages (...),
  ...
)
```
Note that [`ghc-lib`](https://github.com/digital-asset/daml/blob/master/ghc-lib/working-on-ghc-lib.md) is added here as one such "extra package".

We use `hazel_custom_package_hackage` if the default Bazel build that Hazel generates won't quite work and needs some overrides. The overrides go into a file that is pointed at by the `build_file` attribute:
```
hazel_custom_package_hackage(
  package_name = "clock",
  version = "0.7.2",
  sha256 = "886601978898d3a91412fef895e864576a7125d661e1f8abc49a2a08840e691f",
  build_file = "//3rdparty/haskell:BUILD.clock",
)
```

## `BUILD`

At the root of the repository, alongside `WORKSPACE` there exists the top-level package definition file `BUILD`. The primary purpose of this `BUILD` file is to define toolchains (but it does a couple of other little things as well).

The directive
```
package(default_visibility = ["//visibility:public"])
```
sets the default visibility property globally for our targets as `public`. This means that our targets can freely be depended upon by other targets.

The `load` statments
```
load("@io_tweag_rules_haskell//haskell:haskell.bzl",
  "haskell_toolchain", "haskell_toolchain_library",
)
load("@io_tweag_rules_haskell//haskell:c2hs.bzl",
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
  actual = "//daml-foundations/daml-tools/da-hs-damlc-app"
)
```

## Profiling

To produce a binary with profiling information, you need to pass `-c
dbg` to Bazel. E.g., `bazel build -c dbg damlc` will build a profiled
version of `damlc`. Note that by default Bazel won’t automatically add
cost centres in your code. To get cost centres, you can either add
[cost centres manually](https://downloads.haskell.org/~ghc/latest/docs/html/users_guide/profiling.html#inserting-cost-centres-by-hand)
or use one of the
[options provided by GHC](https://downloads.haskell.org/~ghc/latest/docs/html/users_guide/profiling.html#compiler-options-for-profiling)
to add them automatically.
You can either add those options in the `compiler_flags` section of a
specific target, modify the `da_haskell_library` wrapper in
`bazel_tools/haskell.bzl` to add a flag to all DAML targets and
libraries or use it for all targets by modifying the
`compiler_flags` in the `haskell_toolchain`.

## Further reading:

- ["Bazel User Guide"](https://github.com/DACH-NY/da/blob/master/BAZEL.md) (DAML specific)
- ["A Users's Guide to Bazel"](https://docs.bazel.build/versions/master/guide.html) (official documentation)
- [`rules_haskell` documentation](https://api.haskell.build/index.html) (core Haskell rules, Haddock support, Linting, Defining toolchains, Support for protocol buffers, Interop with `cc_*` rules, Workspace rules)
