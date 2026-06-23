# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

This is the **Daml SDK** monorepo (Digital Asset). Nearly all code lives under **`sdk/`**, which is
the Bazel workspace (`WORKSPACE`, `.bazelrc`, `.envrc` are there). The repo **`.git` is at the root**
(the parent of `sdk/`). Practical consequence: **run all build/test/format commands from `sdk/`**, but
run `git` from anywhere in the tree.

## Toolchain: dev-env (Nix) + Bazel

Tools (Bazel, JDK, Scala, GHC/Haskell, Node, Python) are pinned and provided by a Nix-based
`dev-env` — do **not** install Bazel/Java/etc. yourself, and do not run `nix develop` directly. The
`bazel` you use is daml's Nix wrapper around Bazel 7.

Load the dev-env once per shell (this is what `.envrc` does, minus `use nix`):

```bash
cd sdk
eval "$(dev-env/bin/dade assist)"   # puts bazel/jdk/scala/node/python on PATH
```

For a single one-off command you can skip it — `dev-env/bin/bazel …` self-bootstraps the dev-env.

## Build / test / format

All from `sdk/`, with dev-env loaded:

```bash
bazel build //...                                   # build everything (slow first time)
bazel test  //...                                   # run all hermetic tests
./build.sh                                          # full lint + build + test (CI entry point; sets up PostgreSQL)
./fmt.sh                                             # all formatters/linters: scalafmt, hlint, google-java-format, yapf, prettier, buildifier, copyright headers
./fmt.sh --test                                     # check-only (what CI runs)
```

Scope to what you're touching — full builds are large:

```bash
bazel build //compiler/damlc:damlc                  # single target
bazel build //compiler/damlc/...                    # a subtree
bazel test  //daml-lf/data:data-test                # a single test target
```

### Running a single test case (Scala via ScalaTest runner)

`da_scala_test_suite` creates one Bazel target per source file. Pass ScalaTest args through
`--test_arg` (`-t` = exact test name, `-z` = substring), e.g.:

```bash
bazel test //canton/community/daml-lf/engine:tests_..._ApiCommandPreprocessorSpec.scala \
  --test_arg=-z --test_arg="preserve divulged"
bazel test //some:target --test_output=streamed       # see live output
bazel test //some:target --nocache_test_results       # force re-run
```

`bazel query //ledger/...` and `bazel query 'tests(//ledger/...)'` help locate targets. See
`sdk/BAZEL.md` for the full Bazel reference (queries, deps graphs, ibazel continuous build).

### Local SDK install

`daml-sdk-head` installs a local SDK as version `0.0.0`; set `version: 0.0.0` in a Daml project to
use it. `daml-sdk-head --profiling` (or `bazel build -c dbg <tgt>`) for Haskell profiling builds.

## Architecture (big picture)

The SDK spans a **Haskell compiler** front end and a **Scala/JVM runtime** back end, glued by
**Daml-LF**, the typed intermediate language.

Compilation pipeline — Daml source → Daml-LF → DAR:
- `compiler/damlc/` — the `damlc` compiler (Haskell, `da_haskell_binary`). Wraps `ghcide`/GHC
  (`ghc-lib`), runs `daml-preprocessor`, then `daml-lf-conversion` turns GHC Core into Daml-LF.
  Entry point `DA.Cli.Damlc`; commands `compile`/`build`/`test`/`docs`.
- `compiler/daml-lf-ast/` — Daml-LF AST/type system in Haskell. `compiler/daml-lf-proto*` —
  protobuf (de)serialization. A built `.dar` is a zip of `.dalf` (protobuf Daml-LF) + `.daml` sources.
- `daml-lf/` — Daml-LF version/feature definitions (`daml-lf-versions.json`, `daml-lf-features.json`).
- `daml-assistant/` — the user-facing `daml` CLI and `daml.yaml`/SDK config handling.
- `daml-script/runner/` — executes Daml test scripts against a ledger (Scala).

Runtime / JVM side (Scala 2.13, built with `da_scala_library`/`da_scala_binary`/`da_scala_test_suite`):
- `canton/` — **Canton**, the distributed ledger runtime that executes Daml-LF transactions. It is
  **vendored/synced**, not authored here: pinned by `canton/canton_version.bzl`
  (`CANTON_OPEN_SOURCE_TAG`/`_SHA`) and pulled at build time via `canton/canton_jar.bzl`. Dev sync
  scripts: `pull-local-canton-to-daml.sh` / `push-daml-to-local-canton.sh`.
- `ledger-service/` — JSON Ledger API bridge over Canton's gRPC API (e.g. `lf-value-json`).
- `language-support/java` and `language-support/js` — codegen: turn a DAR's Daml-LF into Java and
  TypeScript/JS client bindings (`dar_to_java`, JS codegen).
- `test-common/` — shared test DARs and Canton integration-test libraries.

Bazel rule macros live in `sdk/bazel_tools/` (`scala.bzl`, `haskell.bzl`) and `sdk/rules_daml/`
(`daml` rule). JVM deps are declared in `bazel-java-deps.bzl` and pinned in
`maven_install_2.13.json` (`bazel run @unpinned_maven//:pin` to update).

## Conventions

- **DCO sign-off is required** — always commit with `-s`: `git commit -s -m "…"`. Verify
  `git config user.name` / `user.email` are set first.
- **Commit messages**: concise imperative title; body explains rationale; add `Fixes #NN` to link an
  issue (see `CONTRIBUTING.md`). Use `TODO(#NN)` / `FIXME(#NN)` in code.
- **CI test scope**: PRs run a reduced test set by default. Add a `run-all-tests: true` trailer to the
  commit message to run the full suite; `regenerate-stackage: true` to repin Stackage (see
  `build.sh`). Do not invent other trailers.
- Run `./fmt.sh` (and fix any leftovers) before pushing — CI enforces formatting, linting, and
  copyright headers.
- **Docs**: edit `sdk/docs/manually-written/`; run `sdk/ci/synchronize-docs.sh` to sync into
  `sdk/docs/sharable/`. Prose is linted with Vale.
- **AI policy** (`AI_POLICY.md`): you are accountable for every line submitted — understand and be
  able to justify it; self-review AI output aggressively; PRs opened without a named human in the
  loop need two human reviews.
