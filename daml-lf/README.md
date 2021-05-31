The unified Daml-LF interpreter and engine
==========================================

This package contains the canonical in-memory LF ASTs of both the public
interface and the whole contents, decoders from on-wire LF to those, and
an interpreter for LF.

Additionally a separate package is provided for a standalone
REPL allowing loading of .dalf files and interpretation of
pure functions, updates and scenarios.

We provide both Bazel-based and Sbt-based builds for this project.  The
Sbt builds are provided solely for development purposes, to facilitate
incremental compilation and IDE integration. You can simply import the
sbt project for development, but if making changes, know that the Bazel
build is the sole source of truth for CI and releases.

Components
----------

- `archive` contains the Protobuf definition of the LF format, and
  Protobuf utilities for reading it into a raw memory form. This should
  reflect the [official LF specification][] at any given time.  As with
  the LF specification, changes to the Protobuf definition are governed
  by the [Daml-LF Governance process][].

- `interface` is an ADT of the "public interface" of a given LF package,
  meaning its templates, their choices, and serializable data types in
  the package.  The ADT does *not* include `def`s or expressions.  A
  reader from the raw protobuf is included.  The ADT is usable from
  Java.

- `lfpackage` is the canonical LF ADT, containing all information about
  an LF package. Its main consumer is the `interpreter`, which compiles
  from this faithful representation of the protobuf LF archive into
  a lower-level AST that is then interpreted.

  The current plan with `lfpackage` is to be able to load both old and
  new LF versions into, so that the interpreter and other consumers can
  work with a common format.

  For most use cases `lfpackage` is too complex, and `interface` is more
  convenient; if you need the extra information, this is available,
  though, but without guarantees of stability.

- `transaction` holds ADTs related to the interpretation of LF, as
  `lfpackage` represents the *definitions* in LF.  The base of these is
  the Value ADT, representing serializable values (i.e. values of
  serializable LF type). Building on that is the Transaction ADT,
  representing ledger updates.

  Both have associated Protobuf definitions, also contained in this
  package, and are used in `interpreter` and `engine` respectively.

- `transaction-lib-test` supplies tools to generate transaction and
  the value and transaction ADTs provided by the `transaction` library.

- `data` contains utility datatypes used in the engine, and functions
  designed around specified LF semantics.  For example, if you want
  LF-compatible decimal handling, the `Decimal` API is a good source of
  useful functions.

- `data-scalacheck` supplies Scalacheck `Arbitrary`s for the custom
  collections provided by the `data` library.

- `interpreter` is the "unified interpreter" used for both the sandbox
  and the production ledger.  It is an efficient [CEK machine][],
  interpreting the `lfpackage` terms using a (non-serializable) internal
  value model, ultimately producing `transaction`s.  Most downstream
  will want to use `engine` in addition to this, because only the pure
  interpreter lives here.

- `engine` holds the ledger state on `interpreter`'s behalf and
  implements all of its public-facing aspects, such as the `Command`
  interface, events, and loaded packages.

- `scenario-interpreter` practically demonstrates why `interpreter` is
  separate from `engine`: it is a small set of library functions using
  `interpreter` to evaluate scenarios from an LF.

- `repl` is the below-described REPL, manipulating an internal engine
  state and running scenarios at your command.

- `testing-tools` helps you run scenarios from Scalatest.

[official LF specification]: spec/daml-lf-1.rst
[Daml-LF Governance process]: governance.rst
[CEK machine]: https://gist.github.com/ekmett/f081b5e36bac3fed1ea6b21eb25327c6

Building and testing
--------------------

Daml-LF uses Bazel to build and test the components. Please refer to top-level
`BAZEL.md` and `BAZEL-JVM.md` documents for high-level instructions on how to
use Bazel and how it works within IntelliJ.

To get a list of build targets:
```
bazel query //daml-foundations/daml-lf/...
```

To build and test everything:
```
bazel build //daml-foundations/daml-lf/...
bazel test //daml-foundations/daml-lf/...
```

To watch a target and re-run tests when files change:
```
ibazel test //daml-foundations/daml-lf/...
```

All the above can of course take more fine-grained targets as arguments.
"..." means all targets under this directory, recursively. ":all" would
specify all targets in the specified directory.

To load a package in the scala repl you will need to add a "@repl" target
to BUILD.bazel:
```
load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repl")

scala_repl(
  name = "interpreter@repl",
  deps = [
    ":interpreter"
  ]
)
```

This target can then be invoked with "bazel run":
```
da$ bazel run //daml-lf/interpreter:interpreter@repl

or:

interpreter$ bazel run interpreter@repl
```

Since "rules_scala" does not currently support incremental compilation
you will need to help Bazel along a bit by keep the dependency graph
lean. Try to divide your code into separate scala_library targets as
build results are cached at this level. Preferably unrelated modules
should be separate scala_library targets, unvisible to the outside.
A visible scala_library target should then collect the unrelated modules
into a single target that can be depended on from outside.

Benchmarking
------------

Benchmarks for scenario execution can be run with
```
bazel run //daml-lf/scenario-interpreter:scenario-perf
```
A run of this benchmark will take between 6 and 7 minutes. A faster, less
precise benchmark which takes around 1 minute can be invoked with
```
bazel run //daml-lf/scenario-interpreter:scenario-perf -- -f 0
```
To benchmark scenarios other than the ones configured by default, you can
invoke
```
bazel run //daml-lf/scenario-interpreter:scenario-perf -- -p dar=/path/to/some/dar -p scenario=Some.Module:test
```
This can be combined with the `-f 0` flag as well.

These benchmarks are focused on Daml execution speed and try to avoid noise
caused by, say, I/O as much as possible.

Daml-LF-REPL Usage
------------------

The REPL can be compiled with `bazel build //:daml-lf-repl` and run with
`bazel run //:daml-lf-repl -- repl`. The `//:` prefix is not needed when
at repository root.

Example use:

$ bazel run //:daml-lf-repl -- repl
daml> :load project.dar
daml> Project.double 4
8
daml> :scenario Project.tests
...

See `:help` for more instructions.

The REPL application also provides commands `test` and `testAll` for
running scenarios in packages:

$ bazel run //:daml-lf-repl -- testAll $PWD/project.dalf
$ bazel run //:daml-lf-repl -- test Project.tests $PWD/project.dalf

NOTE: When running via `bazel run` one needs to specify full path (or relative path from repo root), since Bazel runs all commands from repository root.

Profiling scenarios
-------------------

Daml-LF-REPL provides a command to run a scenario and collect profiling
information while running it. This information is then written into a file that
can be viewed using the [speedscope](https://www.speedscope.app/) flamegraph
visualizer. The easiest way to install speedscope is to run
```shell
$ npm install -g speedscope
```
See the [Offline usage](https://github.com/jlfwong/speedscope#usage) section of
its documentation for alternatives.

Once speedscope is installed, the profiler can be invoked via
```shell
$ bazel run //:daml-lf-repl -- profile Module.Name:scenarioName /path/to.dar /path/to/output.json
```
and the profile viewed via
```shell
$ speedscope /path/to/output.json
```


Scala house rules
-----------------

* _Do not_ use `Seq` in the interpreter's code paths, with the possible
    exceptions of accepting inputs in external APIs. Use `ImmArray`,
    `FrontStack`, and `BackStack` as appropriate.

    The reason for this rule is that `Seq` hides completely the
    performance of operations -- for example it defines cons and snoc
    and append for all structures even if it requires a full copy for an
    array.

    `ImmArray` should be used in cases where you do not need to append
    or prepend content often. It is however very cheap to slice the
    `ImmArray` (removing elements from either end).

    `FrontStack` should be used when needing to build up a list of
    elements by prepending elements. Both single elements or chunks
    in the form of `ImmArray` can be prepended. A typical use case
    if traversing a tree in topological order by keeping a stack of
    children to still be visited.

    `BackStack` is like `FrontStack` but you can append rather than
    prepend. For example if you find yourself building and then
    reversing a list, use `BackStack` instead.

* Avoid mutable data structures in external APIs. This is not set in
    stone but generally a code smell.

* Try to always define functions on user-provided data structures (of
    which we have a lot of in this codebase) to be tail recursive. The
    typical way to do this is by defining little "interpreters" to perform
    your function. XXX put good example here once we have an established
    pattern. In doubt, ask Francesco Mazzoli or Gyorgy Farkas about this.

* Disable "Optimize imports on the fly" and the "Optimize Imports" shortcut in
    IntelliJ IDEA, since they mess up diffs and can subtly, insidiously change
    the semantics of your code (Scala imports are order sensitive). You can
    disable "on the fly" at Menu -> Preferences -> Editor -> General -> Auto
    Import -> Scala, and the shortcut key in Preferences -> Keymap -> search
    Optimize Imports -> double-click result -> Remove.
