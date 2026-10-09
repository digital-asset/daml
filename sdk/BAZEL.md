# Bazel User Guide

This document explains how to use the Bazel build system on the Daml repository
from a users perspective. I.e. assuming the project you are working on has
already been ported to Bazel.

This guide does not cover how to port a new project to Bazel. Please refer to
the [Bazel JVM Porting Guide][bazel_jvm_guide] if you intend to port a JVM
project to Bazel.

[bazel_jvm_guide]: ./BAZEL-JVM.md

## Setup

This section goes through the required steps for a basic but fully functioning
setup of the Bazel build system for work on the Daml repository. Additional setup
as for the IntelliJ integration is listed in its own section below.

### Bazel Executable

Bazel is incorporated in the dev-env. If the [dev-env is setup
correctly][dev_env_guide] and `dev-env/bin` is in your `$PATH`, then Bazel
should be ready to use.

[dev_env_guide]: ./README.md

## Build and Test

Once setup is complete, you can build the whole repository with the following
command.

```
bazel build //...
```

You can run all hermetic tests in the repository with the following command.

```
bazel test //...
```

## Bazel Core Concepts

If you are unfamiliar with Bazel it is recommended that you read the official
[Concepts and Terminology guide][bazel_core_concepts]. Here we will only
provide a brief overview which may serve as a refresher.

[bazel_core_concepts]: https://docs.bazel.build/versions/master/build-ref.html

In short, the `daml` repository is a Bazel *workspace*. It contains a `WORKSPACE`
file, which defines external dependencies. The workspace contains several
*packages*. A package is a directory that contains a `BUILD.bazel` or `BUILD`
file. Each package holds multiple *targets*. Targets are either *files* under
the package directory or *rules* defined in the `BUILD.bazel` file. You can
address a target by a *label* of the form `//path/to/package:target`. For
example, `//daml-script/runner:daml-script-binary`. Here `daml-script-binary` is a target in the package
`//daml-script/runner`. It is defined in the file `//daml-script/runner/BUILD.bazel`
using `da_scala_binary` as shown below.

```
da_scala_binary(
    name = "daml-script-binary",
    main_class = "com.digitalasset.daml.lf.engine.script.ScriptMain",
    resources = glob(["src/main/resources/**/*"]),
    scala_runtime_deps = [
        "@maven//:org_apache_pekko_pekko_slf4j",
    ],
    scalacopts = lf_scalacopts_stricter,
    visibility = ["//visibility:public"],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    tags = ["ee-jar-license"],
    deps = [
        ":script-runner-lib",
    ],
)
```

The arguments to `da_scala_binary` are called *attributes*. These define the
name of the target, the sources it is compiled from, its dependencies, etc.
Note, that Bazel build rules are hermetic. I.e. only explicitly declared
dependencies will be available during execution. In particular, if a rule
depends on additional data files, then these have to be declared dependencies
as well. For example using the `resources` or the `data` attributes. The
details depend on the rule in question.

The following rules are commonly used in this repository. For Scala projects
`da_scala_library`, `da_scala_test_suite`, `da_scala_binary`. For Java projects
`java_library`, `java_test_suite`, `java_binary`. For Daml projects `daml`.

Labels can point to a specific target, or to a set of targets using a
wild-card. The following wild-card patterns are recognized.

- Ellipsis (`//some/package/...`):
    All rule targets within or underneath `some/package`.
- All (`//some/package:all`):
    All rule targets within `some/package`.
- Star (`//some/package:*`):
    All rule or file targets within `some/package`.

So far we have talked about targets defined in the current workspace. Bazel
also has a notion of external workspaces. Targets in external workspaces are
labelled as `@workspace_name//path/to/package:target`.

Targets have a *visibility* attribute that determines which other targets can
depend on it. Targets can be

- private (`//visibility:private`)
    Only targets in the same package can depend on it.
- visible to specific packages (`//some/package:__pkg__`)
    Only targets within `//some/package` can depend on it.
- visible to sub-packages (`//some/package:__subpackages__`)
    Only targets within or underneath `//some/package` can depend on it.
- public (`//visibility:public`)
    Any target in any package can depend on it.

Visibility should be kept as strict as possible to help maintain a clean
dependency graph.

Bazel files are written in a language called *Starlark*. It is very similar to
Python. However, Starlark programs cannot perform arbitrary input and output,
and build files are not allowed to use control structures (`for`, `if`, etc.),
or define functions. These restrictions are in place to ensure hermeticity.

[bazel_visibility]: https://docs.bazel.build/versions/master/be/common-definitions.html#common.visibility

## IntelliJ Integration

Make sure to go through the Bazel setup section and to familiarize yourself
with Bazel's core concepts as explained in the sections above, before you
proceed to the IntelliJ integration.

### Setup

If you use the IntelliJ IDE you should install the [Bazel integration plugin
provided by Google][intellij_plugin].

Not every version of Intellij works with the Bazel plugin. Intellij IDEA 2021.2.4 has been verified to work correctly with the Bazel plugin.
It is advisable to use the JetBrains Toolbox to manage installations of Intellij IDEA.

Follow the [installation instructions][intellij_plugin_install] in the official documentation. In short:
Install the plugin from within the IDE (`Settings > Plugins > Marketplace`, and
search for 'Bazel'). Multiple Bazel plugins exist, make sure to select the Bazel
plugin referencing [ij.bazel.build][intellij_plugin].

If the correct plugin does not exist in the list, then your IntelliJ version
might be too recent, and the Bazel plugin might not have been upgraded to
support it, yet. Check for version compatibility on the [JetBrains plugin
page][intellij_plugin_jetbrains].

Set the Bazel binary in the Bazel plugin settings, at `Preferences -> Bazel Settings -> Bazel binary location` to point to the Bazel binary in the `dev-env/bin` directory.

[intellij_plugin]: https://ij.bazel.build/
[intellij_plugin_install]: https://ij.bazel.build/docs/bazel-plugin.html#getting-started
[intellij_plugin_jetbrains]: https://plugins.jetbrains.com/plugin/8609-bazel

#### Side note: code formatting

While not relevant to setup IntelliJ to work with Bazel, if you plan to work on Java
code you are also advised to install the [google-java-format plugin][intellij_plugin_javafmt].
This plugin integrates the IntelliJ code formatting actions with the
[Google Java Style Guide][google_java_style_guide], which is enforced on CI with
the [google-java-format tool][google_java_format_tool].

[intellij_plugin_javafmt]: https://plugins.jetbrains.com/plugin/8527-google-java-format
[google_java_style_guide]: https://google.github.io/styleguide/javaguide.html
[google_java_format_tool]: https://github.com/google/google-java-format

### Importing a project

To import a Bazel project into IntelliJ select "Import Bazel Project" in the
welcome dialog, or `File > Import Bazel Project` in the editor window. In the
import dialog under "Workspace:" enter the path to the Daml repository root.

The Bazel IntelliJ integration uses a *project view* file to define the list of
directories and targets to make accessible in IntelliJ and to control other
aspects of the project. Refer to the [official
documentation][intellij_project_view] for a detailed description.

Choose the "Generate from BUILD file" option and select the `BUILD.bazel` file
of the project that you will be working on. Then, click on "Next".

The following dialog allows you to define the project name, or infer it, and to
set the location of the project data directory. It also allows you to modify
the default project view file. The default should have the following structure:

```
directories:
  .

# Automatically includes all relevant targets under the 'directories' above
derive_targets_from_directories: true

targets:
  # If source code isn't resolving, add additional targets that compile it here

additional_languages:
  # Uncomment any additional languages you want supported
  # ...
```

Make sure to add Scala, or other languages that you require, to the
`additional_languages` section. The section will be pre-populated with a list
of comments specifying the automatically detected supported languages.

Also ensure that you install and setup the Scala plugin.
If you'd like to work with all directories, we recommend the following project
view configuration, which stops IntelliJ from indexing the Bazel cache, and
avoids rebuilding the documentation:

```
directories:
  .
  -docs

# Automatically includes all relevant targets under the 'directories' above
derive_targets_from_directories: true

targets:
  # If source code isn't resolving, add additional targets that compile it here

additional_languages:
  javascript
  python
  scala
  typescript
```

However, you can also provide an allowed list of directories for a faster
experience.

If you wish to define a specific set of targets to work, then you can list
these in the `targets` section. This is not usually necessary, as they
will be derived automatically.

If you choose to limit the directories, you might end up with a project view
file looking like this:

```
directories:
  ledger/sandbox
  # ...

# Automatically includes all relevant targets under the 'directories' above
derive_targets_from_directories: true

targets:
  # If source code isn't resolving, add additional targets that compile it here

additional_languages:
  scala
```

Click "Next" once you are ready. You will be able to modify the project view
file whenever you like, so don't worry too much.

The first import of the project might fail due to a resolution error of the
`bazel` binary. In order to solve this, configure the Bazel plugin settings
with the location of the `bazel` binary,
by setting _Preferences_ → _Bazel Settings_ → _Bazel binary location_
to `./dev-env/bin/bazel`.

Now, re-trigger a sync of the workspace (IntelliJ Action:
_Sync project with BUILD files_). This process will take a while.

[intellij_project_view]: https://ij.bazel.build/docs/project-views.html

### Configuring the JDK in IntelliJ

Daml downloads the version of the JDK it uses from Nix. A symlink will be
created by the dev-env utilities (make sure you've set these up) in
_dev-env/jdk_.

TO configure IntelliJ to use this JDK:

1. Open the _Project Structure_ window.
2. Under _Platform Settings_, select _SDKs_.
3. Press the _plus_ button and select "Add JDK".
4. Choose the _dev-env/jdk_ directory.
5. Name it "Daml JDK" or something similar.
6. Ensure there's sources attached under the _Sourcepath_ tab. If not, add them.
   Press the _plus_ button and select _dev-env/jdk/lib/openjdk/src.zip_.
7. Open _Project Settings_ →  _Project_.
8. Select the Daml JDK from the _Project SDK_ list.

### Overview over Bazel IntelliJ Integration

The IntelliJ interface should largely look the same as under SBT. However, the
main menu will have an additional entry for Bazel, and a Bazel toolbar is
provided for quick access to common tasks.

The most commonly required operations are described below. Refer to the [plugin
documentation][intellij_plugin_docs] for further information.

[intellij_plugin_docs]: https://ij.bazel.build/docs/bazel-plugin.html

#### Sync Project

If you modified a project `BUILD.bazel` file, or the project view file, then
click the "Sync Project with BUILD Files" button in the Bazel toolbar, or the
`Sync > Sync Project with BUILD Files` entry in the Bazel menu to synchronize
IntelliJ with those changes.

#### Modify Project View

Click `Project > Open Local Project View File` in the Bazel menu to open and
modify the current project view file. You may need to sync the project for your
changes to take effect.

#### Build the Project

Click `Build > Compile Project` in the Bazel menu to build the whole project.
Click `Build > Compile "CURRENT FILE"` to compile only the current file.

#### Run or Debug an Executable or Test

Click on the drop-down menu in the Bazel tool bar and select the entry `Bazel
run <your target>` or `Bazel test <your target>`. If the executable or test you
wish to run or debug is not in the list then follow instructions on adding a
run configuration below first. Come back here when ready.

The selected entry is a run configuration. Click the green arrow in the Bazel
toolbar to run the executable or test. Click the green bug icon to debug the
executable or test.

#### Adding a Run Configuration

If you wish to add an executable or test to the run configurations, then the
simplest and most consistent way is to add the target to the project view file
and sync the project.

Otherwise, click on the drop-down menu in the Bazel tool bar and select "Edit
Configurations...". Click the "+" in the upper left corner and select "Bazel
Command" from the list. This will add a new entry to the "Bazel Command"
sub-tree. Fill in the fields in the pane on the right side to define the run
configuration: Give a suitable name, select the Bazel target, and choose the
Bazel command (`run`, `test`, etc.). If applicable, you can also define Bazel
command-line flags or command-line flags to the executable. Click on "Apply",
or "OK" to add the run configuration.

#### Attaching sources to scala library

If you do not have the Scala library sources linked (you only see the decompiled
 sources), you can attach it manually by selecting the `Choose sources...`
 button on the yellow bar at the top, and selecting `scala-library...-src.jar`.

### Known Issues

#### Missing folders in project tree

The "Project" pane contains a tree-view of the folders in the project. The
"Compact Middle Packages" feature can cause intermediate folders to disappear,
only showing their children in the tree-view. The workaround is to disable the
feature by clicking on the gear icon in the "Project" pane and unchecking
"Compact Middle Packages". Refer to the [issue tracker][project_tree_issue] for
details.

[project_tree_issue]: https://github.com/bazelbuild/intellij/issues/437

#### Rerun failed tests is broken

IntelliJ allows to rerun only a single failed test-case by the click of a
button. Unfortunately, this feature does not work with the Bazel plugin on
Scala test-cases. Please refer to the [issue tracker][rerun_failed_tests_issue]
for details.

[rerun_failed_tests_issue]: https://github.com/bazelbuild/intellij/issues/446

### Troubleshooting

#### 'tools.dade-exec-nix-tool' not found

If you get the error
```
error: attribute 'dade-exec-nix-tool' in selection path 'tools.dade-exec-nix-tool' not found
```
in the bazel console during project import, make sure that
_Preferences_ → _Bazel Settings_ → _Bazel binary location_
points to `./dev-env/bin/bazel`
rather than to  `./dev-env/lib/dade-exec-nix-tool` (as Intellij might have expanded the former to the latter).
If that doesn't help try starting IntelliJ from the root
of the `daml` repository by calling `idea .`.

## Bazel Command Reference

The following sections briefly list Bazel commands for the most common
use-cases. Refer to the [official Bazel documentation][bazel_docs] for more
detailed information.

[bazel_docs]: https://docs.bazel.build/versions/master/bazel-overview.html

### Building Targets

- Build all targets

    ```
    bazel build //...
    ```

- Build an individual target

    ```
    bazel build //daml-script/runner:daml-script-binary
    ```

### Running Tests

- Execute all tests

    ```
    bazel test //...
    ```

- Execute a test suite

    ```
    bazel test //daml-script/runner:tests
    ```

- Show test output

    ```
    bazel test //daml-script/runner:tests --test_output=streamed
    ```

    Test outputs are also available in log files underneath the convenience
    symlink `bazel-testlogs` or `bazel-out/*/testlogs`.

- Do not cache test results

    ```
    bazel test //daml-script/runner:tests --nocache_test_results
    ```

- Execute a specific Scala test-suite class

    ```
    bazel test //daml-lf/engine:tests_test_suite_src_test_scala_com_digitalasset_daml_lf_engine_ApiCommandPreprocessorSpec.scala
    ```

- Execute a test with a specific name

    ```
    bazel test \
    //daml-lf/engine:tests_test_suite_src_test_scala_com_digitalasset_daml_lf_engine_ApiCommandPreprocessorSpec.scala \
      --test_arg=-t \
      --test_arg="JdbcLedgerDao (divulgence) should preserve divulged contracts"
    ```

- Execute tests that include a string in their name

    ```
    bazel test \
    //daml-lf/engine:tests_test_suite_src_test_scala_com_digitalasset_daml_lf_engine_ApiCommandPreprocessorSpec.scala \
      --test_arg=-z \
      --test_arg="preserve divulged"
    ```

    More broadly, for Scala tests you can pass through any of the args outlined in http://www.scalatest.org/user_guide/using_the_runner, separating into two instances of the --test-arg parameter as shown in the two examples above.

### Running Executables

- Run an executable target

    ```
    bazel run //daml-script/runner:daml-script-binary
    ```

- Pass arguments to an executable target

    ```
    bazel run //daml-script/runner:daml-script-binary -- --help
    ```

### Running a REPL

- Run a Scala REPL which has the library on the classpath:

    ```
    bazel run //ledger/ledger-on-memory:ledger-on-memory_repl
    ```

### Querying Targets

The Bazel query language is described in detail in the [official Bazel
documentation][bazel_query_lang]. This section will list a few common
use-cases. Filters like `filter` or `kind` accept regular expressions. Query
expressions can be combined using set operations like `intersect` or `union`.

[bazel_query_lang]: https://docs.bazel.build/versions/master/query.html

- List all targets underneath a directory

    ```
    bazel query //ledger/...
    ```

- List all library targets underneath a directory

    ```
    bazel query 'kind("library rule", //ledger/...)'
    ```

- List all Scala library targets underneath a directory

    ```
    bazel query 'kind("scala.*library rule", //ledger/...)'
    ```

- List all test-suites underneath a directory

    ```
    bazel query 'kind("test_suite", //ledger/...)'
    ```

- List all test-cases underneath a directory

    ```
    bazel query 'tests(//ledger/...)'
    ```

- List all Java test-cases underneath a directory

    ```
    bazel query 'kind("java", tests(//ledger/...))'
    ```

- List all Scala library dependencies of a target

    ```
    bazel query 'kind("scala.*library rule", deps(//daml-script/runner:daml-script-binary))'
    ```

- Find available 3rd party dependencies

    ```
    bazel query 'attr(visibility, public, filter(".*scalaz.*", //3rdparty/...))'
    ```

#### Dependency Graphs

Bazel queries can also output dependency graphs between the targets that the
query includes. These can then be rendered using Graphviz.

- Graph all Scala library dependencies of a target

    ```
    bazel query --noimplicit_deps 'kind(scala_library, deps(//daml-script/runner:daml-script-binary))' --output graph > graph.in
    dot -Tpng < graph.in > graph.png
    ```

    The `--noimplicit_deps` flag excludes dependencies that are not explicitly
    listed in the `BUILD` file, but that are added by Bazel implicitly, e.g.
    the unused dependency checker added by `rules_scala`.

### Help

- List available commands

    ```
    bazel help
    ```

- Show help on a Bazel command

    ```
    bazel help build
    ```

- Show details on each option

    ```
    bazel help build --long
    ```

## Continuous Build

By continuous build we mean the ability to watch the repository for source file
changes and rebuild or rerun targets when any relevant files change. The
dev-env provides the tool `ibazel` for that purpose. Similar to Bazel it can be
called with the commands `build`, `test`, or `run` on a specific target. It
will perform the command and determine a list of relevant source files. Then,
it will watch these files for changes and rerun the command on file change. For
example:

```
ibazel test //daml-script/runner:tests
```

Note, that this interacts well with Bazel's test result caching (which is
activated by default). In the above example the outcome of tests whose sources
didn't change will already be cached by Bazel and the tests won't be repeated.

Refer to the [project README][bazel_watcher] for more information.

[bazel_watcher]: https://github.com/bazelbuild/bazel-watcher#readme

## Haskell in Bazel

In Bazel terminology, a directory containing a `BUILD.bazel` file is
called a "package". Packages contain targets and `BUILD.bazel` files
are where targets are defined. Mostly we are concerned in our
`BUILD.bazel` files with writing rules to produce specific Haskell
derived files (artifacts) from Haskell source file inputs. Of these,
most are libraries, some are executables and some are tests.

For Haskell, most `BUILD.bazel` files begin with a variation on the
following:
```
load( "//bazel_tools:haskell.bzl",
      "da_haskell_library", "da_haskell_binary","da_haskell_test" )
```
This directive loads from the `//bazel_tools` package, the rules
[`da_haskell_library`](https://api.haskell.build/haskell/haskell.html#haskell_library)
for building libraries,
[`da_haskell_binary`](https://api.haskell.build/haskell/haskell.html#haskell_binary)
for building executables and
[`da_haskell_test`](https://api.haskell.build/haskell/haskell.html#haskell_test)
for building test-suites. The `da_*` rules are DA specific overrides
of the upstream `rules_haskell` rules. Their API docs can be found in
`//bazel_tools/haskell.bzl`, or by executing the `bazel-api-docs` tool
from `dev-env`. They mostly behave like the upstream rules, just
adding some defaults, and adding a `hackage_deps` attribute (more on
this below) for convenience.

### Library : `da_haskell_library`

One specific library in the `daml-foundations` stack is
`daml-ghc-compiler`. Here's a synopsis of its definition.
```
da_haskell_library(
    name = "daml-ghc-compiler",
    srcs = glob([
        "src/**/*.hs",
    ]),
    src_strip_prefix = "src",
    deps = [
        "//compiler/daml-lf-ast",
        "//compiler/daml-lf-proto",
        ...
    ],
    hackage_deps = [
        "base",
        "bytestring",
        ...
    ],
    visibility = ["//visibility:public"],
)
```
To build this single target from the root of the Daml repository, the
command would be:
```
bazel build //compiler/damlc/daml-compiler
```
since the `BUILD.bazel` that defines the target is in the
`compiler/damlc` sub-folder of the root of the DA
repository and the target `name` is `damlc`.

Let's break this definition down:
- `name`:
    A unique name for the target;
- `srcs`:
    A list of Haskell source files;
- `src_stip_prefix`:
    Directory in which the module hierarchy starts;
- `deps`:
    A list of in-house Haskell or C library dependencies to be linked
    into the target;
- `hackage_deps`:
    A list of external Haskell (Hackage) libraries to be linked into
    the target;
- `visibility`:
    Define whether depending on this target by others is permissible.

Note the use of the Bazel
[`glob`](https://docs.bazel.build/versions/master/be/functions.html#glob)
function to define the `srcs` argument allowing us to avoid having to
enumerate all source files explicitly. The `**` part of the shown glob
expression is Bazel syntax for any sub-path. Read more about `glob`
[here](https://docs.bazel.build/versions/master/be/functions.html#glob).

The `deps` argument in the above invocation can be interpreted as
linking the libraries defined by the list of targets provided on the
right hand side (when we say "package" here we mean Haskell package -
i.e. library):
- `//:ghc-lib` is the `ghc-lib` package defined in the
root `BUILD` file,
- `//compiler/daml-lf-ast` is the
`daml-lf-ast` package defined in the
`daml-foundations/daml-compiler/BUILD.bazel` file (that is,
`//compiler/daml-lf-ast` is shorthand
for
`//compiler/daml-lf-ast:daml-lf-ast`)
- Similarly, `//nix/third-party/proto3-suite` is the Haskell library
`//nix/third-party/proto3-suite:proto3-suite` defined in the file
`nix/third-party/proto3-suite/BUILD.bazel`.

The `hackage_deps` argument details those Haskell packages (from
Hackage) that the `daml-ghc-compiler` target depends upon. In this case
that is `base`, `bytestring` and some other packages not
shown. Finally, `visibility` is set to public so no errors will result
should another target attempt to link `daml-ghc-compiler`. *[Note : Public
visibility means that any other target from anywhere can depend on the
target. To keep the dependency graph sane, its a good idea to keep
visibility restrictive. See
[here](https://docs.bazel.build/versions/master/be/common-definitions.html#common.visibility)
for more detail.]*

### Executable : `da_haskell_binary`

Here's the synopsis of the rule for the executable `daml-ghc`:
```
da_haskell_binary (
  name = "daml-ghc",
  srcs = glob (["src/DA/Cli/**/*.hs", "src/DA/Test/**/*.hs"])
  src_strip_prefix = "DA",
  main_function = "DA.Cli.GHC.Run.main",
  hackage_deps = [ "base", "time", ...],
  data = [
    "//compiler/damlc/pkg-db"
    , ...
  ],
  deps = [
      ":daml-ghc-compiler"
    , "//:ghc-lib"
    , "//compiler/daml-lf-ast"
    , ...
  ]
  , visibility = ["//visibility:public"]
)
```
Haskell binaries require a definition of the distinguished function
`main`. The `main_function` argument allows us to express the
qualified module path to the definition of `main` to use.

The `data` argument in essence is a list of files needed by the target
at runtime. Consult [this
documentation](https://docs.bazel.build/versions/master/be/common-definitions.html#common.data)
for more detail. The targets that are given on the right-hand-side
are (long-hand) labels for "file-groups". Here's the one for
`daml-stdlib-src` for example.
```
filegroup(
  name = "daml-stdlib-src",
  srcs = glob(["daml-stdlib/**"]),
  visibility = ["//visibility:public"]
)
```

Having looked at `deps` in the context of `haskell_library` there's
not much more to say except note the `:daml-ghc-compiler` syntax for the
depedency on `daml-ghc-compiler`. That is, targets defined in the same
`BUILD.bazel` as the target being defined can be referred to by
preceding their names with `:`.

### Test : `da_haskell_test`

For an example of a test target, we turn to
`//libs-haskell/da-hs-base:da-hs-base-tests`:
```
da_haskell_test(
    name = "da-hs-base-tests",
    src_strip_prefix = "src-tests",
    srcs = glob(["src-tests/**/*.hs"]),
    deps = [
        ":da-hs-base",
    ],
    visibility = ["//visibility:public"],
)
```
There is nothing new in the above to expound upon here! How might you
invoke that single target? Simple as this:
```
bazel test "//libs-haskell/da-hs-base:da-hs-base-tests"
```
More comprehensive documentation on the `bazel` command can be found
[here](https://docs.bazel.build/versions/master/command-line-reference.html).

### Beyond defining targets

If your work goes beyond simply adding targets to existing
`BUILD.bazel` files and involves things like defining toolchains and
external dependencies, then [this
document](https://github.com/digital-asset/daml/blob/main/BAZEL-haskell.md)
is for you!

## Scala in Bazel

In this section we will provide an overview of how Scala targets are defined in
Bazel in this repository. This should provide enough information for most
everyday development tasks. For more information refer to the Bazel porting
guide for JVM developers (to be written as of now). For a general reference to
`BUILD.bazel` file syntax refer to the [official
documentation][build_file_docs]. Note, that `BUILD.bazel` and `BUILD` are both
valid file names. However, `BUILD.bazel` is the preferred spelling. `BUILD` is
the older spelling and still to be found in large parts of the documentation.

[build_file_docs]: https://docs.bazel.build/versions/master/build-ref.html#BUILD_files

Bazel targets are defined in `BUILD.bazel` files. For example
`//ledger-client/ods:ods` is defined in `ledger-client/ods/BUILD.bazel`. First,
we import the required rules and macros. For example, the following loads the
`da_scala_library` macro defined in `//bazel_tools:scala.bzl`, and the `daml`
rule defined in `ledger-client/daml.bzl`. The distinction of rules and macros
is not important here.

```
load('//bazel_tools:scala.bzl', 'da_scala_library')
load('//rules_daml:daml.bzl', 'daml')
```

### Scala Libraries

The macro `da_scala_library` is a convenience function that defines a Scala
library and sets common compiler flags, plugins, etc. To explain it we will
take an example instance and describe the individual attributes. For details
refer to the [`rules_scala` project README][rules_scala_docs]. For a Java rules
refer to the [official Bazel documentation][java_docs].

[rules_scala_docs]: https://github.com/bazelbuild/rules_scala#scala-rules-for-bazel
[java_docs]: https://docs.bazel.build/versions/master/be/java.html#java-rules

```
da_scala_library(

  # Set the target name to 'ods'.
  name = 'ods',

  # Mark this target as public.
  # I.e. targets in other package can depend on it.
  visibility = ['//visibility:public'],

  # Define the target's source files by globbing.
  # The details of file globbing are explained here:
  # https://docs.bazel.build/versions/master/be/functions.html#glob
  srcs = glob(['src/main/**/*.scala'], exclude = [...]),

  # Define the target's resources by globbing.
  resources = glob(['src/main/resources/**/*']),

  # Define the target's dependencies.
  # These will appear in the compile-time classpath.
  # And the transient closure of `deps`, `runtime_deps`, and `exports`
  # will appear in the runtime classpath.
  deps = [
    # A third party dependency.
    '//3rdparty/jvm/ch/qos/logback:logback_classic',
    # A dependency in the same workspace.
    '//ledger-client/nanobot-framework',
    # A dependency in the same package.
    ':ods-macro'
    ...
  ],

  # Define the target's runtime dependencies.
  # These will appear only in the runtime classpath.
  runtime_deps = [...],

  # List of exported targets.
  # E.g. if something depends on ':ods', it will also depend on ':ods-macro'.
  exports = [':ods-macro'],

  # Scalac compiler plugins to use for this target.
  # Note, that these have to be specified as JAR targets.
  # I.e. you cannot use `//3rdparty/jvm/org/scalameta/paradise_2_12_6` here.
  plugins = [
    '//external:jar/org/scalameta/paradise_2_12_6',
  ],

  # Scalac compiler options to use for this target.
  scalacopts = ['-Xplugin-require:macroparadise'],

  # JVM flags to pass to the Scalac compiler.
  scalac_jvm_flags = ['-Xmx2G'],
)
```

### Scala Executables

Scala executables are defined using `da_scala_binary`. It takes most of the
same attributes that `da_scala_library` takes. Notable additional attributes
are:

- `main_class`:
    Name of the class defining the entry-point `main()`.
- `jvm_flags`:
    Flags to pass to the JVM at runtime.
- `data`:
    Files that are needed at runtime. In order to access such files at runtime
    you should use the utility library in
    `com.daml.testing.BuildSystemSupport`.

### Scala Test Cases

Scala test-suites are defined using `da_scala_test_suite`. It takes most of the
same attributes as `da_scala_binary`.

Note, that this macro will create one target for every single source file
specified in `srcs`. The advantage is that separate test-cases can be addressed
as separate Bazel targets and Bazel's test-result caching can be applied more
beneficially. However, this means that test-suite source files may not depend
on each other.

A single Scala test-cases, potentially consisting of multiple source files, can
be defined using `da_scala_test`. It is preferable to always use
`da_scala_test_suite`, and define separate testing utility libraries using
`da_scala_library` if test-cases depend on utility modules.

### Scala JMH Benchmarks

Scala benchmarks based on the JMH toolkit can be defined using the
`scala_benchmark_jmh` macro provided by `rules_scala`. It supports a restricted
subset of the attributes of `da_scala_binary`, namely: `name`, `deps`, `srcs`,
`scalacopts` and `resources`.

The end result of building the benchmark is a Scala binary of the same name,
which can be executed with `bazel run`.

### Java and Scala Deployment

Bazel's builtin Java rules and `rules_scala` will automatically generate a fat
JAR suitable for deployment for all your Java and Scala targets. For example,
if you defined a Scala executable target called `foo`, then Bazel will generate
the target `foo_deploy.jar` next to the regular `foo.jar` target. Building the
`foo_deploy.jar` target will generate a self-contained fat JAR suitable to be
passed to `java -jar`. When using `da_scala_binary`, we also generate a 
`foo_distribute.jar` which takes the deployment jar and fixes the notices and
licenses. It will check for the `ee-jar-license` tag to decide whether to use
the default apache license, or our Enterprise Edition license.

### Scaladoc

We generate scaladoc targets for each `da_scala_library` with a
`_scaladoc` name suffix. All scaladoc targets have a `scaladoc` tag
which allows you to filter them out to speed up your builds. You can
either do that on the command line by running e.g.

```
bazel build --build_tag_filters=-scaladoc //...
```

or if you want to never build them locally you can set this in your `.bazelrc.local`:

```
build --build_tag_filters=-scaladoc
```

You can still build an individual target by selecting it explicitly, e.g.

```
bazel build //daml-lf/validation:validation_scaladoc
```

### Daml Packages

Daml package targets are defined using the `daml` rule loaded from
`//rules_daml:daml.bzl`. To explain it we will take an example instance and
describe the individual attributes.

```
daml(
  name = "it-daml",
  # The main Daml file. This file will be passed to damlc.
  main_src = "src/it/resources/TestAll.daml",
  # Other Daml files that may be imported by the main Daml file.
  srcs = glob(["src/it/resources/**/*.daml"]),
  # The directory prefix under which to create the DAR tree.
  target_dir = "target/scala-2.12/resource_managed/it/dars",
  # The group ID.
  group = "com.daml.sample",
  # The artifact ID.
  artifact = "test-all",
  # The package version.
  version = "0.1",
  # The package name.
  package = "com.daml.sample",
)
```

This will compile and package the Daml code into a DAR file under the following
target, where `<group-dir>` is the `group` attribute with `.` replaced by `/`.

```
:<target_dir>/<group-dir>/<artifact>/<version>/<artifact>-<version>.dar,
```

For example:

```
:target/scala-2.12/resource_managed/it/dars/com/digitalasset/sample/test-all/0.1/test-all-0.1.dar
```

POM and SHA files will be stored in the same directory.

Additionally, this will perform Scala code generation and bundle the generated
Scala modules into a source JAR available under the following target.

```
<name>.srcjar
```

For example:

```
it-daml.srcjar
```

### Daml Executables

The rule `daml_binary` is provided to generate executable targets that execute
the Daml sandbox on a given DAR package. For example:

```
daml_binary(
  name = "ping-pong-exec",
  dar = ':target/repository/.../PingPong-0.1.dar',
)
```

Such a target can then be executed as follows, where arguments after `--` are
passed to the Daml sandbox.

```
bazel run //ledger-client/nanobot-sample-app:ping-pong-exec -- --help
```

### External Java and Scala Dependencies

External dependencies are these that are not defined and built within the local
workspace, but are defined in an external workspace in some way. The most
common case are Maven JAR dependencies which are fetched from Artifactory.

We distinguish direct and transitive dependencies. Direct dependencies are
explicitly defined on targets in the local workspace. Most commonly on the
`deps`, `runtime_deps`, `exports`, or `plugins` attributes. Transitive
dependencies are introduced implicitly through direct dependencies, most
commonly on another dependency's `exports` attribute.

All direct Scala and Java dependencies are listed explicitly in the file
`bazel-java-deps.bzl`. Each dependency is defined by its Maven coordinates. The
`maven_install` repository rule calls Coursier to perform transitive dependency
resolution and import the required artifacts into the Bazel build.

The resolved versions are pinned in the file `maven_install_2.13.json`. Execute
`bazel run @unpinned_maven//:pin` when you wish to update or add a new
dependency.
See [`rules_jvm_external`][rules_jvm_external] for details.

[rules_jvm_external]: https://github.com/bazelbuild/rules_jvm_external#updating-maven_installjson

## Typescript in Bazel

We are using [rules_typescript](https://github.com/bazelbuild/rules_typescript) to build
typescript projects. It works in conjunction with rules_nodejs to provide access to
npm packages.

Please refer to the documentation in the above url for usage.
For an example, please see `compiler/daml-extension/BUILD.bazel`.

## Protocol buffers in Bazel

We use protocol buffers for Daml-LF and the Ledger API. The Daml-LF protocol
buffer build rules can be found from //daml-lf/archive/BUILD.bazel.
It produces bindings for Java and Haskell (via proto3-suite).

Bazel provides built-in rules for protocol buffer bindings for Java and C++.
See the following resources for more information on its usage:
[Protocol Buffer Rules](https://docs.bazel.build/versions/master/be/protocol-buffer.html)
[Blog post: Protocol Buffers in Bazel](https://blog.bazel.build/2017/02/27/protocol-buffers.html)

The rules for haskell are currently ad-hoc genrules and use the proto3-suite's compile-proto-file
program directly. Please refer to //daml-lf/archive/BUILD.bazel for example usage.
If you find yourself writing similar rules, please take a moment to write some Starlark to abstract
it out and document it here. Note that proto3-suite isn't compatible with protoc, so it is not currently
possible to hook it up into the "proto_library" tooling.

## Known issues

### Unchanged Haskell library being rebuilt

Unfortunately, [GHC builds are not deterministic](https://gitlab.haskell.org/ghc/ghc/issues/12262). This, coupled with the way Bazel works, may lead to Haskell libraries that have not been changed to be rebuilt. If the library sits at the base of the dependency graph, it may cause a ripple effect that forces you to rebuild most of the workspace without an actual need for it (`ghc-lib` is one example of this).

**To work around this issue** you can clean the local and build cache, making sure you are fetching the GHC build artifacts from remote:

    bazel clean --expunge # clean the build cache
    rm -r .bazel-cache    # clean the local cache

This will also mean that changes made locally will need to be rebuilt, but it's likely that this will still result in a net positive gain on your build time.

If you are still rebuilding after this, you probably also have a
poisoned Nix cache. To clear that run through the following steps:

    bazel clean --expunge # clean the build cache
    rm -r .bazel-cache    # clean the local cache
    rm dev-env/var/gc-roots/* # Remove dev-env GC roots
    rm result* # Remove GC roots you might have from previous nix-build invocations.
    nix-store --gc --print-roots # View all garbage collection roots
    # Verify that there is nothing from our repo or some Bazel cache.
    # If you are not sure ask in #team-daml
    nix-store --gc # Run garbage collection
    nix-build nix -A tools -A cached --no-out-link # Build the nix derivations (they should be fetched from the cache)
    bazel build //... # You should now see things being fetched from the cache


NOTE: If you have re-installed nix, you might have removed or lost settings in `/etc/nix/nix.conf`.
You might run into this warning:

    warning: ignoring untrusted substituter 'https://nix-cache.da-ext.net'

Please follow the instructions for installing nix in your development environment [here](README.md#mac), notably when using macOS:

1. Add yourself as a nix trusted user by running `echo "extra-trusted-users = $USER" | sudo tee -a /etc/nix/nix.conf`
2. Restart the `nix-daemon` by running `sudo launchctl stop org.nixos.nix-daemon && sudo launchctl start org.nixos.nix-daemon`

### Working in environments with low or intermittent connectivity

Bazel tries to leverage the remote cache to speed up the build process but this can turn out to work against you if you are working in an environment with low or intermittent connectivity. To disable fetching from the remote cache in such scenario, you can use the `--noremote_accept_cached` option.
