# Bazel JVM Porting Guide

This document explains how to port an existing JVM project (Java or Scala)
to Bazel.

If you are not intending to port a project to Bazel but rather use Bazel on an
already ported project, then please refer to the [Bazel User
Guide][bazel_user_guide].

This guide assumes familiarity with Bazel's core concepts and a functioning
Bazel setup. If you are not yet familiar with Bazel, need a refresher, or need
to setup Bazel, please refer to the [Bazel User Guide][bazel_user_guide].

[bazel_user_guide]: ./BAZEL.md

This guide is based on the experience of porting an SBT project to Bazel.  If
you are not porting an SBT project but a Maven project instead, then you should
also have a look at the Maven porting guide in the [official Bazel
documentation][bazel_maven_guide]. However, in order to arrive at a homogenous
build system and project structure you should follow the suggestions in this
guide where possible.

[bazel_maven_guide]: https://docs.bazel.build/versions/master/migrate-maven.html

The structure of this guide is as follows: First, we describe how to define
external dependencies to your project in Bazel. This includes additional Bazel
rules that you may need to import from external workspaces, Maven JAR
dependencies, or other artifacts that need to be fetched from Artifactory.
Then, we describe how to build a JVM project with Bazel. This covers the
project structure, in particular when coming from SBT, and the details of
defining Java and Scala targets in Bazel. In the very end we talk about the
Java runtime and toolchain that Bazel uses and provide links to Bazel API
documentation.


## External Dependencies

External dependencies are targets that your project depends on, but that are
not built, or otherwise generated, within the local workspace. Such external
dependencies are defined in the `WORKSPACE` file or in Bazel macros that are
called from the `WORKSPACE` file.

### Bazel Dependencies

Bazel can be extended with custom rules, for example to support languages for
which Bazel provides no builtin rules. Extensions that are defined outside the
current workspace can be included from an external workspace. Bazel provides
the workspace rules `http_archive`, `get_repository`, or `local_repository` in
order to define an external workspace. Refer to the [official
documentation][bazel_workspace_rules] for details. An example is presented in
the Scala section below.

[bazel_workspace_rules]: https://docs.bazel.build/versions/master/be/workspace.html#workspace-rules

#### Java

Bazel has builtin support for Java. No external workspaces need to be imported
to support Java projects at the time of writing.

#### Scala

Bazel does not have builtin Scala support. In this repository we use
[`rules_scala`][rules_scala], which defines Bazel rules to build Scala projects.
First, we need to import the external workspace:

```
http_archive(
  name = 'io_bazel_rules_scala',
  url = 'https://github.com/bazelbuild/rules_scala/...',
  ...
)
```

This will make `rules_scala` available under the label
`@io_bazel_rules_scala//`. Next, we define the exact version of the Scala
compiler and core libraries to use:

```
load('@io_bazel_rules_scala//scala:scala.bzl', 'scala_repositories')
scala_repositories((
    "2.12.12",
    {
        "scala_compiler": "9dfa682ad7c2859cdcf6a31b9734c8f1ee38e7e391aeafaef91967b6ce819b6b",
        "scala_library": "1673ffe8792021f704caddfe92067ed1ec75229907f84380ad68fe621358c925",
        "scala_reflect": "3c502791757c0c8208f00033d8c4d778ed446efa6f49a6f89b59c6f92b347774",
    },
))
load('@io_bazel_rules_scala//scala:toolchains.bzl', 'scala_register_toolchains')
scala_register_toolchains()
```

If you need to update the Scala version, make sure to also update the
corresponding SHA-256 hashes in hexadecimal encoding. If you don't know the
hash, set it to 64 zeroes and Bazel will correct you.

See the [`rules_scala` setup guide][rules_scala_setup] for further details.

[rules_scala]: https://github.com/bazelbuild/rules_scala
[rules_scala_setup]: https://github.com/bazelbuild/rules_scala#getting-started

### Maven JAR Dependencies

Next, we consider the most common case of Maven JARs which are distributed on
Artifactory. We distinguish direct and transitive dependencies. Direct
dependencies are explicitly defined on targets in the local workspace.
Transitive dependencies are introduced implicitly as dependencies of direct
dependencies. It is preferable to only have to define direct dependencies and
let a dependency resolver determine all transitive dependencies.

The [`rules_jvm_external`][rules_jvm_external] rules set can read a list of
direct JAR dependencies and then use [Coursier][coursier] to perform dependency
resolution and generate the whole transitive closure of dependencies.

Direct dependencies are manually defined in the file `bazel-java-deps.bzl` at
the repository root. This file also defines Bazel macros that must be called in
the `WORKSPACE` file in order to import the external JAR dependencies.

```
load("//:bazel-java-deps.bzl", "install_java_deps")
install_java_deps()
load("@maven//:defs.bzl", "pinned_maven_install")
pinned_maven_install()
```

The macro `install_java_deps` defines the direct dependencies and the tools to
load and update pinned versions. The macro `pinned_maven_install` loads the
pinned artifacts into Bazel. Within `bazel-java-deps.bzl` we call
`maven_install` where we define all direct Maven dependencies.

```
maven_install(
    artifacts = [
        "com.google.guava:guava:24.0-jre",
        "org.scalaz:scalaz-core_2.12:7.2.24",
        ...
    ],
    ...
)
```

Dependencies are defined using their Maven coordinates. Note that Scala
packages have the Scala version appended to their artifact id.

In some cases we need to override the automatic dependency resolution performed
by Coursier. E.g. the `org.lang-scala:*` packages need to exactly match the
version of the Scala compiler registered with `rules_scala`. To that end we use
the `override_targets` attribute:

```
override_targets = {
    "org.scala-lang:scala-compiler": "@io_bazel_rules_scala_scala_compiler//:io_bazel_rules_scala_scala_compiler",
    ...
}
```

This maps Maven artifacts to Bazel targets which should be used instead.

Finally, `pinned_maven_install` generates Bazel targets for the imported JARs.
The naming scheme is as follows `@maven//:group_id_artifact_id`, where all
symbols are replaced by `_` characters. E.g. the artifact
`org.scalaz:scalaz-core_2.12:7.2.24` is available as
`@maven//:org_scalaz_scalaz_core_2_12`.

Adding, changing, or removing a Maven dependency requires two steps:
First, you need to modify the `artifacts` attribute to
`maven_install`, second, you need to execute `bazel run
@unpinned_maven//:pin` to update `maven_install.json`.  You should
also run `@unpinned_maven//:pin` if you change other attributes to
`maven_install`.

Refer to the [`rules_jvm_external` documentation][rules_jvm_external] for
further information.

[rules_jvm_external]: https://github.com/bazelbuild/rules_jvm_external#rules_jvm_external
[coursier]: https://github.com/coursier/coursier

#### Conflicting Dependencies

Following the goal of HEAD based development all projects should strive to
share common dependencies in one place, such as `bazel-java-deps.bzl` for JAR
dependencies. If different projects have conflicting dependencies, then these
conflicts should be fixed so that both projects can share the same common
dependencies.

A valid exception to this rule are external tools that are defined in Bazel and
used for build or development. These may have conflicting dependencies with the
projects in this repository and changing their code to rectify these conflicts
may be out of scope or infeasible.

In such cases multiple calls to `maven_install` are supported, see the
[`rules_jvm_external` documentation][rules_jvm_external_multi].

#### Outdated Dependencies

`rules_jvm_external` provides a tool to check for outdated dependencies. To do
so, run `bazel run @maven//:outdated`, which prints to stdout something like the following:

```
Checking for updates of 153 artifacts against the following repositories:
	https://repo1.maven.org/maven2

com.auth0:java-jwt [3.10.3 -> 4.0.0]
com.auth0:jwks-rsa [0.11.0 -> 0.21.2]
com.chuusai:shapeless_2.13 [2.3.3 -> 2.4.0-M1]
...
```

[rules_jvm_external_multi]: https://github.com/bazelbuild/rules_jvm_external#multiple-maven_installjson-files

## Building a Project with Bazel

Recall, that a Bazel workspace consists of packages and targets. A package is a
directory that contains a `BUILD.bazel` file. A `BUILD.bazel` file can define
rule targets. Additionally, regular files underneath a package are targets as
well.

### Project Structure

When structuring a Bazel project you should strive for small targets and small
packages. Bazel's incremental builds will work best when targets are as small
as possible. Additionally, you should make target visibility as tight as
possible in favour of a clean dependency graph.

If you are porting a JVM component in the `da` repository it is likely that you
will be coming from an SBT project structure similar to the following.

```
da
├── WORKSPACE
└── my_component
    ├── build.sbt
    ├── module1
    │   ├── build.sbt
    │   └── src
    │       ├── main
    │       │   └── scala
    │       │       └── com/daml/module
    │       │           ├── Main.scala
    │       │           ⋮
    │       └── test
    │           └── scala
    │               └── com/daml/module
    │                   ├── SomeSpec.scala
    │                   ⋮
    ├── module2
    │   ├── build.sbt
    ⋮   ⋮
    └── project
        ⋮
```

In this case `da/my_component/build.sbt` defines properties of the whole
component and the interdependencies between the modules (subprojects in SBT).
The files `da/my_component/moduleX/build.sbt` define properties of a particular
module, and its external dependencies. Finally, the folder
`da/my_component/project` contains additional files defining further properties
of the component, SBT plugins, etc.

SBT associates tasks with a project, such as `compile` or `test`. It is then
possible to specify dependencies only for certain tasks. E.g. test-only
dependencies. Furthermore, if `module2` depends on `module1`, then `module2`'s
tests can depend on test classes defined in `module1`.

In Bazel things are structured differently. A library, a test-only library, or
a test-suite are all separate Bazel targets. Dependencies are defined at each
individual target. Repetition can be reduced by transitive dependencies or by
giving a name to a list of targets and assigning that list as a dependency

Bazel is not extended by a plugin mechanism like SBT plugins. Instead Bazel is
extended by user defined rules or macros. It is not often the case that one has
to define custom rules or macros. In most cases existing ones can be reused.

#### Fine Grained Bazel Project Structure

As mentioned before one should strive for as small Bazel targets as possible.
Translated to Bazel the above project might take the following form:

```
da
├── WORKSPACE
└── my_component
    ├── BUILD.bazel
    ├── module1
    │   ├── BUILD.bazel
    │   └── src
    │       ├── main
    │       │   └── scala
    │       │       └── com/daml/module
    │       │           ├── BUILD.bazel
    │       │           ├── Main.scala
    │       │           ⋮
    │       └── test
    │           └── scala
    │               └── com/daml/module
    │                   ├── BUILD.bazel
    │                   ├── SomeSpec.scala
    │                   ⋮
    └── module2
        ├── BUILD.bazel
        ⋮
```

Here `da/my_component/moduleX/src/.../BUILD.bazel` would define small library,
test, and executable targets. `da/my_component/moduleX/BUILD.bazel` would
bundle these small targets into larger ones to make module interdependencies
easier to manage. Finally, `da/my_component/BUILD.bazel` would bundle the
module targets to ease management of component interdependencies or component
release.

#### Coarse Grained Intermediate Bazel Project Structure

However, coming from SBT such a fine grained structure may not immediately be
possible without change in the corresponding Java or Scala code. For example
due to cyclic dependencies between larger sets of source files. In this case
the following structure can be used as an intermediate step:

```
da
├── WORKSPACE
└── my_component
    ├── BUILD.bazel
    ├── module1
    │   ├── BUILD.bazel
    │   └── src
    │       ├── main
    │       │   └── scala
    │       │       └── com/daml/module
    │       │           ├── Main.scala
    │       │           ⋮
    │       └── test
    │           └── scala
    │               └── com/daml/module
    │                   ├── SomeSpec.scala
    │                   ⋮
    └── module2
        ├── BUILD.bazel
        ⋮
```

Here, `da/my_component/moduleX/BUILD.bazel` defines targets for what was
previously an SBT subproject. Note, that a single SBT project will already
separate into multiple Bazel targets at this step. Notably, regular library
targets and test-only library targets will be separate. Test-cases should be
made as small targets as possible at this stage already. Bazel test-suite
macros (see below) should be used to reduce boilerplate.

### Java Targets

This section will outline how to define common Java targets. Please refer to
the [Bazel API documentation][bazel-api-documentation] for further details.

#### Libraries

Java libraries can be defined using the builtin `java_library` rule. It will
generate a JAR file containing the compiled Java classes and a source JAR
containing the input Java sources. For example:

```
java_library(
    name = "example",
    # JAR dependencies for compiletime and runtime classpath
    deps = [
        "//some/package:some_target",
        ":some_local_target",
        ...
    ],
    # JAR dependencies for runtime class path only
    runtime_deps = [ ... ],
    # JAR dependencies that should be forwarded to anything that depends on this target.
    exports = [ ... ],
    # Java source files
    srcs = glob(["src/main/java/.../my_component/**/*.java"]),
    # Resource files
    resources = glob(["src/.../resources/**"]),
    # Keep target visibility tight.
    visibility = ["//current_component:__pkg__"],
)
```

#### Tests

Java tests can be defined using the builtin `java_test` rule. It will generate
an executable wrapper around the given test code that executes the test
runner's main method. Test targets can be executed using the `bazel test`
command.

Bazel can cache test results, which can greatly reduce test-suite execution
time. For maximum benefit test targets should be made as small as possible,
ideally a single Java source file.

Use the `java_test_suite` macro to automatically define one test target for
every given source file and bundle them in one test-suite target. If test-cases
depend on utility classes defined in other Java sources, then you should define
a Java library target for these utility classes and let the test targets depend
on this library.

For example:

```
load('//bazel_tools/java_testing:java_test_suite.bzl', 'java_test_suite')

test_utils = glob(["src/test/java/.../utils/**/*.java"])

java_library(
    name = "test-utils",
    srcs = test_utils,
    ...
)

java_test_suite(
    name = "tests",
    srcs = glob(
        ["src/test/java/**/*.java"],
        exclude = test_utils,
    ),
    # Expected runtime and resource requirements.
    size = "small",
    ...
)
```

The `size` attribute is used to determine the default timeout and resource
requirements. Refer to the [official documentation][bazel_test_size] for
details about test size and other common test attributes.

[bazel_test_size]: https://docs.bazel.build/versions/master/be/common-definitions.html#common-attributes-tests

#### Executables

Java executables can be defined using the builtin `java_binary` rule. It will
generate a JAR and executable wrapper script that defines the classpath and
executes the Java runtime. Executable targets can be executed using the `bazel
run` command.

For example:

```
java_binary(
    name = "example",
    # The srcs attribute is optional.
    srcs = [ ... ],
    # The deps attribute is only allowed if srcs are specified.
    deps = [ ... ],
    # Name of the class that contains the entrypoint `main()`.
    # The main class can originate from srcs, deps, or runtime_deps.
    # A missing main class causes runtime failure.
    main_class = ...,
    # A list of flags to pass to the Java runtime.
    jvm_flags = [ ... ],
    # A list of files that should be present in the runtime path at runtime.
    data = [ ... ],
    ...
)
```

### Scala Targets

This section will outline how to define common Scala targets. Please refer to
the [Bazel API documentation][bazel-api-documentation] for further details.

This repository uses `rules_scala` to build Scala code in Bazel, which provides
rules such as `scala_library`, or `scala_test_suite`. Additionally, wrapper
macros are provided in `bazel_tools/scala.bzl`, such as `da_scala_library`, or
`da_scala_test_suite`. These apply common compiler flags, plugins, and linter
configuration. Please prefer the wrapper macros over the underlying
`rules_scala` rules.

If the project you are porting uses a different set of default flags, plugins,
etc., then see if these could be merged with the common defaults. The goal is
to converge on one shared set of common compiler flags.

#### Libraries

Scala libraries can be defined using the `da_scala_library` rule. It will
generate a JAR file containing the compiled Scala classes and a source JAR
containing the input Scala sources. For example:

```
load("//bazel_tools:scala.bzl", "da_scala_library")

da_scala_library(
    name = "example",
    # JAR dependencies for compiletime and runtime classpath
    deps = [
        "//some/package:some_target",
        ":some_local_target",
        ...
    ],
    # JAR dependencies for runtime class path only
    runtime_deps = [ ... ],
    # JAR dependencies that should be forwarded to anything that depends on this target.
    exports = [ ... ],
    # Scala source files
    srcs = glob(["src/main/java/.../my_component/**/*.scala"]),
    # Resource files
    resources = glob(["src/.../resources/**"]),
    # Keep target visibility tight.
    visibility = ["//current_component:__pkg__"],
    # Unused dependencies cause an error at build time.
    unused_dependency_checker_mode = "error",

    # If the library uses Scala macros you need to add the following attributes.
    scalacopts = ['-Xplugin-require:macroparadise'],
    plugins = [
        # Plugins have to be specified as JAR targets.
        '//external:jar/org/scalameta/paradise_2_12_6',
    ],
)
```

Strive to make library targets as small as possible. In a situation where
multiple Scala sources have no interdependencies you can use the
`da_scala_library_suite` macro to automatically generate one library target per
Scala source file, and bundle them in one target. This rule takes the same
attributes as `da_scala_library` with the exception of
`unused_dependency_checker_mode` which will always be disabled.

#### Tests

Scala tests can be defined using the `da_scala_test` rule. It will generate an
executable wrapper around the given test code that executes the test runner's
main method. Test targets can be executed using the `bazel test` command.

Bazel can cache test results, which can greatly reduce test-suite execution
time. For maximum benefit test targets should be made as small as possible,
ideally a single Scala source file.

Use the `da_scala_test_suite` macro to automatically define one test target for
every given source file and bundle them in one test-suite target. If test-cases
depend on utility classes defined in other Scala sources, then you should
define a Scala library target for these utility classes and let the test
targets depend on this library.

For example:

```
load("//bazel_tools:scala.bzl", "da_scala_library", "da_scala_test_suite")

test_utils = glob(["src/test/scala/.../utils/**/*.scala"])

da_scala_library(
    name = "test-utils",
    srcs = test_utils,
    ...
)

da_scala_test_suite(
    name = "tests",
    srcs = glob(
        ["src/test/scala/**/*.scala"],
        exclude = test_utils,
    ),
    # Expected runtime and resource requirements.
    size = "small",
    ...

    # You can adjust the heap size as follows:
    initial_heap_size = "512m",
    max_heap_size = "2g",
)
```

The `size` attribute is used to determine the default timeout and resource
requirements. Refer to the [official documentation][bazel_test_size] for
details about test size and other common test attributes.

A couple of arguments have been added:

  * `initial_heap_size` is translated to `-Xms`, and defaults to `512m`, and
  * `max_heap_size` is translated to `-Xmx`, and defaults to `2g`.

#### Executables

Scala executables can be defined using the `da_scala_binary` rule. It will
generate a JAR and executable wrapper script that defines the classpath and
executes the Java runtime. Executable targets can be executed using the `bazel
run` command.

For example:

```
load("//bazel_tools:scala.bzl", "da_scala_binary")

da_scala_binary(
    name = "example",
    # The srcs attribute is optional.
    srcs = [ ... ],
    # The deps attribute is only allowed if srcs are specified.
    deps = [ ... ],
    # Name of the class that contains the entrypoint `main()`.
    # The main class can originate from srcs, deps, or runtime_deps.
    # A missing main class causes runtime failure.
    main_class = ...,
    # A list of flags to pass to the Java runtime.
    jvm_flags = [ ... ],
    # A list of files that should be present in the runtime path at runtime.
    data = [ ... ],
    ...

    # You can adjust the heap size as follows:
    initial_heap_size = "512m",
    max_heap_size = "2g",
)
```

## SBT Plugins

If you are porting an SBT project to Bazel then you may encounter SBT plugins
that the project's build depends on, or that are important for developer
workflow, or project release. SBT's plugin mechanism is very flexible, and it
is not possible to describe a general procedure on how to port dependence on
any SBT plugin to Bazel.

In the following we list examples of SBT plugins that have been encountered and
ported to Bazel so far. If the SBT plugin you require has already been ported
to Bazel then you should be able to use the ported version in your project as
well. Otherwise, you should check if the plugin is similar to one of the
previously encountered plugins and port it in an analogous way.

### Built-In Bazel Features

It is worth checking whether the functionality provided by an SBT plugin is
already built into Bazel or a particular Bazel rule set. For instance, Bazel
has built-in support for generating dependency graphs, and `rules_scala` has
built-in support for generating deplyment JARs. Refer to the [user
guide][bazel_user_guide] for details.

### Compiler Plugins

Various SBT plugins make use of compiler plugins, or of components that can be
used as compiler plugins. `rules_scala` allows to define compiler plugins on
Scala targets. Therefore, such SBT plugins can be ported to Bazel by activating
the corresponding compiler plugin.

One example is the [wartremover plugin][wartremover_plugin], which provides
linting for Scala code. In the Bazel build the wartremover compiler plugin is
activated on all Scala targets by default. It is configured in the [Scala rule
wrappers][wartremover_config] used in the daml repository.

[wartremover_plugin]: https://github.com/wartremover/wartremover
[wartremover_config]: https://github.com/digital-asset/daml/blob/ea02814b343e4754c70a8718cb14657d6c51915f/bazel_tools/scala.bzl#L71

### Command-Line Tools

If an SBT plugin relies on a tool that can be called as a standalone
command-line application, then it can be ported to Bazel by defining a custom
rule that calls that command-line application.

For example, the [`scalafmt` Scala formatting checker][scalafmt] can be invoked
as a command-line tool. A custom Bazel rule
[`scala_format_test`][scala_format_test] is defined in the daml repository, that
generates a test-case that will run `scalafmt` on the specified Scala source
files.

[scalafmt]: https://github.com/scalameta/scalafmt
[scala_format_test]: https://github.com/DACH-NY/da/blob/e904c8eac1427633ef20b6106906a59f590de5a6/bazel_tools/scalafmt/scalafmt.bzl#L31

### Daml

The SBT build of the `ledger-client` component defines a custom SBT plugin for
handling Daml code. It covers compilation to LF, packaging to DAR, Scala code
generation, and executing the Daml sandbox. This plugin was ported to Bazel as
a set of custom Bazel rules defined in [`rules_daml`][rules_daml]. Refer to the
[user guide][bazel_user_guide] or the [API docs][bazel-api-documentation] for
details.

[rules_daml]: https://github.com/digital-asset/daml/tree/ea02814b343e4754c70a8718cb14657d6c51915f/rules_daml

## Java Runtime and Toolchain

Bazel is itself written in Java. By default Bazel will use its own Java runtime
and toolchain to build and execute JVM targets. At the time of writing the
Bazel executable provided in the dev-env uses the same Oracle JDK version 8 as
the rest of the dev-env.

Bazel supports specifying a different Java runtime and toolchain for JVM
targets via the rules `java_runtime` and `java_toolchain`. Refer to the
[official documentation][bazel_java_rules] for further information. Note, at
the time of writing the documentation does not display these rules, see [issue
6619][bazel_issue_6619]. They can be viewed on the [web
archive][bazel_java_rules_webarchive] instead.

[bazel_java_rules]: https://docs.bazel.build/versions/master/be/java.html#java-rules
[bazel_issue_6619]: https://github.com/bazelbuild/bazel/issues/6619
[bazel_java_rules_webarchive]: https://web.archive.org/web/20181021162843/https://docs.bazel.build/versions/master/be/java.html

## Bazel API Documentation

[bazel-api-documentation]: #bazel-api-documentation

- The Bazel API documentation to Bazel rules defined in this repository can be
    viewed by executing the following command:
    ```
    $ bazel-api-docs
    ```
    And browsing to http://localhost:8000.
    See `bazel-api-docs -h` for further instructions.
- External Bazel rules API documentation is listed in the
    [official Bazel documentation][bazel_encyclopedia].

[bazel_encyclopedia]: https://docs.bazel.build/versions/master/be/overview.html
