# Multi-Package CLI 

The new multi-package build feature improves the workflow for developers working across multiple packages at the same time.

Using multi-package builds, developers can:

- Configure `daml build` to automatically rebuild DARs used in data-dependencies when the source code corresponding to those DARs changes. This is useful in projects with multiple packages.
- Build all of the packages in a project simultaneously using the new `daml build --all` command.
- Clean all build artifacts in a project using `daml clean --all`.

### Quick Usage Overview

The developer begins by configuring which packages are to be built automatically by placing a new configuration file `multi-package.yaml` at the root of their project:

`multi-package.yaml`
```yaml
packages:
- <path to first package>
- <path to second package>
```

The developer then runs `daml build --all` from the root of their project to build all of the packages listed in the `multi-package.yaml` file, or they run `daml build` within a package to build it and its dependencies.

The rest of this document expands on use cases and usage of the CLI and the `multi-package.yaml` file.

## Problem Statement

Frequently when developing Daml, we have the notion of our "project", which contains all of the code related our Daml application. Within this project, we may have multiple Daml packages, each with its own `daml.yaml`.

For example, say a developer is developing an application with some data model, and some logic over that model in a separate package. To make changes, the developer simultaneously works on two packages Logic and Model, where Logic depends on the build artifact of Model, `Model.dar`.

```
> tree
.
├── package-Logic
│   ├── daml/...
│   └── daml.yaml
└── package-Model
    ├── .daml/dist/package-Model-1.0.0.dar
    ├── daml/...
    └── daml.yaml
```

Without multi-package, if the developer makes changes to the source code of both Logic and Model, the developer must invoke `daml build` from within `./package-Model` to produce `package-Model-1.0.0.dar` before invoking `daml build` within `./package-Logic`. If the developer forgets to compile package Model first, package Logic will build against a stale version `package-Model-1.0.0.dar` that does not reflect the latest changes to Model's source code.

This has a number of drawbacks:
- The developer must navigate directories and invoke `daml build` twice.
- The developer must always remember to build in order to propagate changes to package-Logic-1.0.0.dar.
- If the developer forgets, compilation may succeed without warnings due to the stale package-Model-1.0.0.dar, where it would fail if package-Model-1.0.0.dar were up to date.

#### Correct Usage Without Multi-Package

```bash=
> # ... make some changes to Model ...
> cd ./package-Model/  # Navigate to package-Model
> daml build           # Build Model.dar
> cd ../package-Logic/ # Navigate to package-Logic
> daml build           # Build Logic.dar
```

## Using Multi-Package as a Solution

Using multi-package builds, the developer can register multiple packages as interdependent parts of our "project". Whenever a package A in our project refers to the DAR of another package B in our project, any `daml build` on package A will automatically rebuild B's DAR from source if it has changed via a second invocation of `daml build` (See [Caching](#Caching) for details on what "changed" means here).

The directories containing the `daml.yaml` for the various packages that should be part of our project are listed (preferably with relative paths) under the `packages` header in the `multi-package.yaml`.

### Example

For our example project, we would register `package-Model` and `package-Logic` as members of our project. We do this by placing a `multi-package.yaml` file at the root of our project, with two (preferably with relative) references to packages Model and Logic.

`multi-package.yaml`
```yaml
packages:
- ./package-Logic
- ./package-Model
```

Our new tree will look something like:

```
> tree
.
├── multi-package.yaml
├── package-Logic
│   ├── daml/...
│   └── daml.yaml
└── package-Model
    ├── .daml/dist/package-Model-1.0.0.dar
    ├── daml/...
    └── daml.yaml
```

Now that both `package-Logic` and `package-Model` are in the project, any attempt to build `package-Logic` will detect that the `Model.dar` data-dependency belongs to  `package-Model`, and automatically re-builds it.

**Correct Usage With Multi-Package:**

```bash=
> # ... make some changes to Model ...
> cd ./package-Logic/ # Navigate to package-Logic
> daml build # Build package Logic
...
Dependency "package-Model" is stale, rebuilding...
...
Building "package-Logic"...
Done.
```

Using multi-package build, the developer only runs one call to `daml build`, does not need to remember to build, and can guarantee that changes to their dependency are always propagated to dependent packages.

### Building All Packages

To build all packages in the project, provide the `--all` flag. This will build every package listed in the specified `multi-package.yaml`. Note that when using this flag, `daml build` does not need to be invoked from a package directory.

### Finding the multi-package.yaml

Both `daml build` and `daml clean` will need to know where to find your `multi-package.yaml`, as it is common to invoke them from further into your project structure than the root.

Put briefly, the discovery logic closely mimics that of the `daml.yaml`, and resolves as follows:

1. If a path is passed in using `--multi-package-path PATH`, we use the `multi-package.yaml` available at that path. (This is the `multi-package` equivalent to `--project-root`)
2. Otherwise, the CLI (either `daml build` or `daml clean`) will search up the directory tree starting from the directory where it was invoked. We immediately return the first `multi-package.yaml` we find.
3. If no file is found, multi-build is not used.

More on managing multiple `multi-package.yaml` files in [Project Discovery in Nested Projects](#Project-Discovery-in-Nested-Projects).

### Adding a New Package to your Project

We continue the example above to demonstrate the process of adding a new package to a multi-package project. The current project structure is as follows:

```
> tree
.
├── multi-package.yaml
├── package-Logic
│   ├── .daml/dist/package-Logic-1.0.0.dar
│   ├── daml/...
│   └── daml.yaml
└── package-Model
    ├── daml/...
    ├── daml/.dist/package-Model-1.0.0.dar
    └── daml.yaml
```

We add a new package to our tree by running `daml new --template=empty-skeleton package-Tests`. This creates an empty package named `package-Tests` alongside our existing `package-Logic` and `package-Model` packages.

```
> tree
.
├── multi-package.yaml
├── package-Logic
├── package-Model
└── package-Tests
```

In the newly created `package-Tests/daml.yaml`, we remove the `sandbox-options` and add a dependency on the DAR for `Logic`.

```bash
data-dependencies:
- ../package-Logic/.daml/dist/package-Logic-1.0.0.dar
```

Finally, we'll want to tie the new package into our project to take advantage of the multi-package feature - we do this by adding `package-Testing` to the `multi-package.yaml` in the same way Logic and Model have been added.

`multi-package.yaml`
```diff
  packages:
  - ./package-Logic
  - ./package-Model
+ - ./package-Tests
```

#### Running Multi-Package Tests

Assuming we then write our tests, we can build our test package in one of two ways:

- Run `daml build` from the `package-Testing` directory to build the Tests package and its dependency Logic package.
- Run `daml build --all` from the root of the project to build all three packages in our project, including the Tests package.

Once we have built the package, we can run `daml test` from the `package-Testing` directory to run the up-to-date tests.

### Behaviour for Packages Not Listed In multi-package.yaml

Packages that are not listed in the `multi-package.yaml` file will not be automatically recompiled, regardless of whether their source is available in the project's source tree.

For example, say we add a vendor library to our source tree. We build the vendor library from source in isolation, and refer to its `vendor-library.dar` as a data-dependency from within `package-Logic/daml.yaml`.

```
> tree
.
├── multi-package.yaml
├── package-Logic
│   ├── daml/...
│   └── daml.yaml
├── package-Model
│   ├── daml/...
│   ├── daml/.dist/package-Model-1.0.0.dar
│   └── daml.yaml
└── vendor-library
    ├── daml/...
    ├── daml/.dist/vendor-library-1.0.0.dar
    └── daml.yaml
```

If we do not update `multi-package.yaml`, builds of package Logic will **not** try to rebuild `vendor-library.dar`, even though its source and `vendor-library/daml.yaml` are clearly available.

```bash=
> # ... make some changes to vendor-library ...
> cd ./package-Logic/ # Navigate to package-Logic
> daml build # Build package Logic
...
Building "package-Logic"... # We DO NOT rebuild vendor-library
Done.
```

This allows users to distinguish between packages that are part of a given project and are not, without risking recompilation of a vendored package whose upstream they don't control.

## Working with Multiple Projects

In some cases, we have nested projects. For example, say we have two separate GitHub repositories, `application` and `library`, in which we work and change the source regularly. The application repository depends on the library-repository by pointing at DAR files within the library repository.

```
.
├── application-repository
│   ├── .git
│   ├── application-logic
│   │   └── daml.yaml
│   ├── application-tests
│   │   └── daml.yaml
│   └── multi-package.yaml
└── library-repository
    ├── .git
    ├── library-logic
    │   └── daml.yaml
    ├── library-tests
    │   └── daml.yaml
    └── multi-package.yaml
```

`application-repository/application-logic/daml.yaml`
```yaml
version: 1.0.0
...
data-dependencies:
- ../../library-respository/library-logic/.daml/dist/library-logic-1.0.0.daml
```

Both repositories have their own `multi-package.yaml` which points to their respective logic and tests packages, so that we can work effectively in each repository on its own.

```bash=
> cd library-repository
> daml build --all
...
Building "library-logic"...
Building "library-tests"...
Done.
> cd application-repository
> daml build --all
...
Building "application-logic"...
Building "application-tests"...
Done.
```

However, occasionally we may want to make changes to both repositories simultaneously. In the example case above, neither repository is aware of the other, so builds run from within the application repository will *not* rebuild dependencies within the library repository.

```bash=
> cd library-repository
> editor library-logic/... # make some changes
> cd ../application-repository
> daml build --all
Nothing to do. # changes from library-logic are NOT picked up and NOT rebuilt
```

In this case, we'd like invocations of build from inside the application repository to be aware of changes to packages inside the library respository, and rebuild those packages if necessary.

For this case, we provide the ability to include other `multi-package.yaml` files into yours using the `projects` field.

`application-repository/multi-package.yaml`
```yaml
packages:
- ./application-logic
- ./application-tests
# Add the path to the library-repository folder, which contains a multi-package.yaml file we'd like to include
projects:
- ../library-repository
```

Once a local `multi-package.yaml` file's `projects` field is populated with a folder containing an external `multi-package.yaml`, all dependencies mentioned in the external YAML will also be included in the local multi-package.

```bash=
> cd library-repository
> editor library-logic/... # make some changes
> cd ../application-repository
> daml build --all
Building "library-logic"... # changes from library-logic ARE picked up and rebuilt
Building "application-logic" # the application-logic package is rebuilt because its library-logic dependency has changed.
```

### Project Discovery with Multiple Files

Because of the `projects:` field, a project might be composed of many `multi-package.yaml` files. Depending on which `multi-package.yaml` file your call to build refers to, a different set of packages is added to the project.

### Project Discovery in Nested Projects

This following example explores a nested project structure, where we have a top-level `main` package with a `multi-package.yaml` file, and a `libs` subdirectory. The `libs` subdirectory has its own `libs/multi-package.yaml` file and contains two packages, `libs/my-lib` and `libs/my-lib-helper`.

The `main` package depends on `my-lib`, which itself depends on `my-lib-helper`.

```
> tree
.
├── libs
│   ├── multi-package.yaml
│   ├── my-lib
│   │   ├── daml
│   │   │   └── MyLib.daml
│   │   └── daml.yaml
│   └── my-lib-helper
│       ├── daml
│       │   └── MyLibHelper.daml
│       └── daml.yaml
├── multi-package.yaml
└── main
    ├── daml
    │   └── Main.daml
    └── daml.yaml
```

Looking inside the most relevant files:

- `libs/multi-package.yaml`
  ```yaml
  packages:
  - ./my-lib
  - ./my-lib-helper
  ```
- `multi-package.yaml`
  ```yaml
  packages:
  - ./main
  projects:
  - ./libs
  ```
- `main/daml.yaml`
  ```yaml
  version: 1.0.0
  ...
  data-dependencies:
  - ../libs/my-lib/.daml/dist/my-lib-1.0.0.dar # main depends on my-lib
  ```

As before, we can run `daml build --all` from the root of the project to build all libraries and `main`.

```bash=
> # From the root of the project:
> daml build --all
Building "my-lib-helper"...
Building "my-lib"...
Building "main"...
```

However, if we run `daml build --all` from the `libs` directory, it'll traverse the directory tree and first find the `libs/multi-package.yaml`. Because `libs/multi-package.yaml` only refers to `my-lib` and `my-lib-helper`, only those will be built.

```bash=
> cd libs/
> daml build --all
Building "my-lib-helper"...
Building "my-lib"...
# Main is *not* built, because libs/multi-package.yaml was used
```

You can explicitly specify the use of the outer `multi-package.yaml` from within `libs` using `--multi-package-path ../multi-package.yaml`.

```bash=
> cd libs/
> daml build --all --multi-package-path ../multi-package.yaml
Building "my-lib-helper"...
Building "my-lib"...
Building "main"... # Main *is* built, because the root multi-package.yaml was used
```

## Migrating from Previous Workflows

Because multi-package builds are a purely opt-in additive behaviour over existing `daml build`, if you don't plan to use multi-package builds you do not need to change anything about your package configurations.

If you do plan to use multi-package files, your `daml.yaml` files will not need updating in general because the `multi-package.yaml` needs only a path to a folder with a `daml.yaml`.

There is one caveat: `multi-package.yaml` assumes that all dependent packages refer to DAR files by the locations where `daml build` would normally put them. This means that if you have shell scripts that move a package's DAR file after build, you will need use the `--output` flag instead, so that `multi-package.yaml` can statically find the DAR's expected output location.

### The `--output` flag

The `--output` flag can be used to specify the location of the DAR generated by `daml build`. This is not new, however it is important to note that the multi package feature extends the use-cases of this flag.

1. Multi package build behaviour correctly handles the use of the `--output` flag, but only if it is specified in the relevant package's `daml.yaml` `build-options`, for example:

   ```
   build-options:
   - --output=./my-dar.dar
   ```

2. The `--output` flag can be used to abstract away the package structure of sub-projects. This is useful when the maintainers of the sub-project are a different team, allowing them to make structural changes to  without breaking dependent projects.

   For example, take the scenario in [Nested Projects](#Project-Discovery-in-Nested-Projects) with the following structure.

   ```
   > tree
   .
   ├── libs
   │   ├── multi-package.yaml
   │   ├── my-lib
   │   │   ├── daml
   │   │   │   └── MyLib.daml
   │   │   └── daml.yaml
   │   └── my-lib-helper
   │       ├── daml
   │       │   └── MyLibHelper.daml
   │       └── daml.yaml
   ├── multi-package.yaml
   └── main
       ├── daml
       │   └── Main.daml
       └── daml.yaml
   ```
  
   If the `my-lib` package had specified `--output ../dars` in its `daml.yaml` file, that would put the dar in a `libs/dars` directory at the root of the `libs` sub-project. Now, the `main` package could use the DAR as a data dependency without needing to know the source location of the `my-lib` package within the `libs` sub-folder.

   ```
   > tree
   .
   ├── libs
   │   ├── multi-package.yaml
   │   ├── dars
   │   │   └── my-lib.dar <--
   │   ├── my-lib
   │   │   └── ...
   │   └── my-lib-helper
   │       └── ...
   ├── multi-package.yaml
   └── main
       └── ...
   ```

## Caching

Depending on the use case, the multi-package feature may automatically rebuild dependencies or even the whole project.

Because of this, `daml build` will try to cache package results and avoid rebuilds wherever possible. `daml build` checks 2 main properties of generated artifacts to ensure they are up-to-date:

- Compare the contents of all Daml source files against those compiled into the DAR.
- Compare the package IDs of all dependencies against the the package IDs of dependencies compiled into the DAR.

### Disabling Caching
- `--no-cache` disables the caching behaviour for a build, forcing the rebuild of every package needed

### Cleaning the Cache with `daml clean`

To clean out any build artifacts in the project, use `daml clean`.

- `daml clean` will clear any artifacts for the current package and its dependencies.
- `daml-clean --all` will clear any artifacts for the entire project.

When using `daml clean` with multi-package enabled and a `multi-package.yaml` provided, you can now also provide a `--all` flag, which will clean the build artifacts of all packages in your project.
