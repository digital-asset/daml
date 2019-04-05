# language-support/js

Official DAML Ledger API support for JavaScript and TypeScript on the Node.js platform.

---

**WARNING:** The support is currently shipped as an **EXPERIMENTAL** feature.

---

## Prepare the development environment

As with the rest of the repository, Bazel is used as a build system.

The build rules currently used have Bazel managing the dependencies centrally throughout the repository.

The following step is optional but it enables IDEs and editors to pick up the dependencies between packages within this project; in order to build the modules locally and install them in the `node_modules` directory of each package, run the [install.sh](install.sh) script:

    language-support/js/install.sh

Re-run the script every time you make a change that affects other projects (e.g.: something changes in `daml-grpc` that needs to be picked up by `daml-ledger`).