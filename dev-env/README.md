# Dev-env

This folder contains the development dependencies of this project.

## Development

### Structure

* `dev-env/lib/dade-common` is the script meant to be included into all wrappers
  for dev-env provided tools and other dev-env specific tools (e.g. `dade-info`)
* `dev-env/lib/dade-dump-profile` is a script which outputs - in porcelain
  mode - all environmental variables necessary for dev-env to function in a
  given shell.

### Debugging

Run your comment with `DADE_DEBUG=1` as prefix, e.g.:

    DADE_DEBUG=1 jq

### Environmental variables

Dev-env uses a common set of variables across it, here are they listed to be
documented in future.

#### Public interface:

These variables are either used for dev-env initialization or are exported by
dev-env for external tools to consume.

* `DADE_REPO_ROOT` - root of a working directory where dev-env currently is
  executing; mutable content; input and output.
* `DADE_VAR_DIR` - points to a directory which is meant for storing persistent
  and mutable data; mutable content; if not set, can be derived from
  `DADE_DEVENV_DIR`; input and output.
* `DADE_DEVENV_DIR` - directory named `dev-env` under `$DADE_BASE_ROOT`, assume
  to be immutable (not yet the case); has to be set before `dade-dump-profile`
  is executed; input and output.
* `DADE_DESIRED_NIX_VERSION` - user can provide this variable in their profile
  to silence the warning (in future, disable automatic installation of) the
  supported Nix version.

#### Private:

These variables are set in some of the dev-env tools and scripts and are not
guaranteed to be always present in the dev-env environment (e.g. not guaranteed
to be set by `dade-common`).

* `DADE_BASE_ROOT` - root of the dev-env package; the directory which contains
  `dev-env` and `nix` directories, assume its immutable.
* `DADE_BUILD_RESULT` - after a dev-env provided tool is built this variable is
  exported to point to a nix store where the tool was built.
* `DADE_DEBUG` - sets `-x` in shell environemnt where dev-env scripts are executed.
* `DADE_CURRENT_SCRIPT_DIR` - current directory of the currently running/sourced script,
  corresponds to `$BASH_SOURCE[0]`, assume its immutable.
* `DADE_CURRENT_COMMAND` - the name of the current command executed by `dade`
* `DADE_GC_ROOTS` - points to a directory named `gc-roots` under `DADE_VAR_DIR`
  which contains Nix garbage collection roots and hashes for each dev-env
  provided tool individually; always mutable.
* `DADE_LIB_DIR` - points to a directory `lib` under `DADE_DEVENV_DIR` which
  contains libraries for dev-env usage, assume its immutable.
* `DADE_NIXPKGS` - points to a GC root which is a symlink to a Nixpkgs snapshot
  used by dev-env, used only in `dade-dump-profile`.
