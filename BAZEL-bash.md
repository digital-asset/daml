# Working with Bash in Bazel

Bazel has two out-of-the-box rules, `sh_test` and `genrule`, that can be used
for one-shot custom build targets. If there is some reusable logic behind your
target, consider building a new rule in Skylark instead.

## Platform

There are a number of limitations in the way Bazel will set up the environment
for these Bash scripts when running them, and they differ based on the OS. The
instructions given here will hopefully help you get to a point where your rules
run on all platforms.

## Dependencies

Bazel promises hermeticity, but Bash scripts have access to whatever they want.
As far as I can tell they are not run in any kind of chroot or container (at
least on macOS), although they are run with a restricted PATH (on Linux) and in
a specially set-up folder.

As such, in order to get all of your dependencies available, you need to
specify them in the relevant attributes:
- `srcs` for (source) files required during build; note that in the case of
  `sh_test`, this must be a single-element list that contains just the script
  you intend to run.
- `data` for files required at run time (`sh_test` only).
- `deps` for results of other Bazel rules required at run time.
- `tools` for executables (`genrule` only).

## `genrule`: substitutions

The code of a `genrule` directive is the `cmd` attribute, which is a multiline
string embedding a Bash script. Within that script, you need to use `$$` to
produce a single `$` in the Bash script itself, as `$()` will be expanded by
Bazel's own string interpolation. You can use the `location` and `locations`
functions to get paths for Bazel build results. You should note that the paths
these return is relative to the starting `$PWD` of the script, so if your Bash
script `cd`s somwhere, you have to take that into account.

You can include local files (files present in your workdir that are not the
result of another Bazel rule) as part of your `srcs` by using the `glob`
function, as in:
```
    srcs = glob([
        "package.json",
        "syntaxes/*",
    ]) + [
        "//some:rule",
    ],
```

The script will start running at the root of your workspace. While local files
specified in `srcs` are specified using paths relative to the package root in
the `glob` directive (i.e. relative to the containing directory of the
`BUILD.bazel` file); they are added to the run time directory of the script
using their package-qualified path. If the above snippet came from
`my/package/BUILD.bazel`, the `package.json` file would be at
`my/package/package.json`.

Note that tools and data dependencies will also be given as relative (to the
script's starting directory) paths through the `location` and `locations`
functions.

## Conditionals

Bazel supports simple conditionals in its rules, though I have not found a way
to use them in string substitutions. As some rules have different names under
Windows, this creates a bit of a problem. Aliases provide a way around that;
for example:
```
alias(
    name = "yarn",
    actual = "@nodejs//:bin/yarn.cmd" if is_windows else "@nodejs//:bin/yarn",
)
```
would let you use, in the `cmd` entry of a script:
```
        $$DIR/$(location :yarn)
```
to invoke the correct `yarn` executable regardless of platform.

## Links and speed

Bazel relies a lot on symbolic links to provide what should appear like a new,
isolated environment to each build rule without having to pay the cost of
copying files over each time. However, this means that
1. Everything path-related is different on Windows, because symlinks there are
   not well-supported, and
1. While Bazel can claim some nice speed for creating the environment, every
   read your rule does from a Bazel-provided file takes the hit of going
   through the symlink indirection.
That second point can be very costly when you have to read many small files
(e.g. node dependencies).

## `sh_test`: "runfiles"

Whereas `genrule` takes an inline Bash script as its `cmd` attribute, the
`sh_test` script will run a Bash script defined in a separate file and
specified as the single entry in the `srcs` attribute. This adds the
complication that `location` substitution is not available.

A partial workaround for that is to pass the relevant paths to the script as
the `args` attribute of the `sh_rule`; those will be available as traditional
arguments in Bash (`$@`, `$#`, `$1`, etc.).

However, in order for the test script to work under Windows (where symlinks are
not well supported), another level of indirection is introduced by Bazel: the
paths returned by the `location` function cannot be used directly by the script
given to `sh_test`, and must further be translated to "real", absolute paths
through the use of an `rlocation` Bash function.

To make this function available to your Bash script, the `sh_test` rule must
include an additional dependency:
```
deps = ["@bazel_tools//tools/bash/runfiles"],
```

Then, anything your script needs access to, including executables, needs to be
passed in as arguments (using `location` in the `BUILD.bazel` file). In the
script itself, before using any of the arguments, you have to paste [the
following snippet from the Bazel runfiles]():
```
    # Copy-pasted from the Bazel Bash runfiles library v2.
    set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
    source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
      source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
      source "$0.runfiles/$f" 2>/dev/null || \
      source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
      source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
      { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
    # --- end runfiles.bash initialization v2 ---
```
After that, in your Bash script, you can construct the absolute paths by taking
the values Bazel gave you and expanding them like (assuming the first argument
specified in the `BUILD.bazel` file was a `$(location ...)` call) :
```
MY_FIRST_ARG="$(rlocation "$TEST_WORKSPACE/$1")"
```
The `TEST_WORKSPACE` variable is set by Bazel to the current WORKSPACE name.
Note that you should use this name even for dependencies from external
namespaces.

Note that you should use the above method to access _all_ of your dependencies,
_including_ "local" files from the same package as the `sh_test`. Trying to
access those files directly by their expected path, as you can do in
`genrule`s, will not work with `sh_test` on Windows. You may have to wrap your
local files in a `filegroup` to have a valid target for the `$(location ...)`
call you should pass as an `args` to the `sh_test`.
