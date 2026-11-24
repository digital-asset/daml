# Quickstart

This page takes you to your first build of the SDK: what you need to bring, what
the build fetches for itself, and where
to stand when you run it. Other pages explain how the build works on the inside.

The build has not always looked like this. It used to rely on Nix and the
repository's own `dev-env` tool, which together provided the compilers,
libraries, and smaller tools the build needed; until you entered that
environment, nothing would build. That arrangement is gone. The build now brings
its own toolchains and asks very little of your machine.

It asks for [Bazelisk](https://github.com/bazelbuild/bazelisk). Bazelisk isn't a
build tool itself; it is a small program written in Go and maintained by the
Bazel team that sits in front of Bazel. When you run it, it looks up which Bazel
version the repository asks for, downloads that release once into its own cache
(`~/.cache/bazelisk` on Linux), and then passes your arguments to it unchanged.
That way everyone builds with the Bazel the repository expects, without anyone
having to install it.

Besides Bazelisk, you only need Git to get the sources, plus the Xcode Command
Line Tools on macOS and a short one-time setup on Windows, both described below.
The build downloads everything else itself the first time it needs it: the LLVM
compilers for C and C++, Java 17, Python 3.12, Node, and GHC 9.0.2 for the
Haskell code. It keeps their archives in `sdk/.bazel-cache/repo`, so they rarely
have to be downloaded twice.

There is one habit to keep: run every command from `sdk/`. That directory is the
root of the Bazel workspace, where `MODULE.bazel` and `.bazelrc` live. If your
personal `.bazelrc.local` uses relative paths, they are resolved from wherever
you
run the command, so running it from another directory can quietly create a
second, separate set of build outputs next to the first.

## Installing Bazelisk

Bazelisk is published as a single executable for each operating system and
processor, so installing it is only a matter of putting the right file on your
`PATH`. Take it from the
[latest Bazelisk release](https://github.com/bazelbuild/bazelisk/releases/latest)
and rename it to `bazelisk`, or to `bazelisk.exe` on Windows, the name the setup
script below calls it by. On Linux and macOS, also make it executable. On macOS,
`brew install bazelisk` does all of this for you.

| OS      | Release file                                                               |
|---------|----------------------------------------------------------------------------|
| Linux   | `bazelisk-linux-amd64`, or `bazelisk-linux-arm64` on ARM                   |
| macOS   | `bazelisk-darwin-arm64` on Apple silicon, `bazelisk-darwin-amd64` on Intel |
| Windows | `bazelisk-windows-amd64.exe`                                               |

On macOS, the build compiles C code against the macOS SDK that comes with the
Xcode Command Line Tools, so install them with `xcode-select --install` if you
do not have them yet.

### Windows

Windows needs a little preparation before the first build because of two of its
own limits, not anything in the SDK.

The first limit is path length. Bazel nests its work deeply: the files of this
build sit roughly 210 characters below Bazel's output base, the directory where
it keeps everything it builds. Windows refuses paths longer than 260 characters
unless long paths are switched on, and even then it refuses to start a program
whose own path is longer than that. So you have to enable long paths, and the
output base has to move somewhere short, such as `C:/b`.

The second limit is the shell. Bazel runs many build steps as small bash
scripts, but Windows doesn't include bash, so Bazel looks for one via the
`BAZEL_SH` variable. That bash must be in place before Bazel starts, so Bazel
cannot download it itself. The script `setup-windows.ps1` does that instead. It
downloads a pinned portable edition of Git for Windows, which includes bash, and
verifies its SHA-256. It unpacks it next to your checkout in a folder ending
with `_windows_bash` and sets `BAZEL_SH` to its `bash.exe` for your user. Then
it stops any running Bazel server, so the next command starts one that sees the
new shell, and finally warns you if the output base is still longer than 40
characters.

PowerShell adds another hurdle. It runs script files only when its execution
policy allows them, and on desktop editions of Windows, the policy forbids them
by default. Hence, a plain `powershell -File setup-windows.ps1` stops with
"running scripts is disabled on this system". The command below therefore adds
`-ExecutionPolicy Bypass`, which lifts that rule for this one PowerShell process
only and leaves your machine's setting as it was. If you would rather allow
local scripts for good, `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`
does that for your user. Neither helps on a machine whose policy is set by your
company's Group Policy; in that case, whoever manages the machine has to allow
it.

Run these commands once, in this order, from `sdk/` in PowerShell. The order
matters: the output base must already be short when the script checks it. Open a
new terminal afterward so it can see `BAZEL_SH`.

```powershell
# Allow paths longer than 260 characters (needs an admin PowerShell)
Set-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem' -Name LongPathsEnabled -Type DWord -Value 1

# Keep Bazel's output base short, so program paths stay under 260 characters
Add-Content .bazelrc.local 'startup --output_base=C:/b'

# Install the pinned bash that Bazel runs build scripts with, and point BAZEL_SH at it
powershell -ExecutionPolicy Bypass -File setup-windows.ps1
```

## The first build

From `sdk/`, one command builds everything:

```sh
bazelisk build //...
```

`//` is the root of the workspace, and `...` means every package beneath it, so
this asks for every target in the SDK except those tagged `manual`, which are
built only when named. The first run is slow. Bazel downloads the
toolchains, and for most build steps it compiles nothing: it fetches the
result from the shared remote cache at `bazel-cache.da-ext.net`, which CI fills.
Your machine only reads from that cache and never writes to it, so nothing you
build locally can reach anyone else. After the first run, builds are
incremental, and Bazel rebuilds only what your changes touch.

Tests run the same way:

```sh
bazelisk test //...
```

Passing tests stay quiet; for a failing one, Bazel prints its log, and the full
log of every test stays in `bazel-testlogs/<package>/<test>/test.log` after the
run. Test results are cached as well, so a test whose inputs haven't changed
isn't run again and is reported as `(cached) PASSED`.

Most of the time you want less than everything. Every target has a label: `//`,
the package path below `sdk/`, then a colon and the target name. When the name
matches the last directory, you can leave out the colon and the name, so
`//compiler/damlc` is the `damlc` target in `sdk/compiler/damlc`.

```sh
# Build only damlc and what it depends on
bazelisk build //compiler/damlc

# Build damlc and run it; everything after -- goes to damlc
bazelisk run //compiler/damlc -- --help

# Run one test and show its output as it happens
bazelisk test //bazel/checks/buildifier:buildifier_test --test_output=streamed
```

Streaming output makes Bazel run tests one at a time, so keep it for single
tests.

## Before you commit

CI runs checks on every pull request: language formatters, linters, copyright
headers, and a few rules specific to this repository. They live in
`bazel/checks/`, one directory each, and you can run all of them yourself:

```sh
bazelisk test //bazel/checks
```

The checks only report; they never change your files. Each check's page in
[`bazel/checks/`](../checks/checks.md) names the command that fixes what the
check found.

Rather than remembering to run them, you can let Git run them on every commit:

```sh
bazelisk run //:setup_git_hooks
```

This installs a `pre-commit` hook that runs the fast checks, which are all of
them except HLint and Vale, the two slowest; once built, they take about five
seconds. The hook checks the whole working tree, not only what you staged, and,
like the checks themselves, it fixes nothing. When you need to commit anyway,
`git commit --no-verify` skips it.

## Your editor

Haskell editor support comes from a language server, and a language server can
only compile code with the GHC it was built with. So the repository
builds its own, with the build's GHC, and each supported editor has a setup
target that writes the editor's settings for this checkout and points them at
that server. Run the one for your editor:

```sh
bazelisk run //:setup_vscode     # VS Code, with the Haskell extension
bazelisk run //:setup_vim        # Vim and Neovim, through a project exrc file
bazelisk run //:setup_emacs      # Emacs, for both lsp-mode and eglot
bazelisk run //:setup_intellij   # IntelliJ IDEA, with the Haskell LSP plugin
bazelisk run //:setup_ide        # Any other LSP editor; prints where the server is
```

Each setup prints what to do next in the editor, and the four editor setups undo
what they wrote when you add `-- --remove`. Haskell support works on Linux and
macOS.

Before the language server can help with a file, it has to load the part of the
tree the file belongs to. It asks Bazel to build what that code depends on, such
as the Stackage packages and `ghc-lib`, and then compiles the repository's own
Haskell code itself. The Bazel part runs in the background, with its output
going to the server's log rather than to your screen, so until it is done the
editor gives no Haskell help and does not say what it is waiting for. It looks
broken when it is only busy.

That is why the first run of a setup takes a long time. Besides the language
server, it builds what the editor needs for most of the tree. It does this in
your terminal, where you can watch it, so the editor finds everything ready
when it first starts. The editor runs the same build every time it starts, but
Bazel redoes only what has changed, so as long as nothing connected to the
target changes, the build finishes in a moment. The wait comes back only when
something does: files or build settings the target depends on, whether you
changed them yourself or they came with a pull or a branch switch. And the
first time you open a file in a part of the tree that `hie.yaml` maps to a
target of its own, such as `compiler/damlc/tests`, that target is built from
scratch.

You can wait, and the editor will get there in the end, but it is a blind wait,
and that is what confuses most people. It is better to do what the setup did and
run the build yourself in a terminal before you start the editor:

```sh
bazelisk build //compiler/damlc:damlc@ghci@bios   # What the editor needs for most files
bazelisk build //...                              # Everything, the editor's needs included
```

The first builds only what the editor needs for most of the tree, so it's the
quicker way back to work. Most, but not all: `hie.yaml` lists four directories,
such as `compiler/damlc/tests` and `sdk-integration-tests`, that have their own
targets, and the first command doesn't build those. The second does, along with
everything else, and that pays off after a large pull if your work takes you
into those directories. Otherwise, each of them has a rebuild waiting for the
first time you open one of its files, just as silent as the rest. The full build
lets you pay the whole wait once, up front, where you can see it. It's also
cheaper than it sounds: CI has usually built the same commits already, so most
of it is a download from the remote cache, not a compile.

Neither makes the editor instant. The language server still compiles the
repository's own code when it starts, but most editors show that part as
progress, so you can see it working. And if the editor has already started its
own build, yours waits for it, and Bazel prints `Another command (pid=…) is
running`. That at least tells you the editor is busy, not stuck.

## Where things live

Bazel keeps its work in three places. Downloads, such as toolchains and
libraries, go into the repository cache in `sdk/.bazel-cache/repo`, and the
results of finished build steps into the disk cache in `sdk/.bazel-cache/disk`.
Both survive `bazelisk clean`, and you can delete the whole `.bazel-cache`
directory whenever you need the space back, at the price of downloading and
rebuilding again. The build itself happens in the output base, outside the
checkout (under `~/.cache/bazel` on Linux), and the `bazel-bin`, `bazel-out`,
and
`bazel-testlogs` links in `sdk/` point into it.

Personal settings belong in `sdk/.bazelrc.local`. Git ignores it, and `.bazelrc`
reads it last, so a flag you set there overrides the same flag in `.bazelrc`. A
typical use is moving the caches out of the checkout so that several clones can
share them. The output base cannot be shared, so each clone keeps its own. Use
absolute paths in it, since relative ones depend on where you run Bazel. Also
start each line with the same word as the line it replaces in `.bazelrc`: Bazel
applies all `common` lines before all `build` lines, whichever file they come
from, so a `common` line of yours loses to a `build` line there.

```
startup --output_base=/home/you/daml_build/daml-main/output_base
build --disk_cache=/home/you/daml_build/disk_cache
common --repository_cache=/home/you/daml_build/repository_cache
```
