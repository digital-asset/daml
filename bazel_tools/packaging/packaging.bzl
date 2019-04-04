# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Packaging of Linux and macOS binaries into tarballs"""


def _package_app_impl(ctx):
  files = depset(ctx.attr.binary.files)
  runfiles = ctx.attr.binary.default_runfiles.files
  datafiles = ctx.attr.binary[DefaultInfo].data_runfiles.files

  args = ctx.actions.args()
  inputs = depset([], transitive = [files, runfiles, datafiles] + [r.files for r in ctx.attr.resources])
  ctx.actions.run_shell(
    outputs = [ctx.outputs.out],
    inputs =
        [ctx.executable.package_app, ctx.executable.patchelf, ctx.executable.tar, ctx.executable.gzip]
      + inputs.to_list(),
    arguments = [args],
    progress_message = "Packaging " + ctx.attr.name,
    command = """
      set -eu
      export PATH=$PATH:$PWD/`dirname {patchelf}`:$PWD/`dirname {tar}`:$PWD/`dirname {gzip}`
      {package_app} \
        "$PWD/{binary}" \
        "$PWD/{output}" \
        {resources}
    """.format(
      patchelf = ctx.executable.patchelf.path,
      tar = ctx.executable.tar.path,
      gzip = ctx.executable.gzip.path,
      output = ctx.outputs.out.path,
      name = ctx.attr.name,
      package_app = ctx.executable.package_app.path,
      binary = ctx.executable.binary.path,
      resources = " ".join([
        _get_resource_path(r)
        for r in ctx.attr.resources
      ])
    )
  )

def _get_resource_path(r):
  """Get the path to use for a resource.
    If the resource has a single file, it'll be copied to
    the resource root directory. With multiple files the
    relative directory structure is preserved.

    This mirrors how rules that produce directories work
    in Buck.
  """
  files = r.files.to_list()
  if len(files) > 1:
    first_file = files[0].path
    prefix_end = first_file.index(r.label.package)
    # e.g. package foo/bar,
    # first file at bazel-out/k8-fastbuild/bleh/foo/bar/baz/quux
    # return path as bazel-out/k8-fastbuild/bleh/foo/bar.
    return first_file[0:(prefix_end + len(r.label.package))]
  else:
    return files[0].path

package_app = rule(
  implementation = _package_app_impl,
  attrs = dict({
    "binary": attr.label(
      cfg = "target",
      executable = True
    ),
    "resources": attr.label_list(
      allow_files = True
    ),
    "patchelf": attr.label(
      default = Label("@patchelf_nix//:bin/patchelf"),
      cfg = "host",
      executable = True,
      allow_files = True
    ),
    "tar": attr.label(
      default = Label("@tar_dev_env//:tar"),
      cfg = "host",
      executable = True,
      allow_files = True
    ),
    "gzip": attr.label(
      default = Label("@gzip_dev_env//:gzip"),
      cfg = "host",
      executable = True,
      allow_files = True
    ),
    "package_app": attr.label(
      default = Label("//bazel_tools/packaging:package-app.sh"),
      cfg = "host",
      executable = True,
      allow_files = True
    )
  }),
  outputs = {
    "out": "%{name}.tar.gz",
  }
)
"""Package a binary along with its dynamic library dependencies and data dependencies
  into a tarball. The data dependencies are placed in 'resources' directory.
"""
