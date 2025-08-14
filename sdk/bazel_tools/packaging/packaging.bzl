# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Packaging of Linux, macOS and Windows binaries into tarballs"""

load("@os_info//:os_info.bzl", "is_windows")

def _package_app_impl(ctx):
    args = ctx.actions.args()
    args.add(ctx.executable.binary.path)
    args.add(ctx.outputs.out.path)
    args.add_all(ctx.attr.resources, map_each = _get_resource_path)
    ctx.actions.run(
        executable = ctx.executable.package_app,
        outputs = [ctx.outputs.out],
        inputs = ctx.files.resources,
        # Binaries are passed through tools so that Bazel can make the runfiles
        # tree available to the action.
        tools = [ctx.executable.binary],
        arguments = [args],
        progress_message = "Packaging " + ctx.attr.name,
    )

def _package_oci_component_impl(ctx):
    args = ctx.actions.args()
    args.add(ctx.outputs.out.path)
    args.add(ctx.files.component_manifest[0].path)
    args.add(1 if ctx.attr.platform_agnostic else 0)
    args.add_all(ctx.attr.resources, map_each = _get_resource_path)
    ctx.actions.run(
        executable = ctx.executable.package_oci_component,
        outputs = [ctx.outputs.out],
        inputs = ctx.files.resources + ctx.files.component_manifest,
        arguments = [args],
        progress_message = "Packaging OCI " + ctx.attr.name,
    )

def _get_resource_path(r):
    """Return (src, relpath) for each file, preserving relative paths. Pass tarballs as-is."""
    files = r.files.to_list()
    if len(files) == 1:
        return files[0].path

    results = []

    for f in files:
        if f.path.endswith(".tar.gz"):
            results.append(f.path)
        else:
            offset = r.label.package.rfind("/") + 1
            results.append(f.path + ":" + f.short_path[offset:])

    return results

package_app = rule(
    implementation = _package_app_impl,
    attrs = dict({
        "binary": attr.label(
            cfg = "target",
            executable = True,
            allow_files = True,
        ),
        "resources": attr.label_list(
            allow_files = True,
        ),
        "package_app": attr.label(
            default = Label("//bazel_tools/packaging:package-app"),
            cfg = "host",
            executable = True,
            allow_files = True,
        ),
    }),
    outputs = {
        "out": "%{name}.tar.gz",
    },
)
"""Package a binary along with its dynamic library dependencies and data dependencies
  into a tarball. The data dependencies are placed in 'resources' directory.
"""

package_oci_component = rule(
    implementation = _package_oci_component_impl,
    attrs = dict({
        "resources": attr.label_list(
            allow_files = True,
        ),
        "component_manifest": attr.label(
            allow_single_file = True,
        ),
        "platform_agnostic": attr.bool(
            default = False,
        ),
        "package_oci_component": attr.label(
            default = Label("//bazel_tools/packaging:package-oci-component"),
            cfg = "host",
            executable = True,
            allow_files = True,
        ),
    }),
    outputs = {
        "out": "%{name}.tar.gz",
    },
)
"""Package a set of resources into a DPM (Unifi) component. Unpacks tars (i.e. package_app) at root, includes jars directly,
  requires a component.yaml manifest, and allows tagging components as platform agnostic
"""
