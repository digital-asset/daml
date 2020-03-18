# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@build_environment//:configuration.bzl", "ghc_version")

_damlc = attr.label(
    allow_single_file = True,
    default = Label("//compiler/damlc"),
    executable = True,
    cfg = "host",
    doc = "The DAML compiler.",
)

def _daml_configure_impl(ctx):
    project_name = ctx.attr.project_name
    project_version = ctx.attr.project_version
    daml_yaml = ctx.outputs.daml_yaml
    target = ctx.attr.target
    ctx.actions.write(
        output = daml_yaml,
        content = """
            sdk-version: 0.0.0
            name: {name}
            version: {version}
            source: .
            dependencies: []
            build-options: [{target}]
        """.format(
            name = project_name,
            version = project_version,
            target = "--target=" + target if (target) else "",
        ),
    )

_daml_configure = rule(
    implementation = _daml_configure_impl,
    attrs = {
        "project_name": attr.string(
            mandatory = True,
            doc = "Name of the DAML project.",
        ),
        "project_version": attr.string(
            mandatory = True,
            doc = "Version of the DAML project.",
        ),
        "daml_yaml": attr.output(
            mandatory = True,
            doc = "The generated daml.yaml config file.",
        ),
        "target": attr.string(
            doc = "DAML-LF version to output.",
        ),
    },
)

def file_of_target(k):
    [file] = k.files.to_list()
    return file

def make_cp_command(src, dest):
    return "mkdir -p $(dirname {dest}); cp -f {src} {dest}".format(
        src = src,
        dest = dest,
    )

def _daml_build_impl(ctx):
    name = ctx.label.name
    daml_yaml = ctx.file.daml_yaml
    damls = ctx.files.damls
    dar_dict = ctx.attr.dar_dict
    damlc = ctx.file._damlc
    input_dars = [file_of_target(k) for k in dar_dict.keys()]
    output_dar = ctx.outputs.dar
    ctx.actions.run_shell(
        tools = [damlc],
        inputs = [daml_yaml] + damls + input_dars,
        outputs = [output_dar],
        progress_message = "Building DAML project %s" % name,
        command = """
            set -eou pipefail
            tmpdir=$(mktemp -d)
            trap "rm -rf $tmpdir" EXIT
            cp -f {config} $tmpdir/daml.yaml
            {cp_damls}
            {cp_dars}
            {damlc} build --project-root $tmpdir -o $PWD/{output_dar}
        """.format(
            config = daml_yaml.path,
            cp_damls = "\n".join([
                make_cp_command(
                    src = daml.path,
                    dest = "$tmpdir/" + daml.path,
                )
                for daml in damls
            ]),
            cp_dars = "\n".join([
                make_cp_command(
                    src = file_of_target(k).path,
                    dest = "$tmpdir/" + v,
                )
                for k, v in dar_dict.items()
            ]),
            damlc = damlc.path,
            output_dar = output_dar.path,
        ),
    )

_daml_build = rule(
    implementation = _daml_build_impl,
    attrs = {
        "daml_yaml": attr.label(
            allow_single_file = True,
            mandatory = True,
            doc = "The daml.yaml config file.",
        ),
        "damls": attr.label_list(
            allow_files = [".daml"],
            mandatory = True,
            doc = "DAML files in this DAML project.",
        ),
        "dar_dict": attr.label_keyed_string_dict(
            mandatory = True,
            doc = "Other DAML projects referenced by this DAML project.",
        ),
        "dar": attr.output(
            mandatory = True,
            doc = "The generated DAR file.",
        ),
        "_damlc": _damlc,
    },
)

def _extract_main_dalf_impl(ctx):
    project_name = ctx.attr.project_name
    project_version = ctx.attr.project_version
    input_dar = ctx.file.dar
    output_dalf = ctx.outputs.dalf
    ctx.actions.run_shell(
        inputs = [input_dar],
        outputs = [output_dalf],
        progress_message = "Extract DALF from DAR (%s)" % project_name,
        command = """
            set -eou pipefail
            unzip -q {input_dar}
            main_dalf=$(find . -name '{project_name}-{project_version}-[a-z0-9]*.dalf')
            cp $main_dalf {output_dalf}
        """.format(
            project_name = project_name,
            project_version = project_version,
            input_dar = input_dar.path,
            output_dalf = output_dalf.path,
        ),
    )

_extract_main_dalf = rule(
    implementation = _extract_main_dalf_impl,
    attrs = {
        "project_name": attr.string(
            mandatory = True,
            doc = "Name of the DAML project.",
        ),
        "project_version": attr.string(
            mandatory = True,
            doc = "Version of the DAML project.",
        ),
        "dar": attr.label(
            allow_single_file = [".dar"],
            mandatory = True,
            doc = "The DAR from which the DALF will be extracted.",
        ),
        "dalf": attr.output(
            mandatory = True,
            doc = "The extracted DALF.",
        ),
        "_damlc": _damlc,
    },
)

def _daml_validate_test_impl(ctx):
    name = ctx.label.name
    dar = ctx.file.dar
    script = ctx.actions.declare_file(name + ".sh")
    damlc = ctx.file._damlc
    script_content = """
      set -eou pipefail
      DAMLC=$(rlocation $TEST_WORKSPACE/{damlc})
      DAR=$(rlocation $TEST_WORKSPACE/{dar})
      $DAMLC validate-dar $DAR
    """.format(
        damlc = damlc.short_path,
        dar = dar.short_path,
    )
    ctx.actions.write(
        output = script,
        content = script_content,
    )
    runfiles = ctx.runfiles(files = [dar, damlc])
    return [DefaultInfo(executable = script, runfiles = runfiles)]

_daml_validate_test = rule(
    implementation = _daml_validate_test_impl,
    attrs = {
        "dar": attr.label(
            allow_single_file = True,
            mandatory = True,
            doc = "The DAR to validate.",
        ),
        "_damlc": _damlc,
    },
    test = True,
)

_default_project_version = "1.0.0"

def daml_compile(
        name,
        srcs,
        version = _default_project_version,
        target = None,
        **kwargs):
    "Build a DAML project, with a generated daml.yaml."
    daml_yaml = name + ".yaml"
    _daml_configure(
        name = name + ".configure",
        project_name = name,
        project_version = version,
        daml_yaml = daml_yaml,
        target = target,
        **kwargs
    )
    _daml_build(
        name = name + ".build",
        daml_yaml = daml_yaml,
        damls = srcs,
        dar_dict = {},
        dar = name + ".dar",
        **kwargs
    )

def daml_compile_with_dalf(
        name,
        version = _default_project_version,
        **kwargs):
    "Build a DAML project, with a generated daml.yaml, and extract the main DALF."
    daml_compile(
        name = name,
        version = version,
        **kwargs
    )
    _extract_main_dalf(
        name = name + ".extract",
        project_name = name,
        project_version = version,
        dar = name + ".dar",
        dalf = name + ".dalf",
    )

def daml_build_test(
        name,
        project_dir,
        daml_config_basename = "daml.yaml",
        daml_subdir_basename = "daml",
        dar_dict = {},
        **kwargs):
    "Build a DAML project and validate the resulting .dar file."
    daml_yaml = project_dir + "/" + daml_config_basename
    damls = native.glob([project_dir + "/" + daml_subdir_basename + "/**/*.daml"])
    _daml_build(
        name = name,
        daml_yaml = daml_yaml,
        damls = damls,
        dar_dict = dar_dict,
        dar = name + ".dar",
        **kwargs
    )
    _daml_validate_test(
        name = name + ".test",
        dar = name + ".dar",
    )

def _daml_test_impl(ctx):
    script = """
      set -eou pipefail

      DAMLC=$(rlocation $TEST_WORKSPACE/{damlc})
      rlocations () {{ for i in $@; do echo $(rlocation $TEST_WORKSPACE/$i); done; }}

      $DAMLC test --files $(rlocations "{files}")
    """.format(
        damlc = ctx.executable.damlc.short_path,
        files = " ".join([f.short_path for f in ctx.files.srcs]),
    )

    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )
    damlc_runfiles = ctx.attr.damlc[DefaultInfo].data_runfiles
    runfiles = ctx.runfiles(
        collect_data = True,
        files = ctx.files.srcs,
    ).merge(damlc_runfiles)
    return [DefaultInfo(runfiles = runfiles)]

daml_test = rule(
    implementation = _daml_test_impl,
    attrs = {
        "srcs": attr.label_list(
            allow_files = [".daml"],
            default = [],
            doc = "DAML source files to test.",
        ),
        "damlc": attr.label(
            executable = True,
            cfg = "host",
            allow_files = True,
            default = Label("//compiler/damlc"),
        ),
    },
    test = True,
)

def _daml_doctest_impl(ctx):
    script = """
      set -eou pipefail
      DAMLC=$(rlocation $TEST_WORKSPACE/{damlc})
      CPP=$(rlocation $TEST_WORKSPACE/{cpp})
      rlocations () {{ for i in $@; do echo $(rlocation $TEST_WORKSPACE/$i); done; }}
      $DAMLC doctest {flags} --cpp $CPP --package-name {package_name}-{version} $(rlocations "{files}")
    """.format(
        damlc = ctx.executable.damlc.short_path,
        # we end up with "../hpp/hpp" while we want "external/hpp/hpp"
        # so we just do the replacement ourselves.
        cpp = ctx.executable.cpp.short_path.replace("..", "external"),
        package_name = ctx.attr.package_name,
        flags = " ".join(ctx.attr.flags),
        version = ghc_version,
        files = " ".join([
            f.short_path
            for f in ctx.files.srcs
            if all([not f.short_path.endswith(ignore) for ignore in ctx.attr.ignored_srcs])
        ]),
    )
    ctx.actions.write(
        output = ctx.outputs.executable,
        content = script,
    )
    damlc_runfiles = ctx.attr.damlc[DefaultInfo].data_runfiles
    cpp_runfiles = ctx.attr.cpp[DefaultInfo].data_runfiles
    runfiles = ctx.runfiles(
        collect_data = True,
        files = ctx.files.srcs,
    ).merge(damlc_runfiles).merge(cpp_runfiles)
    return [DefaultInfo(runfiles = runfiles)]

daml_doc_test = rule(
    implementation = _daml_doctest_impl,
    attrs = {
        "srcs": attr.label_list(
            allow_files = [".daml"],
            default = [],
            doc = "DAML source files that should be tested.",
        ),
        "ignored_srcs": attr.string_list(
            default = [],
            doc = "DAML source files that should be ignored.",
        ),
        "damlc": attr.label(
            executable = True,
            cfg = "host",
            allow_files = True,
            default = Label("//compiler/damlc"),
        ),
        "cpp": attr.label(
            executable = True,
            cfg = "host",
            allow_files = True,
            default = Label("@hpp//:hpp"),
        ),
        "flags": attr.string_list(
            default = [],
            doc = "Flags for damlc invokation.",
        ),
        "package_name": attr.string(),
    },
    test = True,
)
