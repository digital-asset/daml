# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def file_of_target(k):
    [file] = k.files.to_list()
    return file

_damlc = attr.label(
    allow_single_file = True,
    default = Label("//compiler/damlc"),
    executable = True,
    cfg = "host",
)

def _daml_build_impl(ctx):
    name = ctx.label.name
    config = ctx.file.config
    damls = ctx.files.damls
    dd = ctx.attr.dar_dict
    damlc = ctx.file._damlc
    input_dars = [file_of_target(k) for k in dd.keys()]
    output_dar = ctx.actions.declare_file(name + ".dar")
    ctx.actions.run_shell(
        tools = depset([damlc]),
        inputs = depset([config] + damls + input_dars),
        outputs = [output_dar],
        progress_message = "Building DAML project %s" % name,
        command = """
            set -eou pipefail
            tmpdir=$(mktemp -d)
            trap "rm -rf $tmpdir" EXIT
            mkdir -p $tmpdir/daml
            cp -f {config} $tmpdir
            {cp_damls}
            {cp_dars}
            {damlc} build --project-root $tmpdir -o $PWD/{output_dar}
        """.format(
            config = config.path,
            cp_damls = "\n".join([
                "cp {daml} $tmpdir/daml".format(
                    daml = daml.path,
                )
                for daml in damls
            ]),
            cp_dars = "\n".join([
                "mkdir -p $(dirname $tmpdir/{b}); cp -f {a} $tmpdir/{b}".format(
                    a = file_of_target(k).path,
                    b = v,
                )
                for k, v in dd.items()
            ]),
            damlc = damlc.path,
            output_dar = output_dar.path,
        ),
    )
    return [DefaultInfo(files = depset([output_dar]))]

_daml_build = rule(
    implementation = _daml_build_impl,
    attrs = {
        "config": attr.label(
            allow_single_file = ["daml.yaml"],
            mandatory = True,
            doc = "DAML configuration file to build from.",
        ),
        "damls": attr.label_list(
            allow_files = True,
            mandatory = True,
            doc = "DAML files in this DAML project.",
        ),
        "dar_dict": attr.label_keyed_string_dict(
            default = {},
            doc = "Other DAML projects referenced by this DAML project.",
        ),
        "_damlc": _damlc,
    },
)

def _daml_validate_test_impl(ctx):
    name = ctx.label.name
    project_dar = ctx.file.project_dar
    script = ctx.actions.declare_file(name + ".sh")
    damlc = ctx.file._damlc
    script_content = """
      set -eou pipefail
      DAMLC=$(rlocation $TEST_WORKSPACE/{damlc})
      DAR=$(rlocation $TEST_WORKSPACE/{dar})
      $DAMLC validate-dar $DAR
    """.format(
        damlc = damlc.short_path,
        dar = project_dar.short_path,
    )
    ctx.actions.write(
        output = script,
        content = script_content,
    )
    runfiles = ctx.runfiles(files = [project_dar, damlc])
    return [DefaultInfo(executable = script, runfiles = runfiles)]

_daml_validate_test = rule(
    implementation = _daml_validate_test_impl,
    attrs = {
        "project_dar": attr.label(
            allow_single_file = True,
            mandatory = True,
            doc = "The DAML project whose .dar we want to validate.",
        ),
        "_damlc": _damlc,
    },
    test = True,
)

def daml_build_test(
        name,
        project_dir,
        daml_config_basename = "daml.yaml",
        daml_subdir_basename = "daml",
        **kwargs):
    "Build a DAML project and validate the resulting .dar file."
    config = project_dir + "/" + daml_config_basename
    damls = native.glob([project_dir + "/" + daml_subdir_basename + "/**/*.daml"])
    _daml_build(
        name = name,
        config = config,
        damls = damls,
        **kwargs
    )
    _daml_validate_test(
        name = "{name}.test".format(name = name),
        project_dar = name,
    )
