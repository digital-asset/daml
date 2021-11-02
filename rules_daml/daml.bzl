# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@build_environment//:configuration.bzl", "ghc_version", "sdk_version")
load("//bazel_tools/sh:sh.bzl", "sh_inline_test")

_damlc = attr.label(
    allow_single_file = True,
    default = Label("//compiler/damlc:damlc-compile-only"),
    executable = True,
    cfg = "host",
    doc = "The DAML compiler.",
)

_zipper = attr.label(
    allow_single_file = True,
    default = Label("@bazel_tools//tools/zip:zipper"),
    executable = True,
    cfg = "host",
)

def _daml_configure_impl(ctx):
    project_name = ctx.attr.project_name
    project_version = ctx.attr.project_version
    daml_yaml = ctx.outputs.daml_yaml
    target = ctx.attr.target
    opts = ["--target={}".format(target)] if target else []
    ctx.actions.write(
        output = daml_yaml,
        content = """
            sdk-version: {sdk}
            name: {name}
            version: {version}
            source: .
            dependencies: []
            build-options: [{opts} ]
        """.format(
            sdk = sdk_version,
            name = project_name,
            version = project_version,
            opts = ", ".join(opts),
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
    srcs = ctx.files.srcs
    dar_dict = ctx.attr.dar_dict
    damlc = ctx.file._damlc
    input_dars = [file_of_target(k) for k in dar_dict.keys()]
    output_dar = ctx.outputs.dar
    posix = ctx.toolchains["@rules_sh//sh/posix:toolchain_type"]
    ghc_opts = ctx.attr.ghc_options
    ctx.actions.run_shell(
        tools = [damlc],
        inputs = [daml_yaml] + srcs + input_dars,
        outputs = [output_dar],
        progress_message = "Building DAML project %s" % name,
        command = """
            set -eou pipefail
            tmpdir=$(mktemp -d)
            trap "rm -rf $tmpdir" EXIT
            cp -f {config} $tmpdir/daml.yaml
            # Having to produce all the daml.yaml files via a genrule is annoying
            # so we allow hardcoded version numbers and patch them here.
            {sed} -i 's/^sdk-version:.*$/sdk-version: {sdk_version}/' $tmpdir/daml.yaml
            {cp_srcs}
            {cp_dars}
            {damlc} build --project-root $tmpdir {ghc_opts} -o $PWD/{output_dar}
        """.format(
            config = daml_yaml.path,
            cp_srcs = "\n".join([
                make_cp_command(
                    src = src.path,
                    dest = "$tmpdir/" + src.path,
                )
                for src in srcs
            ]),
            cp_dars = "\n".join([
                make_cp_command(
                    src = file_of_target(k).path,
                    dest = "$tmpdir/" + v,
                )
                for k, v in dar_dict.items()
            ]),
            sed = posix.commands["sed"],
            damlc = damlc.path,
            output_dar = output_dar.path,
            sdk_version = sdk_version,
            ghc_opts = " ".join(ghc_opts),
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
        "srcs": attr.label_list(
            allow_files = [".daml"],
            mandatory = True,
            doc = "DAML files in this DAML project.",
        ),
        "dar_dict": attr.label_keyed_string_dict(
            mandatory = True,
            allow_files = True,
            doc = "Other DAML projects referenced by this DAML project.",
        ),
        "dar": attr.output(
            mandatory = True,
            doc = "The generated DAR file.",
        ),
        "ghc_options": attr.string_list(
            doc = "Options passed to GHC.",
            default = ["--ghc-option=-Werror", "--ghc-option=-Wwarn", "--log-level=WARNING"],
        ),
        "_damlc": _damlc,
    },
    toolchains = ["@rules_sh//sh/posix:toolchain_type"],
)

def _extract_main_dalf_impl(ctx):
    project_name = ctx.attr.project_name
    project_version = ctx.attr.project_version
    input_dar = ctx.file.dar
    output_dalf = ctx.outputs.dalf
    zipper = ctx.file._zipper
    posix = ctx.toolchains["@rules_sh//sh/posix:toolchain_type"]
    ctx.actions.run_shell(
        tools = [zipper],
        inputs = [input_dar],
        outputs = [output_dalf],
        progress_message = "Extract DALF from DAR (%s)" % project_name,
        command = """
set -eou pipefail
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT
# While zipper has a -d option, it insists on it
# being a relative path so we don't use it.
ZIPPER=$PWD/{zipper}
DAR=$PWD/{input_dar}
(cd $TMPDIR && $ZIPPER x $DAR)
main_dalf=$({find} $TMPDIR/ -name '{project_name}-{project_version}-[a-z0-9]*.dalf')
cp $main_dalf {output_dalf}
        """.format(
            zipper = zipper.path,
            find = posix.commands["find"],
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
        "_zipper": _zipper,
    },
    toolchains = ["@rules_sh//sh/posix:toolchain_type"],
)

def _daml_validate_test(
        name,
        dar,
        **kwargs):
    damlc = "//compiler/damlc:damlc-compile-only"
    sh_inline_test(
        name = name,
        data = [damlc, dar],
        cmd = """\
DAMLC=$$(canonicalize_rlocation $(rootpath {damlc}))

$$DAMLC validate-dar $$(canonicalize_rlocation $(rootpath {dar}))
""".format(
            damlc = damlc,
            dar = dar,
        ),
        **kwargs
    )

def _inspect_dar(base):
    name = base + "-inspect"
    dar = base + ".dar"
    pp = base + ".dar.pp"
    native.genrule(
        name = name,
        srcs = [
            dar,
            "//compiler/damlc:damlc-compile-only",
        ],
        outs = [pp],
        cmd = "$(location //compiler/damlc:damlc-compile-only) inspect $(location :" + dar + ") > $@",
    )

_default_project_version = "1.0.0"

default_damlc_opts = ["--ghc-option=-Werror", "--ghc-option=-Wwarn", "--log-level=WARNING"]

def daml_compile(
        name,
        srcs,
        version = _default_project_version,
        target = None,
        project_name = None,
        ghc_options = default_damlc_opts,
        **kwargs):
    "Build a DAML project, with a generated daml.yaml."
    if len(srcs) == 0:
        fail("daml_compile: Expected `srcs' to be non-empty.")
    daml_yaml = name + ".yaml"
    _daml_configure(
        name = name + ".configure",
        project_name = project_name or name,
        project_version = version,
        daml_yaml = daml_yaml,
        target = target,
        **kwargs
    )
    _daml_build(
        name = name + ".build",
        daml_yaml = daml_yaml,
        srcs = srcs,
        dar_dict = {},
        dar = name + ".dar",
        ghc_options = ghc_options,
        **kwargs
    )
    _inspect_dar(
        base = name,
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
        daml_yaml = None,
        dar_dict = {},
        ghc_options = default_damlc_opts,
        **kwargs):
    "Build a DAML project and validate the resulting .dar file."
    if not daml_yaml:
        daml_yaml = project_dir + "/" + daml_config_basename
    srcs = native.glob([project_dir + "/" + daml_subdir_basename + "/**/*.daml"])
    _daml_build(
        name = name,
        daml_yaml = daml_yaml,
        srcs = srcs,
        dar_dict = dar_dict,
        dar = name + ".dar",
        ghc_options = ghc_options,
        **kwargs
    )
    _daml_validate_test(
        name = name + ".test",
        dar = name + ".dar",
    )

def daml_test(
        name,
        srcs = [],
        deps = [],
        data_deps = [],
        damlc = "//compiler/damlc:damlc",
        target = None,
        **kwargs):
    sh_inline_test(
        name = name,
        data = [damlc] + srcs + deps + data_deps,
        cmd = """\
set -eou pipefail
tmpdir=$$(mktemp -d)
trap "rm -rf $$tmpdir" EXIT
DAMLC=$$(canonicalize_rlocation $(rootpath {damlc}))
rlocations () {{ for i in $$@; do echo $$(canonicalize_rlocation $$i); done; }}
DEPS=($$(rlocations {deps}))
DATA_DEPS=($$(rlocations {data_deps}))
JOINED_DATA_DEPS="$$(printf ',"%s"' $${{DATA_DEPS[@]}})"
echo "$$JOINED_DATA_DEPS"
cat << EOF > $$tmpdir/daml.yaml
build-options: [{target}]
sdk-version: {sdk_version}
name: test
version: 0.0.1
source: .
dependencies: [daml-stdlib, daml-prim $$([ $${{#DEPS[@]}} -gt 0 ] && printf ',"%s"' $${{DEPS[@]}})]
data-dependencies: [$$([ $${{#DATA_DEPS[@]}} -gt 0 ] && printf '%s' $${{JOINED_DATA_DEPS:1}})]
EOF
cat $$tmpdir/daml.yaml
{cp_srcs}
cd $$tmpdir
$$DAMLC test --files {files}
""".format(
            damlc = damlc,
            files = " ".join(["$(rootpaths %s)" % src for src in srcs]),
            sdk_version = sdk_version,
            deps = " ".join(["$(rootpaths %s)" % dep for dep in deps]),
            data_deps = " ".join(["$(rootpaths %s)" % dep for dep in data_deps]),
            cp_srcs = "\n".join([
                "mkdir -p $$(dirname {dest}); cp -f {src} {dest}".format(
                    src = "$$(canonicalize_rlocation $(rootpath {}))".format(src),
                    dest = "$$tmpdir/$(rootpath {})".format(src),
                )
                for src in srcs
            ]),
            target = "--target=" + target if (target) else "",
        ),
        **kwargs
    )

def daml_doc_test(
        name,
        package_name,
        srcs = [],
        ignored_srcs = [],
        flags = [],
        cpp = "@stackage-exe//hpp",
        damlc = "//compiler/damlc:damlc",
        **kwargs):
    sh_inline_test(
        name = name,
        data = [cpp, damlc] + srcs,
        cmd = """\
set -eou pipefail
CPP=$$(canonicalize_rlocation $(rootpath {cpp}))
DAMLC=$$(canonicalize_rlocation $(rootpath {damlc}))
FILES=($$(
  for file in {files}; do
    IGNORED=false
    for pattern in {ignored}; do
      if [[ $$file = *$$pattern ]]; then
        IGNORED=true
        continue
      fi
    done
    if [[ $$IGNORED == "false" ]]; then
      echo $$(canonicalize_rlocation $$file)
    fi
  done
))

$$DAMLC doctest {flags} --cpp $$CPP --package-name {package_name}-{version} "$${{FILES[@]}}"
""".format(
            cpp = cpp,
            damlc = damlc,
            package_name = package_name,
            flags = " ".join(flags),
            version = ghc_version,
            files = " ".join(["$(rootpaths %s)" % src for src in srcs]),
            ignored = " ".join(ignored_srcs),
        ),
        **kwargs
    )
