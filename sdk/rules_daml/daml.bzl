# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@build_environment//:configuration.bzl", "ghc_version", "sdk_version")
load("//bazel_tools/sh:sh.bzl", "sh_inline_test")
load("//daml-lf/language:daml-lf.bzl", "COMPILER_LF_VERSIONS", "version_in")
load("@bazel_skylib//lib:paths.bzl", "paths")
load("@os_info//:os_info.bzl", "is_windows")

_damlc = attr.label(
    default = Label("//compiler/damlc:damlc-compile-only"),
    executable = True,
    cfg = "host",
    doc = "The Daml compiler.",
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
    dependencies = ctx.attr.dependencies
    data_dependencies = ctx.attr.data_dependencies
    module_prefixes = ctx.attr.module_prefixes
    upgrades = ctx.attr.upgrades
    typecheck_upgrades = ctx.attr.typecheck_upgrades
    daml_yaml = ctx.outputs.daml_yaml
    target = ctx.attr.target
    opts = (
        (["--target={}".format(target)] if target else []) +
        (["--typecheck-upgrades=no"] if not typecheck_upgrades and using_local_compiler(target) else [])
    )
    ctx.actions.write(
        output = daml_yaml,
        content = """
sdk-version: {sdk}
name: {name}
version: {version}
source: .
data-dependencies: [{data_dependencies}]
dependencies: [{dependencies}]
module-prefixes:
{module_prefixes}
build-options: [{opts}]
{upgrades}
""".format(
            sdk = sdk_version,
            name = project_name,
            version = project_version,
            opts = ", ".join(opts),
            dependencies = ", ".join(dependencies),
            data_dependencies = ", ".join(data_dependencies),
            module_prefixes = "\n".join(["  {}: {}".format(k, v) for k, v in module_prefixes.items()]),
            upgrades = "upgrades: " + upgrades if upgrades and using_local_compiler(target) else "",
        ),
    )

_daml_configure = rule(
    implementation = _daml_configure_impl,
    attrs = {
        "project_name": attr.string(
            mandatory = True,
            doc = "Name of the Daml project.",
        ),
        "project_version": attr.string(
            mandatory = True,
            doc = "Version of the Daml project.",
        ),
        "daml_yaml": attr.output(
            mandatory = True,
            doc = "The generated daml.yaml config file.",
        ),
        "target": attr.string(
            doc = "Daml-LF version to output.",
        ),
        "dependencies": attr.string_list(
            doc = "Dependencies.",
        ),
        "data_dependencies": attr.string_list(
            doc = "Data dependencies.",
        ),
        "module_prefixes": attr.string_dict(
            doc = "Module prefixes.",
        ),
        "upgrades": attr.string(
            doc = "Upgraded package path.",
        ),
        "typecheck_upgrades": attr.bool(
            doc = "Whether or not to typecheck against the upgraded package.",
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
    damlc = ctx.executable.damlc
    input_dars = [file_of_target(k) for k in dar_dict.keys()]
    output_dar = ctx.outputs.dar
    posix = ctx.toolchains["@rules_sh//sh/posix:toolchain_type"]
    ghc_opts = ctx.attr.ghc_options
    ctx.actions.run_shell(
        tools = [damlc],
        inputs = [daml_yaml] + srcs + input_dars,
        outputs = [output_dar],
        progress_message = "Building Daml project %s" % name,
        command = """
            set -eou pipefail
            tmpdir=$(mktemp -d)
            trap "rm -rf $tmpdir" EXIT
            cp -f {config} $tmpdir/daml.yaml
            # Having to produce all the daml.yaml files via a genrule is annoying
            # so we allow hardcoded version numbers and patch them here.
            {sed} -i 's/^sdk-version:.*$/sdk-version: {sdk_version}/' $tmpdir/daml.yaml
            {sed} -i 's/daml-script$/daml-script.dar/;s/daml-trigger$/daml-trigger.dar/' $tmpdir/daml.yaml
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
            dars = dar_dict,
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
            doc = "Daml files in this Daml project.",
        ),
        "dar_dict": attr.label_keyed_string_dict(
            mandatory = True,
            allow_files = True,
            doc = "Other Daml projects referenced by this Daml project.",
        ),
        "dar": attr.output(
            mandatory = True,
            doc = "The generated DAR file.",
        ),
        "ghc_options": attr.string_list(
            doc = "Options passed to GHC.",
            default = ["--ghc-option=-Werror", "--log-level=WARNING"],
        ),
        "damlc": _damlc,
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
            doc = "Name of the Daml project.",
        ),
        "project_version": attr.string(
            mandatory = True,
            doc = "Version of the Daml project.",
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

def _inspect_dar_impl(ctx):
    dar = ctx.file.dar
    damlc = ctx.executable.damlc
    pp = ctx.outputs.pp
    ctx.actions.run(
        executable = damlc,
        inputs = [dar],
        outputs = [pp],
        arguments = ["inspect", dar.path, "-o", pp.path],
    )

_inspect_dar = rule(
    implementation = _inspect_dar_impl,
    attrs = {
        "dar": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "damlc": _damlc,
        "pp": attr.output(mandatory = True),
    },
)

_default_project_version = "1.0.0"

default_damlc_opts = ["--ghc-option=-Werror", "--log-level=WARNING"]

def using_local_compiler(target):
    return not target or target in COMPILER_LF_VERSIONS

def damlc_for_target(target):
    if using_local_compiler(target):
        return "//compiler/damlc:damlc-compile-only"
    else:
        return "@damlc_legacy//:damlc_legacy"

def path_to_dar(data):
    return Label(data).name + ".dar"

def _supports_scenarios(lf_version):
    return version_in(
        lf_version,
        v1_minor_version_range = ("14", "dev"),
        # TODO(#17366): change to None when we deprecate scenarios in 2.x
        v2_minor_version_range = ("0", "dev"),
    )

def daml_compile(
        name,
        srcs,
        version = _default_project_version,
        target = None,
        project_name = None,
        ghc_options = default_damlc_opts,
        enable_scenarios = False,
        dependencies = [],
        data_dependencies = [],
        module_prefixes = None,
        upgrades = None,
        typecheck_upgrades = False,
        **kwargs):
    "Build a Daml project, with a generated daml.yaml."
    if len(srcs) == 0:
        fail("daml_compile: Expected `srcs' to be non-empty.")
    daml_yaml = name + ".yaml"
    _daml_configure(
        dependencies = [path_to_dar(dep) for dep in dependencies],
        data_dependencies = [path_to_dar(data) for data in data_dependencies],
        module_prefixes = module_prefixes,
        upgrades = path_to_dar(upgrades) if upgrades else "",
        typecheck_upgrades = typecheck_upgrades,
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
        dar_dict =
            {dar: path_to_dar(dar) for dar in (dependencies + data_dependencies + ([upgrades] if upgrades else []))},
        dar = name + ".dar",
        ghc_options =
            ghc_options +
            (["--enable-scenarios=yes"] if enable_scenarios and (target == None or _supports_scenarios(target)) else []),
        damlc = damlc_for_target(target),
        **kwargs
    )
    _inspect_dar(
        name = "{}-inspect".format(name),
        dar = "{}.dar".format(name),
        pp = "{}.dar.pp".format(name),
        damlc = damlc_for_target(target),
    )

def daml_compile_with_dalf(
        name,
        version = _default_project_version,
        **kwargs):
    "Build a Daml project, with a generated daml.yaml, and extract the main DALF."
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

def generate_and_track_dar_hash_file(name):
    """
    Given a 'name', defines the following targets:
      (1) ':{name}-dar-hash-file-matches',
      (2) ':{name}-golden-hash'.
      (3) ':{name}-generated-hash',
    Target (1) is a test that checks that the files in targets (2) and (3) match.
    It can also replace (2) with the output of (3) if run with the flag '--accept'.
    Target (2) is an indirection to the file
      ':{name}.dar-hash',
    which must be present in the package where this macro is used.
    Target (3) is transitively defined by 'generate_dar_hash_file'.

    These targets are only generated if 'sdk_version == "0.0.0"'.
    """
    if sdk_version == "0.0.0":
        generate_dar_hash_file(name)

        native.filegroup(
            name = name + "-golden-hash",
            srcs = native.glob([name + ".dar-hash"]),
            visibility = ["//visibility:public"],
        )

        test_name = name + "-dar-hash-file-matches"

        lbl = "//{package}:{target}".format(
            package = native.package_name(),
            target = test_name,
        )

        native.sh_test(
            name = test_name,
            srcs = ["//bazel_tools:match-golden-file"],
            args = [
                lbl,
                "$(location :{}-generated-hash)".format(name),
                "$(location :{}-golden-hash)".format(name),
                "$(POSIX_DIFF)",
            ],
            data = [
                ":{}-generated-hash".format(name),
                ":{}-golden-hash".format(name),
            ],
            toolchains = [
                "@rules_sh//sh/posix:make_variables",
            ],
            deps = [
                "@bazel_tools//tools/bash/runfiles",
            ],
        )

def generate_dar_hash_file(name):
    """
    Given a 'name', defines a target of the form
      ':{name}-generated-hash'.
    with a single file of the form
      ':{name}-generated.dar-hash'.
    The resulting file will contain one line per file present in
      ':{name}.dar',
    sorted by filename and excluding *.hi and *.hie files, where each line
    has the sha256sum for the file followed by the path of the file relative to
    the root of the dar.
    """
    native.genrule(
        name = name + "-generated-hash",
        srcs = ["//rules_daml:generate-dar-hash", ":{}.dar".format(name)],
        tools = ["@python_dev_env//:python"] if is_windows else [],
        outs = [name + "-generated.dar-hash"],
        cmd = "{python} {exe} {dar} > $@".format(
            python = "$(execpath @python_dev_env//:python)" if is_windows else "python",
            exe = "$(location //rules_daml:generate-dar-hash)",
            dar = "$(location :{}.dar)".format(name),
        ),
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
    "Build a Daml project and validate the resulting .dar file."
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
        module_prefixes = {},
        damlc = "//compiler/damlc:damlc",
        additional_compiler_flags = [],
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
module-prefixes:
{module_prefixes}
EOF
cat $$tmpdir/daml.yaml
{cp_srcs}
cd $$tmpdir
$$DAMLC test {damlc_opts} --files {files}
""".format(
            damlc = damlc,
            files = " ".join(["$(rootpaths %s)" % src for src in srcs]),
            sdk_version = sdk_version,
            deps = " ".join(["$(rootpaths %s)" % dep for dep in deps]),
            data_deps = " ".join(["$(rootpaths %s)" % dep for dep in data_deps]),
            module_prefixes = "\n".join(["  {}: {}".format(k, v) for k, v in module_prefixes.items()]),
            damlc_opts = " ".join(default_damlc_opts + additional_compiler_flags),
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
        script_dar = "//daml-script/daml:daml-script.dar",
        **kwargs):
    sh_inline_test(
        name = name,
        data = [cpp, damlc, script_dar] + srcs,
        cmd = """\
set -eou pipefail
CPP=$$(canonicalize_rlocation $(rootpath {cpp}))
DAMLC=$$(canonicalize_rlocation $(rootpath {damlc}))
SCRIPT_DAR=$$(canonicalize_rlocation $(rootpath {script_dar}))
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
$$DAMLC doctest {flags} --script-lib $$SCRIPT_DAR --cpp $$CPP --package-name {package_name}-{version} "$${{FILES[@]}}"
""".format(
            cpp = cpp,
            damlc = damlc,
            script_dar = script_dar,
            package_name = package_name,
            flags = " ".join(flags),
            version = ghc_version,
            files = " ".join(["$(rootpaths %s)" % src for src in srcs]),
            ignored = " ".join(ignored_srcs),
        ),
        **kwargs
    )
