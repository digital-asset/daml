# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:scala.bzl", "da_scala_library")
load("//rules_daml:daml.bzl", "daml_compile")
load("//daml-lf/language:daml-lf.bzl", "mangle_for_java")

def to_camel_case(name):
    return "".join([part.capitalize() for part in name.split("_")])

def da_scala_dar_resources_library(
        daml_root_dir,
        daml_dir_names,
        lf_versions,
        add_maven_tag = False,
        maven_name_prefix = "",
        exclusions = {},
        data_dependencies = {},
        **kwargs):
    """
    Define a Scala library with dar files as resources.
    """
    for lf_version in lf_versions:
        for daml_dir_name in daml_dir_names.get(lf_version, []):
            # 1. Compile daml files
            daml_compile_name = "%s-tests-%s" % (daml_dir_name, lf_version)
            daml_compile_kwargs = {
                "project_name": "%s-tests" % daml_dir_name.replace("_", "-"),
                "srcs": native.glob(["%s/%s/*.daml" % (daml_root_dir, daml_dir_name)], exclude = exclusions.get(lf_version, [])),
                "target": lf_version,
            }
            daml_compile_kwargs.update(kwargs)
            daml_compile(
                name = daml_compile_name,
                data_dependencies = [dep % (lf_version) for dep in data_dependencies.get(daml_dir_name, [])],
                **daml_compile_kwargs
            )

        # 2. Generate lookup objects
        genrule_name = "test-dar-lookup-%s" % lf_version
        genrule_command = """
cat > $@ <<EOF
package com.daml.ledger.test

import com.daml.lf.language.LanguageVersion

sealed trait TestDar {{ val path: String }}

object TestDar {{
  val lfVersion: LanguageVersion = LanguageVersion.v{lf_version}
  val paths: List[String] = List(
EOF
""".format(lf_version = mangle_for_java(lf_version)) + "\n".join(["""
echo "    \\"%s/%s-tests-%s.dar\\"," >> $@
""" % (native.package_name(), test_name, lf_version) for test_name in daml_dir_names.get(lf_version, [])]) + """
echo "  )\n}\n" >> $@
""" + "\n".join(["""
echo "case object %sTestDar extends TestDar { val path = \\"%s/%s-tests-%s.dar\\" }" >> $@
""" % (to_camel_case(test_name), native.package_name(), test_name, lf_version) for test_name in daml_dir_names.get(lf_version, [])])

        genrule_kwargs = {
            "outs": ["TestDar-%s.scala" % mangle_for_java(lf_version)],
            "cmd": genrule_command,
        }
        genrule_kwargs.update(kwargs)
        native.genrule(name = genrule_name, **genrule_kwargs)

        # 3. Build a Scala library with the above
        filegroup_name = "dar-files-%s" % lf_version
        filegroup_kwargs = {
            "srcs": ["%s-tests-%s.dar" % (dar_name, lf_version) for dar_name in daml_dir_names.get(lf_version, [])],
        }
        filegroup_kwargs.update(kwargs)
        native.filegroup(name = filegroup_name, **filegroup_kwargs)

        da_scala_library_name = "dar-files-%s-lib" % lf_version
        da_scala_library_kwargs = {
            "srcs": [":test-dar-lookup-%s" % lf_version],
            "generated_srcs": [":test-dar-files-%s.scala" % lf_version],  # required for scaladoc
            "resources": ["dar-files-%s" % lf_version],
            "deps": ["//daml-lf/language"],
        }
        if add_maven_tag:
            da_scala_library_kwargs.update({"tags": ["maven_coordinates=com.daml:%s-dar-files-%s-lib:__VERSION__" % (maven_name_prefix, lf_version)]})
        da_scala_library_kwargs.update(kwargs)
        da_scala_library(name = da_scala_library_name, **da_scala_library_kwargs)
