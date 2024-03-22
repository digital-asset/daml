# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//bazel_tools:scala.bzl", "da_scala_library")
load("//rules_daml:daml.bzl", "daml_compile")
load("//daml-lf/language:daml-lf.bzl", "mangle_for_java")
load(
    "//daml-lf/language:daml-lf.bzl",
    "lf_version_configuration",
    "version_in",
)

def to_camel_case(name):
    return "".join([part.capitalize() for part in name.split("_")])

def da_scala_dar_resources_library(
        daml_root_dir,
        test_dars,
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
        for test_dar in test_dars.get(lf_version, {}).keys():
            package_versions = test_dars[lf_version][test_dar]
            if package_versions:
                for package_version in package_versions:
                    daml_compile_name = "%s-tests-%s-%s" % (test_dar, package_version, lf_version)
                    daml_compile_kwargs = {
                        "project_name": "%s-tests" % test_dar.replace("_", "-"),
                        "srcs": native.glob(["%s/%s/%s/*.daml" % (daml_root_dir, test_dar, package_version)], exclude = exclusions.get(lf_version, [])),
                        "target": lf_version,
                        "version": package_version,
                    }
                    daml_compile_kwargs.update(kwargs)
                    daml_compile(
                        name = daml_compile_name,
                        data_dependencies = [dep % (lf_version) for dep in data_dependencies.get(test_dar, [])],
                        **daml_compile_kwargs
                    )
            else:
                daml_compile_name = "%s-tests-%s" % (test_dar, lf_version)
                daml_compile_kwargs = {
                    "project_name": "%s-tests" % test_dar.replace("_", "-"),
                    "srcs": native.glob(["%s/%s/*.daml" % (daml_root_dir, test_dar)], exclude = exclusions.get(lf_version, [])),
                    "target": lf_version,
                }
                daml_compile_kwargs.update(kwargs)
                daml_compile(
                    name = daml_compile_name,
                    data_dependencies = [dep % (lf_version) for dep in data_dependencies.get(test_dar, [])],
                    **daml_compile_kwargs
                )

        # 2. Generate lookup objects
        genrule_name = "test-dar-lookup-%s" % lf_version
        header = """
cat > $@ <<EOF
package com.daml.ledger.test

import com.daml.lf.language.LanguageVersion

sealed trait TestDar {{ val path: String }}

object TestDar {{
  val lfVersion: LanguageVersion = LanguageVersion.v{lf_version}
  val paths: List[String] = List(
EOF
""".format(lf_version = mangle_for_java(lf_version))

        dar_list = []
        for test_dar in test_dars.get(lf_version, {}).keys():
            package_versions = test_dars[lf_version][test_dar]
            if package_versions:
                for package_version in package_versions:
                    dar_list += ["""
echo "    \\"%s/%s-tests-%s-%s.dar\\"," >> $@
""" % (native.package_name(), test_dar, package_version, lf_version)]
            else:
                dar_list += ["""
echo "    \\"%s/%s-tests-%s.dar\\"," >> $@
""" % (native.package_name(), test_dar, lf_version)]

        dar_def_list = []
        for test_dar in test_dars.get(lf_version, {}).keys():
            package_versions = test_dars[lf_version][test_dar]
            if package_versions:
                for package_version in package_versions:
                    dar_def_list += ["""
echo "case object %sTestDar%s extends TestDar { val path = \\"%s/%s-tests-%s-%s.dar\\" }" >> $@
""" % (to_camel_case(test_dar), mangle_for_java(package_version), native.package_name(), test_dar, package_version, lf_version)]
            else:
                dar_def_list += ["""
echo "case object %sTestDar extends TestDar { val path = \\"%s/%s-tests-%s.dar\\" }" >> $@
""" % (to_camel_case(test_dar), native.package_name(), test_dar, lf_version)]

        delimiter = """
echo "  )\n}\n" >> $@
"""

        genrule_kwargs = {
            "outs": ["TestDar-%s.scala" % mangle_for_java(lf_version)],
            "cmd": header + "\n".join(dar_list) + delimiter + "\n".join(dar_def_list),
        }
        genrule_kwargs.update(kwargs)
        native.genrule(name = genrule_name, **genrule_kwargs)

        # 3. Build a Scala library with the above
        srcs = []
        for test_name in test_dars.get(lf_version, {}).keys():
            package_versions = test_dars[lf_version][test_name]
            if package_versions:
                for package_version in package_versions:
                    srcs += ["%s-tests-%s-%s.dar" % (test_name, package_version, lf_version)]
            else:
                srcs += ["%s-tests-%s.dar" % (test_name, lf_version)]
        filegroup_kwargs = {
            "srcs": srcs,
        }
        filegroup_name = "dar-files-%s" % lf_version
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

def merge_test_dars_versioned(lf_versions, general_test_dars, carbon_test_dars, carbon_test_dars_v1_minor_version_range, carbon_test_dars_v2_minor_version_range, upgrading_test_dars, upgrading_test_dars_v1_minor_version_range, upgrading_test_dars_v2_minor_version_range):
    merged_tests = {}
    for lf_version in lf_versions:
        tests_for_lf_version = {}
        tests_for_lf_version.update(general_test_dars)
        if version_in(lf_version, v1_minor_version_range = carbon_test_dars_v1_minor_version_range, v2_minor_version_range = carbon_test_dars_v2_minor_version_range):
            tests_for_lf_version.update(carbon_test_dars)
        if version_in(lf_version, v1_minor_version_range = upgrading_test_dars_v1_minor_version_range, v2_minor_version_range = upgrading_test_dars_v2_minor_version_range):
            tests_for_lf_version.update(upgrading_test_dars)
        merged_tests[lf_version] = tests_for_lf_version
    return merged_tests
