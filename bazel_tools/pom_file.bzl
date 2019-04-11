# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# These rules are similar to the rules in bazel_common.
# However, our requirements are different in a few ways so we have our own version for now:
# 1. We only include immediate dependencies not all transitive dependencies.
# 2. We support Scala.
# 3. We produce full pom files instead of only the dependency section.
# 4. We have some special options to deal with our specific setup.

MavenInfo = provider(
    fields = {
        "maven_coordinates": """
        The Maven coordinates of this target or the one wrapped by it.
        """,
        "maven_dependencies": """
        The Maven coordinates of the direct dependencies of this target.
        """,
    },
)

_EMPTY_MAVEN_INFO = MavenInfo(
    maven_coordinates = None,
    maven_dependencies = depset(),
)

_MAVEN_COORDINATES_PREFIX = "maven_coordinates="

_SCALA_VERSION = "2.12"

def _maven_coordinates(targets):
    return [target[MavenInfo].maven_coordinates for target in targets if MavenInfo in target and target[MavenInfo].maven_coordinates]

def jar_version(name):
    return name.rsplit("-", 1)[1].rsplit(".", 1)[0]

def _collect_maven_info_impl(_target, ctx):
    tags = getattr(ctx.rule.attr, "tags", [])
    deps = getattr(ctx.rule.attr, "deps", [])
    exports = getattr(ctx.rule.attr, "exports", [])
    jars = getattr(ctx.rule.attr, "jars", [])

    # We need to detect targets generated by bazel-deps to avoid propagating transitive dependencies
    # in that case. This is slightly tricky as bazel-deps generates a java_import that has the tag we care about
    # and then a java_library that has the java_import in exports but also the immediate dependencies.
    # We detect these targets by looking for a jar:jar export.
    bazel_deps_reexport = []
    if ctx.rule.kind == "java_import" or ctx.rule.kind == "java_library":
        for e in exports:
            if e.label.name == "jar" and e.label.package == "jar":
                if MavenInfo not in e:
                    fail("Expected maven info for jar dependency: {}".format(e.label))
                return [e[MavenInfo]]
    elif ctx.rule.kind == "scala_import":
        if len(jars) != 1:
            fail("Expected exactly one jar in a scala_import")
        jar = jars[0]

        # This corresponds replacements section in dependencies.yaml.
        replacements = {
            "io_bazel_rules_scala_scala_compiler": "org.scala-lang:scala-compiler",
            "io_bazel_rules_scala_scala_library": "org.scala-lang:scala-library",
            "io_bazel_rules_scala_scala_reflect": "org.scala-lang:scala-reflect",
            "io_bazel_rules_scala_scala_parser_combinators": "org.scala-lang.modules:scala-parser-combinators",
        }
        if jar.label.workspace_name in replacements:
            return [MavenInfo(
                maven_coordinates = "{}_{}:{}".format(replacements[jar.label.workspace_name], _SCALA_VERSION, jar_version(jar.label.name)),
                maven_dependencies = [],
            )]
        if MavenInfo not in jar:
            fail("Expected maven info for jar dependency: {}".format(jar.label))
        return [jar[MavenInfo]]
    elif ctx.rule.kind == "scala_library":
        # For builtin libraries defined in the replacements section in dependencies.yaml.
        if len(exports) == 1:
            e = exports[0]
            if MavenInfo in e and e[MavenInfo].maven_coordinates:
                if e[MavenInfo].maven_coordinates.startswith("org.scala-lang"):
                    return e[MavenInfo]

    maven_coordinates = None
    only_external_deps = False
    fat_jar = False
    for tag in tags:
        if tag.startswith(_MAVEN_COORDINATES_PREFIX):
            tag_val = tag[len(_MAVEN_COORDINATES_PREFIX):].split(":")
            group_id = tag_val[0]
            artifact_id = tag_val[1]
            version = tag_val[2]
            if ctx.rule.kind.startswith("scala") and version == "__VERSION__":
                artifact_id += "_{}".format(_SCALA_VERSION)
            maven_coordinates = "{}:{}:{}".format(group_id, artifact_id, version)
        if tag == "only_external_deps":
            only_external_deps = True
        if tag == "fat_jar":
            fat_jar = True

    deps = depset([], transitive = [depset([d]) for d in _maven_coordinates(deps + exports + jars)])
    filtered_deps = [
        d for d in deps
          if not (only_external_deps and (d.split(":")[0].startswith("com.daml") or
                                          d.split(":")[0].startswith("com.digitalasset")))
    ]

    if maven_coordinates:
        return [
            MavenInfo(
                maven_coordinates = maven_coordinates,
                maven_dependencies = [] if fat_jar else filtered_deps
            )
        ]
    else:
        return _EMPTY_MAVEN_INFO

_collect_maven_info = aspect(
    attr_aspects = [
        "deps",
        "exports",
        "jars",
    ],
    doc = """
    Collects the Maven information for targets and their dependencies.
    """,
    implementation = _collect_maven_info_impl,
)

DEP_BLOCK = """
<dependency>
  <groupId>{0}</groupId>
  <artifactId>{1}</artifactId>
  <version>{2}</version>
</dependency>
""".strip()

CLASSIFIER_DEP_BLOCK = """
<dependency>
  <groupId>{0}</groupId>
  <artifactId>{1}</artifactId>
  <version>{2}</version>
  <type>{3}</type>
  <classifier>{4}</classifier>
</dependency>
""".strip()

def _pom_file(ctx):
    mvn_deps = ctx.attr.target[MavenInfo].maven_dependencies

    if not ctx.attr.target[MavenInfo].maven_coordinates:
        fail("Target {} needs to have a maven_coordinates tag".format(ctx.attr.target.label))

    maven_coordinates = ctx.attr.target[MavenInfo].maven_coordinates.split(":")
    groupId = maven_coordinates[0]
    artifactId = maven_coordinates[1]
    version= maven_coordinates[2]

    formatted_deps = []
    for dep in [":".join(d) for d in sorted([d.split(":") for d in mvn_deps])]:
        parts = dep.split(":")
        if len(parts) == 3:
            template = DEP_BLOCK
        elif len(parts) == 5:
            template = CLASSIFIER_DEP_BLOCK
        else:
            fail("Unknown dependency format: %s" % dep)

        formatted_deps.append(template.format(*parts))

    pom_file_tmpl = ctx.actions.declare_file(ctx.outputs.pom_file.path + ".tmpl")

    substitutions = {}
    substitutions.update({
        "{generated_bzl_deps}": "\n".join(["    " + l for l in "\n".join(formatted_deps).splitlines()]),
        "{groupId}": groupId,
        "{artifactId}": artifactId,
        "{version}": version,
    })

    ctx.actions.expand_template(
        template = ctx.file.template_file,
        output = pom_file_tmpl,
        substitutions = substitutions,
    )

    ctx.actions.run_shell(
        outputs = [ctx.outputs.pom_file],
        inputs = [pom_file_tmpl, ctx.file.component_version],
        command = """
        VERSION=$(cat {})
        sed "s/__VERSION__/$VERSION/" {} > {}
        """.format(ctx.file.component_version.path, pom_file_tmpl.path, ctx.outputs.pom_file.path),
    )

pom_file = rule(
    attrs = {
        "template_file": attr.label(
            allow_single_file = True,
            default = "//bazel_tools:pom_template.xml",
        ),
        "component_version": attr.label(
            allow_single_file = True,
            default = "//:component-version",
        ),
        "target": attr.label(
            mandatory = True,
            aspects = [_collect_maven_info],
        ),
    },
    outputs = {"pom_file": "%{name}.xml"},
    implementation = _pom_file,
)
