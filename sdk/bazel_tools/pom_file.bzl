# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# These rules are similar to the rules in bazel_common.
# However, our requirements are different in a few ways so we have our own version for now:
# 1. We only include immediate dependencies not all transitive dependencies.
# 2. We support Scala.
# 3. We produce full pom files instead of only the dependency section.
# 4. We have some special options to deal with our specific setup.

load("@scala_version//:index.bzl", "scala_major_version")

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
    maven_dependencies = [],
)

_MAVEN_COORDINATES_PREFIX = "maven_coordinates="

# Map from a dependency to the exclusions for that dependency.
# The exclusions will be automatically inserted in every pom file that
# depends on the target.
EXCLUSIONS = {"io.grpc:grpc-protobuf": ["com.google.protobuf:protobuf-lite"]}

def _maven_coordinates(targets):
    return [target[MavenInfo].maven_coordinates for target in targets if MavenInfo in target and target[MavenInfo].maven_coordinates]

def jar_version(name):
    return name.rsplit("-", 1)[1].rsplit(".", 1)[0]

def has_scala_version_suffix(kind, version, tags):
    if not kind.startswith("scala"):
        return False
    if version != "__VERSION__":
        return False
    for tag in tags:
        if tag == "no_scala_version_suffix":
            return False
    return True

def _collect_maven_info_impl(_target, ctx):
    tags = getattr(ctx.rule.attr, "tags", [])
    deps = getattr(ctx.rule.attr, "deps", [])
    runtime_deps = getattr(ctx.rule.attr, "runtime_deps", [])
    exports = getattr(ctx.rule.attr, "exports", [])
    jars = getattr(ctx.rule.attr, "jars", [])

    if ctx.rule.kind == "scala_import":
        if ctx.label == Label("@io_bazel_rules_scala//scala/scalatest:scalatest"):
            # rules_scala meta package that introduces scalatest and scalactic.
            # The scalatest and scalctic packages will be captured by the aspect,
            # since it traverses along `exports`.
            return []
        if len(jars) != 1:
            fail("Expected exactly one jar in a scala_import")
        jar = jars[0]

        # This corresponds replacements section in dependencies.yaml.
        replacements = {
            "io_bazel_rules_scala_scala_compiler": "org.scala-lang:scala-compiler",
            "io_bazel_rules_scala_scala_library": "org.scala-lang:scala-library",
            "io_bazel_rules_scala_scala_reflect": "org.scala-lang:scala-reflect",
            "io_bazel_rules_scala_scala_parser_combinators": "org.scala-lang.modules:scala-parser-combinators_{}".format(scala_major_version),
            "io_bazel_rules_scala_scalactic": "org.scalactic:scalactic_{}".format(scala_major_version),
            "io_bazel_rules_scala_scalatest": "org.scalatest:scalatest_{}".format(scala_major_version),
        }
        if jar.label.workspace_name in replacements:
            return [MavenInfo(
                maven_coordinates = "{}:{}".format(replacements[jar.label.workspace_name], jar_version(jar.label.name)),
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
            if has_scala_version_suffix(ctx.rule.kind, version, tags):
                artifact_id += "_{}".format(scala_major_version)
            maven_coordinates = "{}:{}:{}".format(group_id, artifact_id, version)
        if tag == "only_external_deps":
            only_external_deps = True
        if tag == "fat_jar":
            fat_jar = True

    deps = depset([], transitive = [depset([d]) for d in _maven_coordinates(deps + runtime_deps + exports + jars)])
    filtered_deps = [
        d
        for d in deps.to_list()
        if not (only_external_deps and (d.split(":")[0].startswith("com.daml") or
                                        d.split(":")[0].startswith("com.digitalasset")))
    ]

    if maven_coordinates:
        return [
            MavenInfo(
                maven_coordinates = maven_coordinates,
                maven_dependencies = [] if fat_jar else filtered_deps,
            ),
        ]
    else:
        return _EMPTY_MAVEN_INFO

_collect_maven_info = aspect(
    attr_aspects = [
        "deps",
        "exports",
        "jars",
        "runtime_deps",
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
  <exclusions>
{3}
  </exclusions>
</dependency>
""".strip()

CLASSIFIER_DEP_BLOCK = """
<dependency>
  <groupId>{0}</groupId>
  <artifactId>{1}</artifactId>
  <version>{2}</version>
  <type>{3}</type>
  <classifier>{4}</classifier>
  <exclusions>
{5}
  </exclusions>
</dependency>
""".strip()

EXCLUSION_BLOCK = """
<exclusion>
  <groupId>{0}</groupId>
  <artifactId>{1}</artifactId>
</exclusion>
""".strip()

def _pom_file(ctx):
    mvn_deps = ctx.attr.target[MavenInfo].maven_dependencies

    if not ctx.attr.target[MavenInfo].maven_coordinates:
        fail("Target {} needs to have a maven_coordinates tag".format(ctx.attr.target.label))

    maven_coordinates = ctx.attr.target[MavenInfo].maven_coordinates.split(":")
    groupId = maven_coordinates[0]
    artifactId = maven_coordinates[1]
    version = maven_coordinates[2]

    formatted_deps = []
    for dep in [":".join(d) for d in sorted([d.split(":") for d in mvn_deps])]:
        parts = dep.split(":")
        if len(parts) == 3:
            template = DEP_BLOCK
        elif len(parts) == 5:
            template = CLASSIFIER_DEP_BLOCK
        else:
            fail("Unknown dependency format: %s" % dep)
        exclusions = EXCLUSIONS.get("{}:{}".format(parts[0], parts[1]), [])
        formatted_exclusions = []
        for exclusion in exclusions:
            exclusion_parts = exclusion.split(":")
            formatted_exclusions += [EXCLUSION_BLOCK.format(*exclusion_parts)]
        parts += ["\n".join(["    " + l for l in "\n".join(formatted_exclusions).splitlines()])]
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
            default = "//:MVN_VERSION",
        ),
        "target": attr.label(
            mandatory = True,
            aspects = [_collect_maven_info],
        ),
    },
    outputs = {"pom_file": "%{name}.xml"},
    implementation = _pom_file,
)
