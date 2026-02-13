# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# When adding, removing or changing a dependency in this file, update the pinned dependencies by executing
# $ REPIN=1 bazel run @unpinned_maven//:pin
# See https://github.com/bazelbuild/rules_jvm_external#updating-maven_installjson

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")
load(
    "@scala_version//:index.bzl",
    "scala_major_version",
    "scala_version",
)
load(
    "//bazel_tools:scalapb.bzl",
    "scalapb_protoc_version",
    "scalapb_version",
)
load("//canton:canton_version.bzl", "CANTON_OPEN_SOURCE_TAG")
load("@java_deps_json//:java_deps.bzl", "JAVA_DEPS")

version_specific = {
}

# ** Upgrading tcnative in sync with main netty version **
# Look for "tcnative.version" in top-level pom.xml.
# For example for netty version netty-4.1.68.Final look here https://github.com/netty/netty/blob/netty-4.1.68.Final/pom.xml#L511:
# ```
# <tcnative.version>2.0.42.Final</tcnative.version>
# ```

# Bumping versions of io.grpc:* has a few implications:
# 1. io.grpc:grpc-protobuf has a dependency on com.google.protobuf:protobuf-java, which in
#    turn needs to be aligned with the version of protoc we are using (as declared in deps.bzl).
#    ScalaPB also depends on a specific version of protobuf-java, but it's not strict:
#    as long as the version we use is greater than or equal to the version required by ScalaPB,
#    everything should work.
#
# 2. As recommended by https://github.com/grpc/grpc-java/blob/master/SECURITY.md#netty we use

# grpc-netty-shaded than embedded netty and netty-boringssl-tcnative (shaded)

apispec_version = "0.11.7"
grpc_version = "1.77.0"
protobuf_version = "3.25.5"
pekko_version = "1.2.1"
pekko_http_version = "1.1.0"
tapir_version = "1.8.5"

canton_version = CANTON_OPEN_SOURCE_TAG

upickle_version = "4.1.0"
ujson_version = "4.0.2"

guava_version = "33.3.0-jre"

# Updated 2024-03-15
opentelemetry_version = "1.43.0"

# we pick the newest version of opentelemetry_instrumentation built with the opentelemetry version above
# https://github.com/open-telemetry/opentelemetry-java-instrumentation/blob/v2.9.0/dependencyManagement/build.gradle.kts
opentelemetry_instrumentation_version = "2.9.0-alpha"
prometheus_version = "0.16.0"

protostuff_version = "3.1.40"

aws_version = "2.29.5"

# group libraries controlled by the same org
circe_version = "0.14.2"

def install_java_deps():
    maven_install(
        artifacts = version_specific.get(scala_major_version, []) + [
            "com.daml:daml-lf-ide-ledger_{}:{}".format(scala_major_version, canton_version),
            "com.daml:bindings-java:{}".format(canton_version),
            "com.daml:ledger-api-core_{}:{}".format(scala_major_version, canton_version),
            "com.daml:rs-grpc-pekko_{}:{}".format(scala_major_version, canton_version),
            "com.daml:rs-grpc-bridge:{}".format(canton_version),
            # TODO(https://github.com/DACH-NY/canton/issues/30144): check whether this dependency can be gotten rid of
            "com.daml:testing-utils_{}:{}".format(scala_major_version, canton_version),
            # TODO(https://github.com/DACH-NY/canton/issues/30144): move to this repo
            "com.daml:ledger-resources-test-lib_{}:{}".format(scala_major_version, canton_version),
            "com.daml:timer-utils_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-tls_{}:{}".format(scala_major_version, canton_version),
            "com.daml:community-base_{}:{}".format(scala_major_version, canton_version),

            # TODO(https://github.com/DACH-NY/canton/issues/30144): move these dependencies to shared_dependencies in
            #    the canton repo
            "com.oracle.database.jdbc:ojdbc8:19.18.0.0",
            "com.sparkjava:spark-core:2.9.4",
            "com.squareup:javapoet:1.13.0",
            "io.circe:circe-optics_{}:{}".format(scala_major_version, "0.15.0"),
            "io.circe:circe-yaml_{}:{}".format(scala_major_version, "0.15.0-RC1"),
            "io.reactivex.rxjava2:rxjava:2.2.21",
            "org.junit.jupiter:junit-jupiter-engine:5.9.2",
            "org.junit.platform:junit-platform-runner:1.9.2",
            "org.scalatestplus:scalacheck-1-15_{}:3.2.11.0".format(scala_major_version),
            "org.tpolecat:doobie-postgres_{}:0.13.4".format(scala_major_version),
            "org.typelevel:kind-projector_{}:0.13.3".format(scala_version),
            "org.wartremover:wartremover_{}:3.2.5".format(scala_version),
        ] + ["{}:{}".format(artifact, version) for artifact, version in JAVA_DEPS.items()],
        fetch_sources = True,
        maven_install_json = "@com_github_digital_asset_daml//:maven_install_{}.json".format(scala_major_version),
        override_targets = {
            # Replacements for core Scala libraries.
            # These libraries must be provided by the Scala toolchain.
            #
            # Without these you may get obscure compiler errors about missing implicits,
            # or types that should be `Any`.
            # This needs to be kept in sync with //bazel-tools:pom_file.bzl
            "org.scala-lang:scala-compiler": "@io_bazel_rules_scala_scala_compiler//:io_bazel_rules_scala_scala_compiler",
            "org.scala-lang:scala-library": "@io_bazel_rules_scala_scala_library//:io_bazel_rules_scala_scala_library",
            "org.scala-lang:scala-reflect": "@io_bazel_rules_scala_scala_reflect//:io_bazel_rules_scala_scala_reflect",
            "org.scala-lang.modules:scala-parser-combinators": "@io_bazel_rules_scala_scala_parser_combinators//:io_bazel_rules_scala_scala_parser_combinators",
            "org.scala-tools.testing:test-interface": "//:org_scala_sbt_test_interface",
            "org.scalactic:scalactic_{}".format(scala_major_version): "@io_bazel_rules_scala_scalactic//:io_bazel_rules_scala_scalactic",
            "org.scalatest:scalatest_{}".format(scala_major_version): "@io_bazel_rules_scala_scalatest//:io_bazel_rules_scala_scalatest",
        },
        repositories = [
            "https://repo1.maven.org/maven2",
            "https://europe-maven.pkg.dev/da-images/public-maven-unstable",
        ],
        # The strict_visibility attribute controls whether all artifacts should
        # be visible (including transitive dependencies), or whether only
        # explicitly declared artifacts should be visible. The targets
        # generated by maven_install do not forward transitive dependencies.
        # Instead, users need to explicitly declare each package a dependency
        # from which they wish to import. This makes strict visibility
        # inconvenient as one would have to pin versions of transitive
        # dependencies in this file, which complicates version updates later
        # on. Therefore, we don't enable strict visibility. This is the default.
        # strict_visibility = True,
        version_conflict_policy = "pinned",
        fail_if_repin_required = True,
    )
