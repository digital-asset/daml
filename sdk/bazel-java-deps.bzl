# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# When adding, removing or changing a dependency in this file, update the pinned dependencies by executing
# $ REPIN=1 bazel run @unpinned_maven//:pin
# See https://github.com/bazelbuild/rules_jvm_external#updating-maven_installjson

load("@rules_jvm_external//:defs.bzl", "maven_install")
load(
    "@scala_version//:index.bzl",
    "scala_major_version",
    "scala_version",
)
load("//canton:canton_version.bzl", "CANTON_OPEN_SOURCE_TAG", "LOCAL_CANTON_PATH")
load("@java_deps_json//:java_deps.bzl", "JAVA_DEPS")

version_specific = {
}

canton_version = "3.7.0-SNAPSHOT" if LOCAL_CANTON_PATH != None else CANTON_OPEN_SOURCE_TAG

def install_java_deps():
    maven_install(
        artifacts = version_specific.get(scala_major_version, []) + [
            "com.daml:bindings-java:{}".format(canton_version),
            "com.daml:community-base_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-api-type-signature_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-archive-encoder_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-archive_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-data_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-encoder_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-engine_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-ide-ledger_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-interpreter_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-language_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-parser_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-stable-packages_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-transaction_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-validation_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-tls_{}:{}".format(scala_major_version, canton_version),
            "com.daml:ledger-api-core_{}:{}".format(scala_major_version, canton_version),
            "com.daml:rs-grpc-bridge_{}:{}".format(scala_major_version, canton_version),
            "com.daml:rs-grpc-pekko_{}:{}".format(scala_major_version, canton_version),
            # TODO(https://github.com/DACH-NY/canton/issues/30144): check whether this dependency can be gotten rid of
            "com.daml:testing-utils_{}:{}".format(scala_major_version, canton_version),
            "com.daml:timer-utils_{}:{}".format(scala_major_version, canton_version),
            "com.daml:daml-lf-upgrade-check_{}:{}".format(scala_major_version, canton_version),

            # TODO(https://github.com/DACH-NY/canton/issues/30144): move these dependencies to shared_dependencies in
            #    the canton repo
            "com.oracle.database.jdbc:ojdbc8:19.18.0.0",
            "com.sparkjava:spark-core:2.9.4",
            "com.squareup:javapoet:1.13.0",
            "io.circe:circe-yaml_{}:{}".format(scala_major_version, "0.15.0-RC1"),
            "io.reactivex.rxjava2:rxjava:2.2.21",
            "org.junit.jupiter:junit-jupiter-engine:5.9.2",
            "org.junit.platform:junit-platform-runner:1.9.2",
            "org.slf4j:jul-to-slf4j:2.0.6",
            "org.scalactic:scalactic_{}:3.2.19".format(scala_major_version),
            "org.scalatest:scalatest_{}:3.2.19".format(scala_major_version),
            "org.scalatestplus:scalacheck-1-15_{}:3.2.11.0".format(scala_major_version),
            "org.tpolecat:doobie-postgres_{}:0.13.4".format(scala_major_version),
            "org.typelevel:kind-projector_{}:0.13.4".format(scala_version),
            "org.wartremover:wartremover_{}:3.5.8".format(scala_version),
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
        },
        repositories = [
            "https://repo1.maven.org/maven2",
            "https://europe-maven.pkg.dev/da-images/public-maven",
            "https://europe-maven.pkg.dev/da-images/public-maven-unstable",
        ] + (["m2local"] if LOCAL_CANTON_PATH != None else []),
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
