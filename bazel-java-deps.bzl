# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# When adding, removing or changing a dependency in this file, update the pinned dependencies by executing
# $ bazel run @unpinned_maven//:pin
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
# 2. To keep TLS for the Ledger API Server working, the following three artifacts need be updated
# in sync according to https://github.com/grpc/grpc-java/blob/master/SECURITY.md#netty
#
# * io.grpc:grpc-netty
# * io.netty:netty-handler
# * io.netty:netty-tcnative-boringssl-static
#
# This effectively means all io.grpc:*, io.netty:*, and `com.google.protobuf:protobuf-java
# need to be updated with careful consideration.

netty_tcnative_version = "2.0.61.Final"
netty_version = "4.1.100.Final"
grpc_version = "1.60.0"
protobuf_version = "3.24.0"
pekko_version = "1.0.1"
pekko_http_version = "1.0.0"

#gatling_version = "3.5.1"
guava_version = "31.1-jre"

# observability libs
# cannot update to 4.2.x because of https://github.com/dropwizard/metrics/issues/2920
dropwizard_version = "4.1.33"
opentelemetry_version = "1.12.0"
prometheus_version = "0.14.1"

# group libraries controlled by the same org
circe_version = "0.14.2"

def install_java_deps():
    maven_install(
        artifacts = version_specific.get(scala_major_version, []) + [
            "ch.qos.logback:logback-classic:1.4.14",
            "ch.qos.logback:logback-core:1.4.14",
            "com.auth0:java-jwt:4.2.1",
            "com.auth0:jwks-rsa:0.21.2",
            "com.chuusai:shapeless_{}:2.3.6".format(scala_major_version),
            "com.fasterxml.jackson.core:jackson-core:2.14.3",
            "com.fasterxml.jackson.core:jackson-databind:2.14.3",
            "com.github.ben-manes.caffeine:caffeine:3.1.2",
            "com.github.blemale:scaffeine_{}:5.2.1".format(scala_major_version),
            "com.github.pathikrit:better-files_{}:3.9.1".format(scala_major_version),
            "com.github.pureconfig:pureconfig-cats_{}:0.14.0".format(scala_major_version),
            "com.github.pureconfig:pureconfig-core_{}:0.14.0".format(scala_major_version),
            "com.github.pureconfig:pureconfig-generic_{}:0.14.0".format(scala_major_version),
            "com.github.pureconfig:pureconfig_{}:0.14.0".format(scala_major_version),
            "com.github.scopt:scopt_{}:4.1.0".format(scala_major_version),
            "com.github.tototoshi:scala-csv_{}:1.3.10".format(scala_major_version),
            "com.google.code.findbugs:jsr305:3.0.2",
            "com.google.code.gson:gson:2.10",
            "com.google.crypto.tink:tink:1.3.0",
            "com.google.guava:guava:{}".format(guava_version),
            "com.google.protobuf:protobuf-java-util:{}".format(protobuf_version),
            "com.google.protobuf:protobuf-java:{}".format(protobuf_version),
            "com.h2database:h2:2.1.210",
            "com.lihaoyi:ammonite-compiler_{}:2.5.9".format(scala_version),
            "com.lihaoyi:ammonite-compiler-interface_{}:2.5.9".format(scala_version),
            "com.lihaoyi:ammonite-interp-api_{}:2.5.9".format(scala_version),
            "com.lihaoyi:ammonite-interp_{}:2.5.9".format(scala_version),
            "com.lihaoyi:ammonite-repl_{}:2.5.9".format(scala_version),
            "com.lihaoyi:ammonite-runtime_{}:2.5.9".format(scala_version),
            "com.lihaoyi:ammonite-util_{}:2.5.9".format(scala_major_version),
            "com.lihaoyi:ammonite_{}:2.5.9".format(scala_version),
            "com.lihaoyi:fansi_{}:0.4.0".format(scala_major_version),
            "com.lihaoyi:os-lib_{}:0.8.0".format(scala_major_version),
            "com.lihaoyi:pprint_{}:0.8.1".format(scala_major_version),
            "com.lihaoyi:sourcecode_{}:0.3.0".format(scala_major_version),
            "com.oracle.database.jdbc.debug:ojdbc8_g:19.18.0.0",
            "com.oracle.database.jdbc:ojdbc8:19.18.0.0",
            "com.sparkjava:spark-core:2.9.4",
            "com.squareup:javapoet:1.13.0",
            "com.storm-enroute:scalameter-core_{}:0.21".format(scala_major_version),
            "com.storm-enroute:scalameter_{}:0.21".format(scala_major_version),
            "com.thesamet.scalapb:compilerplugin_{}:{}".format(scala_major_version, scalapb_version),
            "com.thesamet.scalapb:lenses_{}:{}".format(scala_major_version, scalapb_version),
            "com.thesamet.scalapb:protoc-bridge_{}:{}".format(scala_major_version, scalapb_protoc_version),
            "com.thesamet.scalapb:protoc-gen_{}:{}".format(scala_major_version, scalapb_protoc_version),
            "com.thesamet.scalapb:scalapb-json4s_{}:0.11.1".format(scala_major_version, scalapb_version),
            "com.thesamet.scalapb:scalapb-runtime-grpc_{}:{}".format(scala_major_version, scalapb_version),
            "com.thesamet.scalapb:scalapb-runtime_{}:{}".format(scala_major_version, scalapb_version),
            "org.apache.pekko:pekko-actor-testkit-typed_{}:{}".format(scala_major_version, pekko_version),
            "org.apache.pekko:pekko-actor-typed_{}:{}".format(scala_major_version, pekko_version),
            "org.apache.pekko:pekko-actor_{}:{}".format(scala_major_version, pekko_version),
            "org.apache.pekko:pekko-http-spray-json_{}:{}".format(scala_major_version, pekko_http_version),
            "org.apache.pekko:pekko-http-testkit_{}:{}".format(scala_major_version, pekko_http_version),
            "org.apache.pekko:pekko-http_{}:{}".format(scala_major_version, pekko_http_version),
            "org.apache.pekko:pekko-slf4j_{}:{}".format(scala_major_version, pekko_version),
            "org.apache.pekko:pekko-stream-testkit_{}:{}".format(scala_major_version, pekko_version),
            "org.apache.pekko:pekko-stream_{}:{}".format(scala_major_version, pekko_version),
            "org.apache.pekko:pekko-testkit_{}:{}".format(scala_major_version, pekko_version),
            "com.typesafe.scala-logging:scala-logging_{}:3.9.5".format(scala_major_version),
            "com.typesafe.slick:slick-hikaricp_{}:3.3.3".format(scala_major_version),
            "com.typesafe.slick:slick_{}:3.3.3".format(scala_major_version),
            "com.zaxxer:HikariCP:3.2.0",
            "commons-codec:commons-codec:1.11",
            "commons-io:commons-io:2.11.0",
            "dev.optics:monocle-core_{}:3.2.0".format(scala_major_version),
            "dev.optics:monocle-macro_{}:3.2.0".format(scala_major_version),
            "eu.rekawek.toxiproxy:toxiproxy-java:2.1.7",
            "io.chrisdavenport:cats-scalacheck_{}:0.3.2".format(scala_major_version),
            "io.circe:circe-core_{}:{}".format(scala_major_version, circe_version),
            "io.circe:circe-generic-extras_{}:{}".format(scala_major_version, circe_version),
            "io.circe:circe-generic_{}:{}".format(scala_major_version, circe_version),
            "io.circe:circe-parser_{}:{}".format(scala_major_version, circe_version),
            "io.circe:circe-yaml_{}:{}".format(scala_major_version, "0.15.0-RC1"),
            #            "io.gatling.highcharts:gatling-charts-highcharts:{}".format(gatling_version),
            #            "io.gatling:gatling-app:{}".format(gatling_version),
            #            "io.gatling:gatling-charts:{}".format(gatling_version),
            #            "io.gatling:gatling-commons:{}".format(gatling_version),
            #            "io.gatling:gatling-core:{}".format(gatling_version),
            #            "io.gatling:gatling-http-client:{}".format(gatling_version),
            #            "io.gatling:gatling-http:{}".format(gatling_version),
            #            "io.gatling:gatling-recorder:{}".format(gatling_version),
            "io.get-coursier:interface:0.0.21",
            "io.github.paoloboni:spray-json-derived-codecs_{}:2.3.10".format(scala_major_version),
            "io.grpc:grpc-api:{}".format(grpc_version),
            # grpc-core has a *runtime* dependency on grpc-util, and grpc-util has a dependency on
            # grpc-core. Because maven_install doesn't differentiate between runtime and
            # compile-time deps (https://github.com/bazelbuild/rules_jvm_external/issues/966), we
            # need to manually exclude grpc-util from the dependencies of grpc-core.
            maven.artifact(
                artifact = "grpc-core",
                exclusions = [
                    "io.grpc:grpc-util",
                ],
                group = "io.grpc",
                version = grpc_version,
            ),
            "io.grpc:grpc-inprocess:{}".format(grpc_version),
            "io.grpc:grpc-netty:{}".format(grpc_version),
            "io.grpc:grpc-protobuf:{}".format(grpc_version),
            "io.grpc:grpc-services:{}".format(grpc_version),
            "io.grpc:grpc-stub:{}".format(grpc_version),
            "io.grpc:grpc-util:{}".format(grpc_version),
            "io.netty:netty-buffer:{}".format(netty_version),
            "io.netty:netty-codec-http2:{}".format(netty_version),
            "io.netty:netty-handler-proxy:{}".format(netty_version),
            "io.netty:netty-handler:{}".format(netty_version),
            "io.netty:netty-resolver:{}".format(netty_version),
            "io.netty:netty-tcnative-boringssl-static:{}".format(netty_tcnative_version),
            "io.opentelemetry.instrumentation:opentelemetry-grpc-1.6:{}-alpha".format(opentelemetry_version),
            "io.opentelemetry.instrumentation:opentelemetry-instrumentation-api:{}-alpha".format(opentelemetry_version),
            "io.opentelemetry.instrumentation:opentelemetry-runtime-metrics:{}-alpha".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-api:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-context:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-exporter-jaeger:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-exporter-otlp-common:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-exporter-otlp-trace:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-exporter-prometheus:{}-alpha".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-exporter-zipkin:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-common:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-extension-autoconfigure-spi:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-extension-autoconfigure:{}-alpha".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-logs:{}-alpha".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-metrics-testing:{}-alpha".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-metrics:{}-alpha".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-testing:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-trace:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-semconv:{}-alpha".format(opentelemetry_version),
            "io.prometheus:simpleclient:{}".format(prometheus_version),
            "io.prometheus:simpleclient_httpserver:{}".format(prometheus_version),
            "io.prometheus:simpleclient_servlet:{}".format(prometheus_version),
            "io.reactivex.rxjava2:rxjava:2.2.21",
            "io.scalaland:chimney_{}:0.6.1".format(scala_major_version),
            "io.spray:spray-json_{}:1.3.6".format(scala_major_version),
            "javax.annotation:javax.annotation-api:1.3.2",
            "javax.ws.rs:javax.ws.rs-api:2.1",
            "net.logstash.logback:logstash-logback-encoder:6.6",
            "org.apache.commons:commons-lang3:3.12.0",
            "org.apache.commons:commons-text:1.10.0",
            "org.apache.logging.log4j:log4j-core:2.17.0",
            "org.awaitility:awaitility:4.2.0",
            "org.bouncycastle:bcpkix-jdk15on:1.70",
            "org.bouncycastle:bcprov-jdk15on:1.70",
            "org.checkerframework:checker:3.28.0",
            "org.codehaus.janino:janino:3.1.4",
            "org.flywaydb:flyway-core:8.4.1",
            "org.freemarker:freemarker-gae:2.3.32",
            "org.jline:jline-reader:3.22.0",
            "org.jline:jline:3.22.0",
            "org.junit.jupiter:junit-jupiter-api:5.9.2",
            "org.junit.jupiter:junit-jupiter-engine:5.9.2",
            "org.junit.platform:junit-platform-engine:1.9.2",
            "org.junit.platform:junit-platform-runner:1.9.2",
            "org.mockito:mockito-core:3.6.28",
            "org.mockito:mockito-inline:3.6.28",
            "org.mockito:mockito-scala_{}:1.16.3".format(scala_major_version),
            "org.pcollections:pcollections:4.0.1",
            "org.playframework.anorm:anorm-tokenizer_{}:2.7.0".format(scala_major_version),
            "org.playframework.anorm:anorm_{}:2.7.0".format(scala_major_version),
            "org.postgresql:postgresql:42.6.0",
            "org.reactivestreams:reactive-streams-tck:1.0.4",
            "org.reactivestreams:reactive-streams:1.0.4",
            "org.reflections:reflections:0.9.12",
            "org.sangria-graphql:sangria-spray-json_{}:1.0.2".format(scala_major_version),
            "org.sangria-graphql:sangria_{}:4.0.2".format(scala_major_version),
            "org.scala-lang.modules:scala-collection-contrib_{}:0.2.2".format(scala_major_version),
            "org.scala-lang.modules:scala-parallel-collections_{}:1.0.4".format(scala_major_version),
            "org.scala-lang:scala-library:{}".format(scala_version),
            "org.scalacheck:scalacheck_{}:1.15.4".format(scala_major_version),
            "org.scalactic:scalactic_{}:3.2.11".format(scala_major_version),
            "org.scalameta:munit_{}:0.7.26".format(scala_major_version),
            "org.scalatest:scalatest_{}:3.2.11".format(scala_major_version),
            "org.scalatestplus:scalacheck-1-15_{}:3.2.11.0".format(scala_major_version),
            "org.scalatestplus:testng-7-5_{}:3.2.11.0".format(scala_major_version),
            "org.scalaz:scalaz-core_{}:7.2.33".format(scala_major_version),
            "org.scalaz:scalaz-scalacheck-binding_{}:7.2.33-scalacheck-1.15".format(scala_major_version),
            "org.slf4j:jul-to-slf4j:2.0.6",
            "org.slf4j:jul-to-slf4j:2.0.6",
            "org.slf4j:slf4j-api:2.0.6",
            "org.slf4j:slf4j-simple:2.0.6",
            "org.testcontainers:jdbc:1.15.1",
            "org.testcontainers:postgresql:1.15.1",
            "org.testcontainers:testcontainers:1.15.1",
            "org.tpolecat:doobie-core_{}:0.13.4".format(scala_major_version),
            "org.tpolecat:doobie-hikari_{}:0.13.4".format(scala_major_version),
            "org.tpolecat:doobie-postgres_{}:0.13.4".format(scala_major_version),
            "org.typelevel:cats-core_{}:2.9.0".format(scala_major_version),
            "org.typelevel:cats-free_{}:2.9.0".format(scala_major_version),
            "org.typelevel:cats-kernel_{}:2.9.0".format(scala_major_version),
            "org.typelevel:cats-laws_{}:2.9.0".format(scala_major_version),
            "org.typelevel:kind-projector_{}:0.13.2".format(scala_version),
            "org.typelevel:paiges-core_{}:0.4.2".format(scala_major_version),
            "org.wartremover:wartremover_{}:2.4.21".format(scala_version),
            "org.xerial:sqlite-jdbc:3.36.0.1",
            maven.artifact("com.github.pureconfig", "pureconfig-macros_2.12", "0.14.0", neverlink = True),
        ],
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
            "org.scalactic:scalactic_2.12": "@io_bazel_rules_scala_scalactic//:io_bazel_rules_scala_scalactic",
            "org.scalatest:scalatest_2.12": "@io_bazel_rules_scala_scalatest//:io_bazel_rules_scala_scalatest",
        },
        repositories = [
            "https://repo1.maven.org/maven2",
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
    )
    maven_install(
        name = "canton_maven",
        maven_install_json = "@//:canton_maven_install.json",
        artifacts = [
            "org.flywaydb:flyway-core:9.15.2",
        ],
        repositories = [
            "https://repo1.maven.org/maven2",
        ],
        fetch_sources = True,
        version_conflict_policy = "pinned",
    )

    # Do not use those dependencies in anything new !
    maven_install(
        name = "deprecated_maven",
        maven_install_json = "@//:deprecated_maven_install.json",
        artifacts = [
            "io.gatling.highcharts:gatling-charts-highcharts:3.5.1",
            "io.gatling:gatling-app:3.5.1",
        ],
        repositories = [
            "https://repo1.maven.org/maven2",
        ],
        fetch_sources = True,
        version_conflict_policy = "pinned",
    )
