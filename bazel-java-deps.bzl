# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

netty_tcnative_version = "2.0.46.Final"
netty_version = "4.1.72.Final"
grpc_version = "1.44.0"
protobuf_version = "3.19.3"
akka_version = "2.6.18"
akka_http_version = "10.2.8"
gatling_version = "3.5.1"
guava_version = "31.0.1-jre"

# observability libs
# cannot update to 4.2.x because of https://github.com/dropwizard/metrics/issues/2920
dropwizard_version = "4.1.33"
opentelemetry_version = "1.12.0"
prometheus_version = "0.14.1"

def install_java_deps():
    maven_install(
        artifacts = version_specific.get(scala_major_version, []) + [
            "ch.qos.logback:logback-classic:1.2.8",
            "ch.qos.logback:logback-core:1.2.8",
            "com.auth0:java-jwt:3.10.3",
            "com.auth0:jwks-rsa:0.11.0",
            "com.chuusai:shapeless_{}:2.3.3".format(scala_major_version),
            "com.github.ben-manes.caffeine:caffeine:3.0.5",
            "com.github.pureconfig:pureconfig_{}:0.14.0".format(scala_major_version),
            "com.github.pureconfig:pureconfig-core_{}:0.14.0".format(scala_major_version),
            "com.github.pureconfig:pureconfig-generic_{}:0.14.0".format(scala_major_version),
            maven.artifact("com.github.pureconfig", "pureconfig-macros_2.12", "0.14.0", neverlink = True),
            "com.github.scopt:scopt_{}:4.0.0".format(scala_major_version),
            "com.google.code.findbugs:jsr305:3.0.2",
            "com.google.code.gson:gson:2.9.0",
            "com.google.guava:guava:{}".format(guava_version),
            "com.h2database:h2:2.1.210",
            "com.github.pathikrit:better-files_{}:3.8.0".format(scala_major_version),
            "com.github.tototoshi:scala-csv_{}:1.3.10".format(scala_major_version),
            "com.lihaoyi:sourcecode_{}:0.2.7".format(scala_major_version),
            "com.lihaoyi:pprint_{}:0.7.1".format(scala_major_version),
            "com.lihaoyi:sjsonnet_{}:0.3.0".format(scala_major_version),
            "commons-io:commons-io:2.5",
            "com.oracle.database.jdbc:ojdbc8:19.14.0.0",
            "com.sparkjava:spark-core:2.9.1",
            "com.oracle.database.jdbc.debug:ojdbc8_g:19.14.0.0",
            "com.squareup:javapoet:1.11.1",
            "com.storm-enroute:scalameter_{}:0.19".format(scala_major_version),
            "com.storm-enroute:scalameter-core_{}:0.19".format(scala_major_version),
            "com.typesafe.akka:akka-actor_{}:{}".format(scala_major_version, akka_version),
            "com.typesafe.akka:akka-actor-testkit-typed_{}:{}".format(scala_major_version, akka_version),
            "com.typesafe.akka:akka-actor-typed_{}:{}".format(scala_major_version, akka_version),
            "com.typesafe.akka:akka-http_{}:{}".format(scala_major_version, akka_http_version),
            "com.typesafe.akka:akka-http-spray-json_{}:{}".format(scala_major_version, akka_http_version),
            "com.typesafe.akka:akka-http-testkit_{}:{}".format(scala_major_version, akka_http_version),
            "com.typesafe.akka:akka-slf4j_{}:{}".format(scala_major_version, akka_version),
            "com.typesafe.akka:akka-stream_{}:{}".format(scala_major_version, akka_version),
            "com.typesafe.akka:akka-stream-testkit_{}:{}".format(scala_major_version, akka_version),
            "com.typesafe.akka:akka-testkit_{}:{}".format(scala_major_version, akka_version),
            "org.playframework.anorm:anorm_{}:2.6.8".format(scala_major_version),
            "org.playframework.anorm:anorm-akka_{}:2.6.8".format(scala_major_version),
            "com.typesafe.scala-logging:scala-logging_{}:3.9.2".format(scala_major_version),
            "com.zaxxer:HikariCP:3.2.0",
            "eu.rekawek.toxiproxy:toxiproxy-java:2.1.3",
            "io.circe:circe-core_{}:0.13.0".format(scala_major_version),
            "io.circe:circe-generic_{}:0.13.0".format(scala_major_version),
            "io.circe:circe-generic-extras_{}:0.13.0".format(scala_major_version),
            "io.circe:circe-parser_{}:0.13.0".format(scala_major_version),
            "io.circe:circe-yaml_{}:0.13.0".format(scala_major_version),
            "io.dropwizard.metrics:metrics-core:{}".format(dropwizard_version),
            maven.artifact("io.dropwizard.metrics", "metrics-graphite", dropwizard_version, exclusions = ["com.rabbitmq:amqp-client"]),
            "io.dropwizard.metrics:metrics-jmx:{}".format(dropwizard_version),
            "io.dropwizard.metrics:metrics-jvm:{}".format(dropwizard_version),
            "io.opentelemetry:opentelemetry-api:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-context:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-exporter-prometheus:{}-alpha".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-common:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-metrics:{}-alpha".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-metrics-testing:{}-alpha".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-testing:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-sdk-trace:{}".format(opentelemetry_version),
            "io.opentelemetry:opentelemetry-semconv:1.12.0-alpha",
            "io.prometheus:simpleclient:{}".format(prometheus_version),
            "io.prometheus:simpleclient_dropwizard:{}".format(prometheus_version),
            "io.prometheus:simpleclient_httpserver:{}".format(prometheus_version),
            "io.prometheus:simpleclient_servlet:{}".format(prometheus_version),
            "io.grpc:grpc-api:{}".format(grpc_version),
            "io.grpc:grpc-core:{}".format(grpc_version),
            "io.grpc:grpc-netty:{}".format(grpc_version),
            "io.grpc:grpc-protobuf:{}".format(grpc_version),
            "io.grpc:grpc-services:{}".format(grpc_version),
            "io.grpc:grpc-stub:{}".format(grpc_version),
            # netty
            "io.netty:netty-buffer:{}".format(netty_version),
            "io.netty:netty-codec-http2:{}".format(netty_version),
            "io.netty:netty-handler:{}".format(netty_version),
            "io.netty:netty-handler-proxy:{}".format(netty_version),
            "io.netty:netty-resolver:{}".format(netty_version),
            "io.netty:netty-tcnative-boringssl-static:{}".format(netty_tcnative_version),
            # protobuf
            "com.google.protobuf:protobuf-java:{}".format(protobuf_version),
            # scalapb
            "com.thesamet.scalapb:compilerplugin_{}:{}".format(scala_major_version, scalapb_version),
            "com.thesamet.scalapb:lenses_{}:{}".format(scala_major_version, scalapb_version),
            "com.thesamet.scalapb:protoc-bridge_{}:{}".format(scala_major_version, scalapb_protoc_version),
            "com.thesamet.scalapb:protoc-gen_{}:{}".format(scala_major_version, scalapb_protoc_version),
            "com.thesamet.scalapb:scalapb-runtime_{}:{}".format(scala_major_version, scalapb_version),
            "com.thesamet.scalapb:scalapb-runtime-grpc_{}:{}".format(scala_major_version, scalapb_version),
            "com.thesamet.scalapb:scalapb-json4s_{}:0.11.1".format(scala_major_version, scalapb_version),
            # ---- end of grpc-protobuf-netty block
            "io.gatling:gatling-app:{}".format(gatling_version),
            "io.gatling:gatling-core:{}".format(gatling_version),
            "io.gatling:gatling-commons:{}".format(gatling_version),
            "io.gatling:gatling-recorder:{}".format(gatling_version),
            "io.gatling:gatling-charts:{}".format(gatling_version),
            "io.gatling.highcharts:gatling-charts-highcharts:{}".format(gatling_version),
            "io.gatling:gatling-http:{}".format(gatling_version),
            "io.gatling:gatling-http-client:{}".format(gatling_version),
            "io.reactivex.rxjava2:rxjava:2.2.1",
            "io.spray:spray-json_{}:1.3.5".format(scala_major_version),
            "io.github.paoloboni:spray-json-derived-codecs_{}:2.3.4".format(scala_major_version),
            "javax.annotation:javax.annotation-api:1.2",
            "javax.ws.rs:javax.ws.rs-api:2.1",
            "junit:junit:4.12",
            "junit:junit-dep:4.10",
            "net.logstash.logback:logstash-logback-encoder:6.6",
            "org.bouncycastle:bcpkix-jdk15on:1.70",
            "org.bouncycastle:bcprov-jdk15on:1.70",
            "org.codehaus.janino:janino:3.1.4",
            "org.apache.commons:commons-lang3:3.9",
            "org.apache.commons:commons-text:1.4",
            "org.awaitility:awaitility:3.1.6",
            "org.checkerframework:checker:2.5.4",
            "org.flywaydb:flyway-core:8.4.1",
            "org.freemarker:freemarker-gae:2.3.28",
            "org.jline:jline:3.7.1",
            "org.jline:jline-reader:3.7.1",
            "org.junit.jupiter:junit-jupiter-api:5.0.0",
            "org.junit.jupiter:junit-jupiter-engine:5.0.0",
            "org.junit.platform:junit-platform-engine:1.0.0",
            "org.junit.platform:junit-platform-runner:1.0.0",
            "org.mockito:mockito-core:3.6.28",
            "org.mockito:mockito-inline:3.6.28",
            "org.mockito:mockito-scala_{}:1.16.3".format(scala_major_version),
            "org.pcollections:pcollections:2.1.3",
            "org.postgresql:postgresql:42.2.25",
            "org.reactivestreams:reactive-streams:1.0.2",
            "org.reactivestreams:reactive-streams-tck:1.0.2",
            "org.reflections:reflections:0.9.12",
            "org.sangria-graphql:sangria_{}:2.0.1".format(scala_major_version),
            "org.sangria-graphql:sangria-spray-json_{}:1.0.2".format(scala_major_version),
            "org.scalacheck:scalacheck_{}:1.15.4".format(scala_major_version),
            "org.scala-lang.modules:scala-parallel-collections_{}:1.0.0".format(scala_major_version),
            "org.scalameta:munit_{}:0.7.26".format(scala_major_version),
            "org.scalactic:scalactic_{}:3.2.9".format(scala_major_version),
            "org.scalatest:scalatest_{}:3.2.9".format(scala_major_version),
            "org.scalatestplus:scalacheck-1-15_{}:3.2.9.0".format(scala_major_version),
            "org.scalatestplus:selenium-3-141_{}:3.2.9.0".format(scala_major_version),
            "org.scalatestplus:testng-6-7_{}:3.2.9.0".format(scala_major_version),
            "org.scalaz:scalaz-core_{}:7.2.33".format(scala_major_version),
            "org.scalaz:scalaz-scalacheck-binding_{}:7.2.33-scalacheck-1.15".format(scala_major_version),
            "org.seleniumhq.selenium:selenium-java:3.12.0",
            "org.slf4j:slf4j-api:1.7.26",
            "org.slf4j:slf4j-simple:1.7.26",
            "org.typelevel:cats-core_{}:2.6.1".format(scala_major_version),
            "org.typelevel:cats-kernel_{}:2.6.1".format(scala_major_version),
            "org.typelevel:cats-laws_{}:2.6.1".format(scala_major_version),
            "org.typelevel:kind-projector_{}:0.13.2".format(scala_version),
            "org.tpolecat:doobie-core_{}:0.13.4".format(scala_major_version),
            "org.tpolecat:doobie-hikari_{}:0.13.4".format(scala_major_version),
            "org.tpolecat:doobie-postgres_{}:0.13.4".format(scala_major_version),
            "org.typelevel:paiges-core_{}:0.3.2".format(scala_major_version),
            "org.wartremover:wartremover_{}:2.4.16".format(scala_version),
            "org.xerial:sqlite-jdbc:3.36.0.1",
            "com.fasterxml.jackson.core:jackson-core:2.12.0",
            "com.fasterxml.jackson.core:jackson-databind:2.12.0",
            "org.scala-lang:scala-library:{}".format(scala_version),
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
