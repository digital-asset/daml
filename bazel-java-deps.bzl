# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# When adding, removing or changing a dependency in this file, update the pinned dependencies by executing
# $ bazel run @unpinned_maven//:pin
# See https://github.com/bazelbuild/rules_jvm_external#updating-maven_installjson

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")
load("@scala_version//:index.bzl", "scala_major_version", "scala_version")

version_specific = {
    "2.12": [
        "ai.x:diff_2.12:2.0.1",
        "org.gnieh:diffson-spray-json_2.12:3.1.1",
    ],
}

def install_java_deps():
    maven_install(
        artifacts = version_specific.get(scala_major_version, []) + [
            "ch.qos.logback:logback-classic:1.2.3",
            "ch.qos.logback:logback-core:1.2.3",
            "com.auth0:java-jwt:3.10.3",
            "com.auth0:jwks-rsa:0.11.0",
            "com.chuusai:shapeless_{}:2.3.3".format(scala_major_version),
            "com.github.ben-manes.caffeine:caffeine:2.8.0",
            "com.github.ghik:silencer-lib_{}:1.7.1".format(scala_version),
            "com.github.ghik:silencer-plugin_{}:1.7.1".format(scala_version),
            "com.github.pureconfig:pureconfig_2.12:0.14.0",
            "com.github.pureconfig:pureconfig-core_2.12:0.14.0",
            "com.github.pureconfig:pureconfig-generic_2.12:0.14.0",
            maven.artifact("com.github.pureconfig", "pureconfig-macros_2.12", "0.14.0", neverlink = True),
            "com.github.scopt:scopt_{}:3.7.1".format(scala_major_version),
            "com.google.code.findbugs:jsr305:3.0.2",
            "com.google.code.gson:gson:2.8.2",
            "com.google.guava:guava:29.0-jre",
            "com.h2database:h2:1.4.200",
            "com.lihaoyi:pprint_{}:0.6.0".format(scala_major_version),
            "commons-io:commons-io:2.5",
            "com.sparkjava:spark-core:2.9.1",
            "com.squareup:javapoet:1.11.1",
            "com.storm-enroute:scalameter_{}:0.19".format(scala_major_version),
            "com.storm-enroute:scalameter-core_{}:0.19".format(scala_major_version),
            "com.typesafe.akka:akka-actor_{}:2.6.10".format(scala_major_version),
            "com.typesafe.akka:akka-actor-typed_{}:2.6.10".format(scala_major_version),
            "com.typesafe.akka:akka-http_{}:10.2.1".format(scala_major_version),
            "com.typesafe.akka:akka-http-spray-json_{}:10.2.1".format(scala_major_version),
            "com.typesafe.akka:akka-http-testkit_{}:10.2.1".format(scala_major_version),
            "com.typesafe.akka:akka-slf4j_{}:2.6.10".format(scala_major_version),
            "com.typesafe.akka:akka-stream_{}:2.6.10".format(scala_major_version),
            "com.typesafe.akka:akka-stream-testkit_{}:2.6.10".format(scala_major_version),
            "com.typesafe.akka:akka-testkit_{}:2.6.10".format(scala_major_version),
            "org.playframework.anorm:anorm_{}:2.6.8".format(scala_major_version),
            "org.playframework.anorm:anorm-akka_{}:2.6.8".format(scala_major_version),
            "com.typesafe.scala-logging:scala-logging_{}:3.9.2".format(scala_major_version),
            "com.zaxxer:HikariCP:3.2.0",
            "eu.rekawek.toxiproxy:toxiproxy-java:2.1.3",
            "io.circe:circe-core_{}:0.13.0".format(scala_major_version),
            "io.circe:circe-generic_{}:0.13.0".format(scala_major_version),
            "io.circe:circe-parser_{}:0.13.0".format(scala_major_version),
            "io.circe:circe-yaml_{}:0.13.0".format(scala_major_version),
            "io.dropwizard.metrics:metrics-core:4.1.2",
            "io.dropwizard.metrics:metrics-graphite:4.1.2",
            "io.dropwizard.metrics:metrics-jmx:4.1.2",
            "io.dropwizard.metrics:metrics-jvm:4.1.2",
            "io.opentelemetry:opentelemetry-api:0.8.0",

            # Bumping versions of io.grpc:* has a few implications:
            # 1. io.grpc:grpc-protobuf has a dependency on com.google.protobuf:protobuf-java, which in
            #    turn needs to be aligned with the version of protoc we are using (as declared in deps.bzl).
            #    ScalaPB also depends on a version of protobuf-java, but for the most part we expect here a
            #    version mismatch between ScalaPBs declared protobuf-java dependency and the version on the
            #    classpath doesn't matter.
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
            # grpc
            "io.grpc:grpc-api:1.29.0",
            "io.grpc:grpc-core:1.29.0",
            "io.grpc:grpc-netty:1.29.0",
            "io.grpc:grpc-protobuf:1.29.0",
            "io.grpc:grpc-services:1.29.0",
            "io.grpc:grpc-stub:1.29.0",
            # netty
            "io.netty:netty-codec-http2:4.1.48.Final",
            "io.netty:netty-handler:4.1.48.Final",
            "io.netty:netty-handler-proxy:4.1.48.Final",
            "io.netty:netty-resolver:4.1.48.Final",
            "io.netty:netty-tcnative-boringssl-static:2.0.30.Final",
            # protobuf
            "com.google.protobuf:protobuf-java:3.11.0",
            #scalapb
            "com.thesamet.scalapb:compilerplugin_{}:0.9.0".format(scala_major_version),
            "com.thesamet.scalapb:lenses_{}:0.9.0".format(scala_major_version),
            "com.thesamet.scalapb:protoc-bridge_{}:0.7.8".format(scala_major_version),
            "com.thesamet.scalapb:scalapb-runtime_{}:0.9.0".format(scala_major_version),
            "com.thesamet.scalapb:scalapb-runtime-grpc_{}:0.9.0".format(scala_major_version),
            # ---- end of grpc-protobuf-netty block
            "io.reactivex.rxjava2:rxjava:2.2.1",
            "io.spray:spray-json_{}:1.3.5".format(scala_major_version),
            "io.zipkin.brave:brave:4.6.0",
            "io.zipkin.reporter:zipkin-sender-okhttp3:1.0.4",
            "javax.annotation:javax.annotation-api:1.2",
            "javax.ws.rs:javax.ws.rs-api:2.1",
            "junit:junit:4.12",
            "junit:junit-dep:4.10",
            "net.logstash.logback:logstash-logback-encoder:6.3",
            "org.apache.commons:commons-lang3:3.9",
            "org.apache.commons:commons-text:1.4",
            "org.awaitility:awaitility:3.1.6",
            "org.checkerframework:checker:2.5.4",
            "org.flywaydb:flyway-core:6.5.0",
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
            "org.postgresql:postgresql:42.2.18",
            "org.reactivestreams:reactive-streams:1.0.2",
            "org.reactivestreams:reactive-streams-tck:1.0.2",
            "org.sangria-graphql:sangria_{}:2.0.1".format(scala_major_version),
            "org.sangria-graphql:sangria-spray-json_{}:1.0.2".format(scala_major_version),
            "org.scalacheck:scalacheck_{}:1.14.0".format(scala_major_version),
            "org.scala-lang.modules:scala-collection-compat_{}:2.1.6".format(scala_major_version),
            "org.scala-lang.modules:scala-java8-compat_{}:0.9.0".format(scala_major_version),
            "org.scala-sbt:sbt:1.1.4",
            "org.scalactic:scalactic_{}:3.1.2".format(scala_major_version),
            "org.scalatest:scalatest_{}:3.1.2".format(scala_major_version),
            "org.scalatestplus:scalacheck-1-14_{}:3.1.4.0".format(scala_major_version),
            "org.scalatestplus:selenium-3-141_{}:3.1.3.0".format(scala_major_version),
            "org.scalatestplus:testng-6-7_{}:3.1.4.0".format(scala_major_version),
            "org.scalaz:scalaz-core_{}:7.2.30".format(scala_major_version),
            "org.scalaz:scalaz-scalacheck-binding_{}:7.2.30-scalacheck-1.14".format(scala_major_version),
            "org.seleniumhq.selenium:selenium-java:3.12.0",
            "org.slf4j:slf4j-api:1.7.26",
            "org.slf4j:slf4j-simple:1.7.26",
            "org.typelevel:kind-projector_{}:0.11.2".format(scala_version),
            "org.tpolecat:doobie-core_{}:0.9.2".format(scala_major_version),
            "org.tpolecat:doobie-postgres_{}:0.9.2".format(scala_major_version),
            "org.typelevel:paiges-core_{}:0.3.2".format(scala_major_version),
            "org.wartremover:wartremover_{}:2.4.10".format(scala_version),
            "org.xerial:sqlite-jdbc:3.30.1",
            # gatling dependencies
            "io.gatling:gatling-app:3.3.1",
            "io.gatling:gatling-core:3.3.1",
            "io.gatling:gatling-commons:3.3.1",
            "io.gatling:gatling-recorder:3.3.1",
            "io.gatling:gatling-charts:3.3.1",
            "io.gatling.highcharts:gatling-highcharts:3.3.1",
            "io.gatling:gatling-http:3.3.1",
            "io.gatling:gatling-http-client:3.3.1",
            "com.fasterxml.jackson.core:jackson-core:2.12.0",
            "com.fasterxml.jackson.core:jackson-databind:2.12.0",
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
