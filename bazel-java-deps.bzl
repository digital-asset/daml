# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# When adding, removing or changing a dependency in this file, update the pinned dependencies by executing
# $ bazel run @unpinned_maven//:pin
# See https://github.com/bazelbuild/rules_jvm_external#updating-maven_installjson

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

def install_java_deps():
    maven_install(
        artifacts = [
            "ai.x:diff_2.12:2.0.1",
            "ch.qos.logback:logback-classic:1.2.3",
            "ch.qos.logback:logback-core:1.2.3",
            "com.auth0:java-jwt:3.10.3",
            "com.auth0:jwks-rsa:0.11.0",
            "com.chuusai:shapeless_2.12:2.3.2",
            "com.github.ben-manes.caffeine:caffeine:2.8.0",
            "com.github.ghik:silencer-lib_2.12.12:1.7.1",
            "com.github.ghik:silencer-plugin_2.12.12:1.7.1",
            "com.github.pureconfig:pureconfig_2.12:0.8.0",
            maven.artifact("com.github.pureconfig", "pureconfig-macros_2.12", "0.8.0", neverlink = True),
            "com.github.scopt:scopt_2.12:3.7.1",
            "com.google.code.findbugs:jsr305:3.0.2",
            "com.google.code.gson:gson:2.8.2",
            "com.google.guava:guava:29.0-jre",
            "com.h2database:h2:1.4.200",
            "com.lihaoyi:pprint_2.12:0.5.3",
            "commons-io:commons-io:2.5",
            "com.sparkjava:spark-core:2.9.1",
            "com.squareup:javapoet:1.11.1",
            "com.storm-enroute:scalameter_2.12:0.10.1",
            "com.storm-enroute:scalameter-core_2.12:0.10.1",
            "com.typesafe.akka:akka-actor_2.12:2.6.10",
            "com.typesafe.akka:akka-actor-typed_2.12:2.6.10",
            "com.typesafe.akka:akka-http_2.12:10.2.1",
            "com.typesafe.akka:akka-http-spray-json_2.12:10.2.1",
            "com.typesafe.akka:akka-http-testkit_2.12:10.2.1",
            "com.typesafe.akka:akka-slf4j_2.12:2.6.10",
            "com.typesafe.akka:akka-stream_2.12:2.6.10",
            "com.typesafe.akka:akka-stream-testkit_2.12:2.6.10",
            "com.typesafe.akka:akka-testkit_2.12:2.6.10",
            "com.typesafe.play:anorm_2.12:2.5.3",
            "com.typesafe.play:anorm-akka_2.12:2.5.3",
            "com.typesafe.scala-logging:scala-logging_2.12:3.9.2",
            "com.zaxxer:HikariCP:3.2.0",
            "eu.rekawek.toxiproxy:toxiproxy-java:2.1.3",
            "io.circe:circe-core_2.12:0.10.0",
            "io.circe:circe-generic_2.12:0.10.0",
            "io.circe:circe-parser_2.12:0.10.0",
            "io.circe:circe-yaml_2.12:0.10.0",
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
            "com.thesamet.scalapb:compilerplugin_2.12:0.9.0",
            "com.thesamet.scalapb:lenses_2.12:0.9.0",
            "com.thesamet.scalapb:protoc-bridge_2.12:0.7.8",
            "com.thesamet.scalapb:scalapb-runtime_2.12:0.9.0",
            "com.thesamet.scalapb:scalapb-runtime-grpc_2.12:0.9.0",
            # ---- end of grpc-protobuf-netty block
            "io.reactivex.rxjava2:rxjava:2.2.1",
            "io.spray:spray-json_2.12:1.3.5",
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
            "org.gnieh:diffson-spray-json_2.12:3.1.1",
            "org.jline:jline:3.7.1",
            "org.jline:jline-reader:3.7.1",
            "org.junit.jupiter:junit-jupiter-api:5.0.0",
            "org.junit.jupiter:junit-jupiter-engine:5.0.0",
            "org.junit.platform:junit-platform-engine:1.0.0",
            "org.junit.platform:junit-platform-runner:1.0.0",
            "org.mockito:mockito-core:3.6.28",
            "org.mockito:mockito-inline:3.6.28",
            "org.mockito:mockito-scala_2.12:1.16.3",
            "org.pcollections:pcollections:2.1.3",
            "org.postgresql:postgresql:42.2.18",
            "org.reactivestreams:reactive-streams:1.0.2",
            "org.reactivestreams:reactive-streams-tck:1.0.2",
            "org.sangria-graphql:sangria_2.12:1.4.2",
            "org.sangria-graphql:sangria-spray-json_2.12:1.0.2",
            "org.scalacheck:scalacheck_2.12:1.14.0",
            "org.scala-lang.modules:scala-collection-compat_2.12:2.1.6",
            "org.scala-lang.modules:scala-java8-compat_2.12:0.9.0",
            "org.scala-lang.modules:scala-parser-combinators_2.12:1.0.4",
            "org.scala-sbt:sbt:1.1.4",
            "org.scalactic:scalactic_2.12:3.1.2",
            "org.scalatest:scalatest_2.12:3.1.2",
            "org.scalatestplus:scalacheck-1-14_2.12:3.1.4.0",
            "org.scalatestplus:selenium-3-141_2.12:3.1.3.0",
            "org.scalatestplus:testng-6-7_2.12:3.1.4.0",
            "org.scalaz:scalaz-concurrent_2.12:7.2.24",
            "org.scalaz:scalaz-core_2.12:7.2.24",
            "org.scalaz:scalaz-scalacheck-binding_2.12:7.2.24-scalacheck-1.14",
            "org.seleniumhq.selenium:selenium-java:3.12.0",
            "org.slf4j:slf4j-api:1.7.26",
            "org.slf4j:slf4j-simple:1.7.26",
            "org.spire-math:kind-projector_2.12:0.9.3",
            "org.tpolecat:doobie-core_2.12:0.9.2",
            "org.tpolecat:doobie-postgres_2.12:0.9.2",
            "org.typelevel:paiges-core_2.12:0.2.1",
            "org.wartremover:wartremover_2.12.12:2.4.10",
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
        maven_install_json = "@com_github_digital_asset_daml//:maven_install.json",
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
