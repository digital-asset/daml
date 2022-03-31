// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

ThisBuild / scalaVersion := "2.13.8"
ThisBuild / version := sys.env.get("VERSION").getOrElse("0.0.0")
ThisBuild / organization := "com.daml"
ThisBuild / organizationName := "Digital Asset"
ThisBuild / licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

publishTo := Some(MavenCache("temp", file(sys.env.get("TEMP_MVN").get)))

addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full)

lazy val root = (project in file("."))
  .settings(
    name := "scalatest-utils",
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.15.4",
      "org.scalactic" %% "scalactic" % "3.2.9",
      "org.scalatest" %% "scalatest-core" % "3.2.9",
      "org.scalatest" % "scalatest-compatible" % "3.2.9",
      "org.scalatest" %% "scalatest-flatspec" % "3.2.9",
      "org.scalatest" %% "scalatest-matchers-core" % "3.2.9",
      "org.scalatest" %% "scalatest-shouldmatchers" % "3.2.9",
      "org.scalatest" %% "scalatest-wordspec" % "3.2.9",
      "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0",
      "org.scalaz" %% "scalaz-core" % "7.2.33",
    ),
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
