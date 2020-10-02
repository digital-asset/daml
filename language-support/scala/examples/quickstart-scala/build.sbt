import sbt._

import Versions._

version in ThisBuild := "0.0.1"
scalaVersion in ThisBuild := "2.12.8"
isSnapshot := true

lazy val parent = project
  .in(file("."))
  .settings(
    name := "quickstart-scala",
    publishArtifact in (Compile, packageDoc) := false,
    publishArtifact in (Compile, packageSrc) := false
  )
  .aggregate(`scala-codegen`, `application`)

// <doc-ref:modules>
lazy val `scala-codegen` = project
  .in(file("scala-codegen"))
  .settings(
    name := "scala-codegen",
    commonSettings,
    libraryDependencies ++= codeGenDependencies
  )

lazy val `application` = project
  .in(file("application"))
  .settings(
    name := "application",
    commonSettings,
    libraryDependencies ++= codeGenDependencies ++ applicationDependencies
  )
  .dependsOn(`scala-codegen`)
// </doc-ref:modules>

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-feature",
    "-target:jvm-1.8",
    "-deprecation",
    "-Xfatal-warnings",
    "-Xsource:2.13",
    "-unchecked",
    "-Xfuture",
    "-Xlint:_,-unused"
  ),
  // uncomment next line, if you have to build against local maven repository
  // resolvers += Resolver.mavenLocal,
  classpathTypes += "maven-plugin"
)

// <doc-ref:dependencies>
lazy val codeGenDependencies = Seq(
  "com.daml" %% "bindings-scala" % daSdkVersion
)

lazy val applicationDependencies = Seq(
  "com.daml" %% "bindings-akka" % daSdkVersion
)
// </doc-ref:dependencies>
