import sbt._

import Versions._

version in ThisBuild := "0.0.1"
scalaVersion in ThisBuild := "2.12.8"
isSnapshot := true

lazy val parent = project
  .in(file("."))
  .settings(
    name := "iou-no-codegen",
    publishArtifact in (Compile, packageDoc) := false,
    publishArtifact in (Compile, packageSrc) := false
  )
  .aggregate(application)

lazy val application = project
  .in(file("application"))
  .settings(
    name := "application",
    commonSettings,
    libraryDependencies ++= applicationDependencies
  )

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-feature",
    "-target:jvm-1.8",
    "-deprecation",
    "-Xfatal-warnings",
    "-unchecked",
    "-Xfuture",
    "-Xlint:_,-unused"
  ),
  // uncomment next line, if you have to build against local maven repository
  // resolvers += Resolver.mavenLocal,
  classpathTypes += "maven-plugin"
)

lazy val applicationDependencies = Seq(
  "com.daml" %% "bindings-scala" % daSdkVersion,
  "com.daml" %% "bindings-akka" % daSdkVersion
)
