import sbt._

import com.digitalasset.codegen.CodeGen.Novel
import com.digitalasset.codegen.CodeGen

import Versions._
import Artifactory._

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
    libraryDependencies ++= codeGenDependencies,
    sourceGenerators in Compile += (damlScala in Compile).taskValue,
    damlScala in Compile := {
      generateScalaFrom(
        darFile = darFile,
        packageName = "com.digitalasset.quickstart.iou.model",
        outputDir = (sourceManaged in Compile).value,
        cacheDir = streams.value.cacheDirectory / name.value
      ).toSeq
    }
  )

lazy val `application` = project
  .in(file("application"))
  .settings(
    name := "application",
    commonSettings,
    libraryDependencies ++= codeGenDependencies ++ applicationDependencies,
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
  resolvers ++= daResolvers,
  classpathTypes += "maven-plugin",
)

// <doc-ref:dependencies>
lazy val codeGenDependencies = Seq(
  "com.daml.scala" %% "bindings" % daSdkVersion,
)

lazy val applicationDependencies = Seq(
  "com.daml.scala" %% "bindings-akka" % daSdkVersion,
)
// </doc-ref:dependencies>

lazy val damlScala = taskKey[Seq[File]]("Generate Scala code.")
damlScala := Seq() // by default, do nothing

// <doc-ref:generate-scala>
def generateScalaFrom(
    darFile: File,
    packageName: String,
    outputDir: File,
    cacheDir: File): Set[File] = {

  require(
    darFile.getPath.endsWith(".dar") && darFile.exists(),
    s"DAR file doest not exist: ${darFile.getPath: String}")

  val cache = FileFunction.cached(cacheDir, FileInfo.hash) { _ =>
    if (outputDir.exists) IO.delete(outputDir.listFiles)
    CodeGen.generateCode(List(darFile), packageName, outputDir, Novel)
    (outputDir ** "*.scala").get.toSet
  }
  cache(Set(darFile))
}
// </doc-ref:generate-scala>
