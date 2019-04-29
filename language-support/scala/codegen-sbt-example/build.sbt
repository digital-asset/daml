import sbt._

import com.digitalasset.codegen.CodeGen.Novel
import com.digitalasset.codegen.CodeGen
import com.digitalasset.damlc.Runner.{main => runDamlc}

import Versions._
import Artifactory._

version in ThisBuild := "0.0.1"
scalaVersion in ThisBuild := "2.12.8"
isSnapshot := true

lazy val parent = project
  .in(file("."))
  .settings(
    name := "codegen-sbt-example",
    description := "Scala Codegen SBT Example",
    publishArtifact in (Compile, packageDoc) := false,
    publishArtifact in (Compile, packageSrc) := false
  )
  .aggregate(`scala-codegen`, `mock-example`, `sandbox-example`)

lazy val `scala-codegen` = project
  .in(file("scala-codegen"))
  .settings(
    name := "scala-codegen",
    commonSettings,
    libraryDependencies ++= codeGenDependencies,
    damlPackage in Compile := {
      val damlDir = (baseDirectory in ThisBuild).value / "daml-model" / "src"
      val cacheDir = streams.value.cacheDirectory / "daml-model" / "src"
      val darFile = (baseDirectory in Compile).value / "target" / "repository" / "daml-codegen" / "Main.dar"
      compileDaml(streams.value.log, damlDir, damlDir / "Main.daml", "Main", darFile, cacheDir)
    },
    sourceGenerators in Compile += (damlScala in Compile).taskValue,
    damlScala in Compile := ((damlPackage in Compile).value match {
      case None =>
        Seq.empty[File]
      case Some(darFile) =>
        generateScalaFrom(
          darFile,
          "Main.dalf",
          "com.digitalasset.example.daml",
          (sourceManaged in Compile).value,
          streams.value.cacheDirectory / name.value / "dalfs").toSeq
    })
  )

lazy val `mock-example` = project
  .in(file("mock-example"))
  .settings(
    name := "mock-example",
    commonSettings,
    libraryDependencies ++= codeGenDependencies,
  )
  .dependsOn(`scala-codegen`)

lazy val `sandbox-example` = project
  .in(file("sandbox-example"))
  .settings(
    name := "sandbox-example",
    commonSettings,
    libraryDependencies ++= codeGenDependencies ++ sandboxDependencies,
  )
  .dependsOn(`scala-codegen`)

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

lazy val sandboxDependencies = Seq(
  "com.daml.scala" %% "bindings-akka" % sdkVersion,
  "com.digitalasset.platform" %% "sandbox" % sdkVersion,
)

lazy val codeGenDependencies = Seq(
  "com.daml.scala" %% "bindings" % sdkVersion,
)

// ##################################### inlined sbt-daml ##############################
lazy val damlPackage = taskKey[Option[File]]("Create DAML Package (DAR).")
damlPackage := None // by default, do nothing

lazy val damlScala = taskKey[Seq[File]]("Generate Scala code.")
damlScala := Seq() // by default, do nothing

def compileDaml(
    log: internal.util.ManagedLogger,
    srcDir: File,
    input: File,
    packageName: String,
    output: File,
    cacheDir: File): Option[File] = {

  require(srcDir.isDirectory)
  require(input.isFile)
  require(input.toPath.startsWith(srcDir.toPath))
  require(input.getName.endsWith(".daml"))

  val cache: Set[File] => Set[File] = FileFunction.cached(cacheDir, FilesInfo.hash) {
    files: Set[File] =>
      log.info("DAML changes detected, rebuilding DAR.")
      log.info(s"Changed files:")
      files.foreach(f => log.info(s"\t${f.getAbsolutePath: String}"))

      output.getParentFile.mkdirs()
      runDamlc(
        Array("package", input.getAbsolutePath, packageName, "--output", output.getAbsolutePath))

      val generatedFiles: Set[File] = output.getParentFile.listFiles.toSet
      val generatedFileNames: Set[String] = generatedFiles.map(_.getAbsolutePath)
      val expectedFileName: String = output.getAbsolutePath

      if (generatedFileNames != Set(expectedFileName))
        sys.error(s"Expected to create exactly one archive: ${expectedFileName: String}, but got: ${generatedFileNames
          .mkString(", "): String}.\nPlease run `sbt clean`. If the issue still present, this is probably because `damlc` output changed.")

      log.info(s"Created: ${expectedFileName: String}")
      generatedFiles
  }
  cache((srcDir ** "*.daml").get.toSet).headOption
}

def generateScalaFrom(
    darFile: File,
    mainDalfName: String,
    packageName: String,
    srcManagedDir: File,
    cacheDir: File): Set[File] = {

  require(darFile.getPath.endsWith(".dar"))

  // directory containing the dar is used as a work directory
  val outDir = darFile.getParentFile

  // use a FileFunction.cached on the dar
  val cache = FileFunction.cached(cacheDir, FileInfo.hash) { _ =>
    if (srcManagedDir.exists) // purge output if exists
      IO.delete(srcManagedDir.listFiles)

    CodeGen.generateCode(List(darFile), packageName, srcManagedDir, Novel)
    (srcManagedDir ** "*.scala").get.toSet
  }
  cache(Set(darFile))
}

// #####################################   end sbt-daml   ##############################
