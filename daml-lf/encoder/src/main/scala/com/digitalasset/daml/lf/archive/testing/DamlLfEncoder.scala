// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive.testing

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.zip.ZipEntry

import com.daml.lf.data.Ref
import com.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.daml.lf.testing.parser.{ParserParameters, parseModules}
import com.daml.lf.validation.Validation

import scala.annotation.tailrec
import scala.collection.breakOut
import scala.io.Source
import scala.util.control.NonFatal

private[daml] object DamlLfEncoder extends App {

  import Encode._

  private def error[X](message: String): X = {
    System.err.println(message)
    System.exit(1)
    throw new Error("You should not get this error")
  }

  private val pkgId = Ref.PackageId.assertFromString("-self-")

  private def main() =
    try {
      val appArgs = parseArgs()

      implicit val parserParameters: ParserParameters[this.type] =
        ParserParameters(
          defaultPackageId = pkgId,
          languageVersion = appArgs.languageVersion
        )

      makeDar(readSources(appArgs.inputFiles), Paths.get(appArgs.outputFile).toFile)

    } catch {
      case e: EncodeError =>
        error(s"Encoding error: ${e.message}")
      case NonFatal(e) =>
        error(s"error: $e")
    }

  private def readSources(files: Seq[String]): String =
    files.flatMap(file => Source.fromFile(Paths.get(file).toFile, "UTF8"))(breakOut)

  private def makeArchive(source: String)(
      implicit parserParameters: ParserParameters[this.type]) = {

    val modules = parseModules[this.type](source).fold(error, identity)

    val metadata =
      if (LanguageVersion.ordering
          .gteq(parserParameters.languageVersion, LanguageVersion.Features.packageMetadata)) {
        Some(
          Ast.PackageMetadata(
            Ref.PackageName.assertFromString("encoder_binary"),
            Ref.PackageVersion.assertFromString("1.0.0")))
      } else None

    val pkgs = Map(pkgId -> Ast.Package(modules, Set.empty[Ref.PackageId], metadata))

    Validation.checkPackage(pkgs, pkgId).left.foreach(e => error(e.pretty))

    encodeArchive(pkgId -> pkgs(pkgId), parserParameters.languageVersion)
  }

  private def makeDar(source: String, file: File)(
      implicit parserParameters: ParserParameters[this.type]) = {
    import java.io.FileOutputStream
    import java.util.zip.ZipOutputStream

    val archive = makeArchive(source)

    val out = new ZipOutputStream(new FileOutputStream(file))
    out.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"))
    out.write(MANIFEST)
    out.closeEntry()

    out.putNextEntry(new ZipEntry("archive.dalf"))
    out.write(archive.toByteArray)
    out.closeEntry()
    out.close()

  }

  private case class Arguments(
      inputFiles: List[String],
      outputFile: String,
      languageVersion: LanguageVersion
  )

  private def parseArgs() = {
    val nAgrs = args.length

    @tailrec
    def go(
        appArgs: Arguments = Arguments(List.empty, "", LanguageVersion.default),
        i: Int = 0
    ): Arguments =
      if (i == nAgrs) {
        if (appArgs.outputFile.isEmpty)
          error("output file not set")
        if (appArgs.inputFiles.isEmpty)
          error("no input files set")
        else
          appArgs
      } else
        args(i) match {
          case "--target" if i + 1 < nAgrs =>
            go(appArgs.copy(languageVersion = parseVersion(args(i + 1))), i + 2)
          case "--output" if i + 1 < nAgrs =>
            go(appArgs.copy(outputFile = args(i + 1)), i + 2)
          case _ if i + 1 >= nAgrs =>
            error(
              s"usage: encoder_binary inputFile1 ... inputFileN --output outputFile [--target version]")
          case x =>
            go(appArgs.copy(inputFiles = x :: appArgs.inputFiles), i + 1)
        }

    go()
  }

  private def parseVersion(version: String) =
    version.split("""\.""").toSeq match {
      case Seq("1", minor)
          if LanguageMajorVersion.V1.supportsMinorVersion(minor) || minor == "dev" =>
        LanguageVersion(LanguageMajorVersion.V1, minor)
      case _ =>
        error(s"version '$version' not supported")
    }

  // Be careful when adjusting the Manifest.
  // Each line of the manifest must be shorter than 72 bytes.
  // See https://docs.oracle.com/javase/6/docs/technotes/guides/jar/jar.html#JAR%20Manifest.
  private val MANIFEST =
    """Manifest-Version: 1.0
      |Created-By: DAML-LF Encoder
      |Sdk-Version: 0.0.0
      |Main-Dalf: archive.dalf
      |Dalfs: archive.dalf
      |Format: daml-lf
      |Encryption: non-encrypted
      |""".stripMargin.getBytes(StandardCharsets.US_ASCII)

  main()

}
