// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive.testing

import java.io.File
import java.nio.file.Paths
import com.daml.lf.archive.{Dar, DarWriter}
import com.daml.lf.data.Ref
import com.daml.lf.language.{LanguageVersion, PackageInterface}
import com.daml.lf.testing.parser.{ParserParameters, parsePackage}
import com.daml.lf.validation.Validation
import com.daml.SdkVersion

import scala.annotation.tailrec
import scala.collection.immutable.Queue
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

      System.err.println(
        "Daml-LF encoder is designed for testing purpose only, and provided without any guarantee of stability."
      )

      val appArgs = parseArgs()

      implicit val parserParameters: ParserParameters[this.type] =
        ParserParameters(
          defaultPackageId = pkgId,
          languageVersion = appArgs.languageVersion,
        )

      makeDar(
        readSources(appArgs.inputFiles),
        Paths.get(appArgs.outputFile).toFile,
        validation = appArgs.validation,
      )

    } catch {
      case e: EncodeError =>
        error(s"Encoding error: ${e.message}")
      case NonFatal(e) =>
        error(s"error: $e")
    }

  private def readSources(files: Seq[String]): String =
    files.view.flatMap(file => Source.fromFile(Paths.get(file).toFile, "UTF8")).mkString

  private def makeArchive(
      source: String,
      validation: Boolean,
  )(implicit parserParameters: ParserParameters[this.type]) = {

    val pkg = parsePackage[this.type](source).fold(error, identity)
    val pkgs = PackageInterface(Map(pkgId -> pkg))

    if (validation)
      Validation.checkPackage(pkgs, pkgId, pkg).left.foreach(e => error(e.pretty))

    encodeArchive(pkgId -> pkg, parserParameters.languageVersion)
  }

  private def makeDar(source: String, file: File, validation: Boolean)(implicit
      parserParameters: ParserParameters[this.type]
  ) = {
    val archive = makeArchive(source, validation = validation)
    DarWriter.encode(
      SdkVersion.sdkVersion,
      Dar(("archive.dalf", archive.toByteArray), List()),
      file.toPath,
    )
  }

  private case class Arguments(
      inputFiles: Queue[String],
      outputFile: String,
      languageVersion: LanguageVersion,
      validation: Boolean,
  )

  private def parseArgs() = {
    @tailrec
    def go(appArgs: Arguments, args: List[String]): Arguments =
      args match {
        case "--target" :: version :: tail =>
          go(appArgs.copy(languageVersion = LanguageVersion.assertFromString(version)), tail)
        case "--output" :: file :: tail =>
          go(appArgs.copy(outputFile = file), tail)
        case "--skip-validation" :: tail =>
          go(appArgs.copy(validation = false), tail)
        case option :: _ if option.startsWith("-") =>
          error(
            s"usage: encoder_binary inputFile1 ... inputFileN --output outputFile [--target version]"
          )
        case Nil =>
          if (appArgs.outputFile.isEmpty)
            error("output file not set")
          if (appArgs.inputFiles.isEmpty)
            error("no input files set")
          else
            appArgs
        case x :: tail =>
          go(appArgs.copy(inputFiles = appArgs.inputFiles.enqueue(x)), tail)
      }

    go(Arguments(Queue.empty, "", LanguageVersion.default, true), args.toList)
  }

  main()

}
