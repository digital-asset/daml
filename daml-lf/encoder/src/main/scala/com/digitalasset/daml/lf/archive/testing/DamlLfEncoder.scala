// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.archive.testing

import java.io.File
import java.nio.file.Paths

import com.daml.lf.archive.{Dar, DarWriter}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.{Ast, PackageInterface, LanguageVersion}
import com.daml.lf.testing.parser.{ParserParameters, parseModules}
import com.daml.lf.validation.Validation
import com.daml.SdkVersion

import scala.Ordering.Implicits.infixOrderingOps
import scala.annotation.tailrec
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
          languageVersion = appArgs.languageVersion,
        )

      makeDar(readSources(appArgs.inputFiles), Paths.get(appArgs.outputFile).toFile)

    } catch {
      case e: EncodeError =>
        error(s"Encoding error: ${e.message}")
      case NonFatal(e) =>
        error(s"error: $e")
    }

  private def readSources(files: Seq[String]): String =
    files.view.flatMap(file => Source.fromFile(Paths.get(file).toFile, "UTF8")).mkString

  private def makeArchive(
      source: String
  )(implicit parserParameters: ParserParameters[this.type]) = {

    val modules = parseModules[this.type](source).fold(error, identity)

    val metadata =
      if (parserParameters.languageVersion >= LanguageVersion.Features.packageMetadata) {
        Some(
          Ast.PackageMetadata(
            Ref.PackageName.assertFromString("encoder_binary"),
            Ref.PackageVersion.assertFromString("1.0.0"),
          )
        )
      } else None

    val pkg =
      Ast.Package(modules, Set.empty[PackageId], parserParameters.languageVersion, metadata)
    val pkgs = PackageInterface(Map(pkgId -> pkg))

    Validation.checkPackage(pkgs, pkgId, pkg).left.foreach(e => error(e.pretty))

    encodeArchive(pkgId -> pkg, parserParameters.languageVersion)
  }

  private def makeDar(source: String, file: File)(implicit
      parserParameters: ParserParameters[this.type]
  ) = {
    val archive = makeArchive(source)
    DarWriter.encode(
      SdkVersion.sdkVersion,
      Dar(("archive.dalf", archive.toByteArray), List()),
      file.toPath,
    )
  }

  private case class Arguments(
      inputFiles: List[String],
      outputFile: String,
      languageVersion: LanguageVersion,
  )

  private def parseArgs() = {
    val nAgrs = args.length

    @tailrec
    def go(
        appArgs: Arguments = Arguments(List.empty, "", LanguageVersion.default),
        i: Int = 0,
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
            go(appArgs.copy(languageVersion = LanguageVersion.assertFromString(args(i + 1))), i + 2)
          case "--output" if i + 1 < nAgrs =>
            go(appArgs.copy(outputFile = args(i + 1)), i + 2)
          case _ if i + 1 >= nAgrs =>
            error(
              s"usage: encoder_binary inputFile1 ... inputFileN --output outputFile [--target version]"
            )
          case x =>
            go(appArgs.copy(inputFiles = x :: appArgs.inputFiles), i + 1)
        }

    go()
  }

  main()

}
