// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.assembler

import java.nio.file.{Files, Paths}

import com.digitalasset.daml.lf.archive.Encode
import com.digitalasset.daml.lf.archive.EncodeV1.EncodeError
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.testing.parser.{ParserParameters, parseModules}
import com.digitalasset.daml.lf.validation.Validation

import scala.io.Source
import scala.util.control.NonFatal

object DamlLfAssembler extends App {

  private def error[X](message: String): X = {
    System.err.println(message)
    System.exit(1)
    throw new Error("You should not get this error")
  }

  private def assert[X](e: Either[String, X]) =
    e.fold(error, identity)

  private val pkgId = Ref.PackageId.assertFromString("-self-")

  try {
    val (inputFile, outputFile, languageVersion): (String, String, LanguageVersion) =
      args.toSeq match {
        case Seq(input, output) =>
          (input, output, LanguageVersion.default)
        case Seq(input, output, version) =>
          version.split("""\.""").toSeq match {
            case Seq("1", minor)
                if LanguageMajorVersion.V1.supportsMinorVersion(minor) || minor == "dev" =>
              (input, output, LanguageVersion(LanguageMajorVersion.V1, minor))
            case _ =>
              error(s"version '$version' not supported")
          }
        case _ =>
          error("usage: damlf-as input output [version]")
      }

    implicit val parserParameters: ParserParameters[this.type] =
      ParserParameters(
        defaultPackageId = pkgId,
        languageVersion = languageVersion
      )

    val string = Source.fromFile(Paths.get(inputFile).toFile, "UTF8").mkString

    val modules = parseModules[this.type](string).fold(error, identity)

    val pkgs = Map(pkgId -> Ast.Package(modules))

    Validation.checkPackage(pkgs, pkgId).left.foreach(e => error(e.pretty))

    val archive = Encode.encodeArchive(pkgId -> pkgs(pkgId), languageVersion)

    Files.write(Paths.get(outputFile), archive.toByteArray)

  } catch {
    case e: EncodeError =>
      error(s"Encoding error: ${e.message}")
    case NonFatal(e) =>
      error(s"error: ${e.getMessage}")
  }

}
