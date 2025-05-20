// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.digitalasset.canton.ledger.error.PackageServiceErrors.Validation
import java.io.File
import com.daml.lf.archive.{DarDecoder, ArchiveDecoder}
import com.daml.lf.archive.Dar
import com.daml.lf.archive.{Error => ArchiveError}
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast

import com.digitalasset.canton.participant.admin.PackageUpgradeValidator
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}

final case class CouldNotReadDarOrDalf(path: String, darErr: ArchiveError, dalfErr: ArchiveError) {
  val message: String = s"Error reading ${path}: file is neither a DAR nor a DALF.\nError when trying to read as DAR: ${darErr}\nError when trying to read as DALF: ${dalfErr}"
}

case class UpgradeCheckMain(loggerFactory: NamedLoggerFactory) {
  def logger = loggerFactory.getLogger(classOf[UpgradeCheckMain])

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.empty

  private def decodeDarOrDalf(
      path: String
  ): Either[CouldNotReadDarOrDalf, Dar[(Ref.PackageId, Ast.Package)]] = {
    val s: String = s"Decoding DAR from ${path}"
    logger.debug(s)
    val result = DarDecoder.readArchiveFromFile(new File(path))
    result match {
      case Left(darErr) => ArchiveDecoder.fromFile(new File(path)) match {
        case Left(dalfErr) => Left(CouldNotReadDarOrDalf(path, darErr, dalfErr))
        case Right(x) => Right(Dar(x, List()))
      }
      case Right(x) => Right(x)
    }
  }

  val validator = new PackageUpgradeValidator(
    getPackageMap = _ => Map.empty,
    getLfArchive = _ => _ => Future(None),
    loggerFactory = loggerFactory,
  )

  def check(paths: Array[String]): Int = {
    logger.debug(s"Called UpgradeCheckMain with paths: ${paths.toSeq.mkString("\n")}")

    val (failures, dars) = paths.partitionMap(decodeDarOrDalf(_))
    if (failures.nonEmpty) {
      failures.foreach((e: CouldNotReadDarOrDalf) => logger.error(e.message))
      1
    } else {
      val archives = for { dar <- dars; archive <- dar.all.toSeq } yield {
        logger.debug(s"Package with ID ${archive._1} and metadata ${archive._2.metadata}")
        archive
      }

      dars.foreach(dar => validator.warnDamlScriptUpload(dar.main, dar.all))

      val validation = validator.validateUpgrade(archives.toList)
      Await.result(validation.value, Duration.Inf) match {
        case Left(err: Validation.Upgradeability.Error) =>
          logger.error(s"Error while checking DARs:\n${err.cause}")
          1
        case Left(err) =>
          logger.error(s"Error while checking DARs:\n${err.cause}")
          1
        case Right(()) => 0
      }
    }
  }

  def main(args: Array[String]) = sys.exit(check(args))
}

object UpgradeCheckMain {
  lazy val default: UpgradeCheckMain = {
    UpgradeCheckMain(NamedLoggerFactory.root)
  }
}
