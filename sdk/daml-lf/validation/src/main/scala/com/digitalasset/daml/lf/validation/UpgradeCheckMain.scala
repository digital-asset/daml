// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.canton.ledger.error.PackageServiceErrors.Validation
import java.io.File
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.archive.{Error => ArchiveError}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast

import com.digitalasset.canton.platform.apiserver.services.admin.PackageUpgradeValidator
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}

final case class CouldNotReadDar(path: String, err: ArchiveError) {
  val message: String = s"Error reading DAR from ${path}: ${err.msg}"
}

case class UpgradeCheckMain(loggerFactory: NamedLoggerFactory) {
  def logger = loggerFactory.getLogger(classOf[UpgradeCheckMain])

  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.empty

  private def decodeDar(
      path: String
  ): Either[CouldNotReadDar, Dar[(Ref.PackageId, Ast.Package)]] = {
    val s: String = s"Decoding DAR from ${path}"
    logger.debug(s)
    val result = DarDecoder.readArchiveFromFile(new File(path))
    result.left.map(CouldNotReadDar(path, _))
  }

  val validator = new PackageUpgradeValidator(
    getPackageMap = _ => Map.empty,
    getLfArchive = _ => _ => Future(None),
    loggerFactory = loggerFactory,
  )

  def check(paths: Array[String]): Int = {
    logger.debug(s"Called UpgradeCheckMain with paths: ${paths.toSeq.mkString("\n")}")

    val (failures, dars) = paths.partitionMap(decodeDar(_))
    if (failures.nonEmpty) {
      failures.foreach((e: CouldNotReadDar) => logger.error(e.message))
      1
    } else {
      val archives = for { dar <- dars; archive <- dar.all.toSeq } yield {
        logger.debug(s"Package with ID ${archive._1} and metadata ${archive._2.pkgNameVersion}")
        archive
      }

      val validation = validator.validateUpgrade(archives.toList)
      Await.result(validation.value, Duration.Inf) match {
        case Left(err: Validation.Upgradeability.Error) =>
          logger.error(s"Error while checking two DARs:\n${err.cause}")
          1
        case Left(err) =>
          logger.error(s"Error while checking two DARs:\n${err.cause}")
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
