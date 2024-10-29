// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.digitalasset.canton.ledger.error.PackageServiceErrors.Validation
import java.io.File
import com.daml.lf.archive.DarDecoder
import com.daml.lf.archive.Dar
import com.daml.lf.archive.{Error => ArchiveError}
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast

import com.digitalasset.canton.participant.admin.PackageUpgradeValidator
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration._
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}

final case class CouldNotReadDar(path: String, err: ArchiveError) {
  val message: String = s"Error reading DAR from ${path}: ${err.msg}"
}

case class UpgradeCheckMain() {}
object UpgradeCheckMain {
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.empty
  val loggerFactory = NamedLoggerFactory.root
  def logger = loggerFactory.getLogger(classOf[UpgradeCheckMain])

  private def decodeDar(
      path: String
  ): Either[CouldNotReadDar, Dar[(Ref.PackageId, Ast.Package)]] = {
    logger.debug(s"Decoding DAR from ${path}")
    val result = DarDecoder.readArchiveFromFile(new File(path))
    result.left.map(CouldNotReadDar(path, _))
  }

  val validator = new PackageUpgradeValidator(
    getPackageMap = _ => Map.empty,
    getLfArchive = _ => _ => Future(None),
    loggerFactory = loggerFactory,
  )

  def main(args: Array[String]): Unit = {
    logger.debug(s"Called UpgradeCheckMain with args: ${args.toSeq.mkString("\n")}")

    val (failures, dars) = args.partitionMap(decodeDar(_))
    if (failures.nonEmpty) {
      failures.foreach((e: CouldNotReadDar) => logger.error(e.message))
      sys.exit(1)
    } else {
      val archives = for { dar <- dars; archive <- dar.all.toSeq } yield {
        logger.debug(s"Package with ID ${archive._1} and metadata ${archive._2.metadata}")
        archive
      }

      val validation = validator.validateUpgrade(archives.toList)
      Await.result(validation.value, Duration.Inf) match {
        case Left(err: Validation.Upgradeability.Error) =>
          logger.error(s"Error while checking two DARs:\n${err.upgradeError.prettyInternal}")
          sys.exit(1)
        case Left(err) =>
          logger.error(s"Error while checking two DARs:\n${err.cause}")
          sys.exit(1)
        case Right(()) => ()
      }
    }
  }
}
