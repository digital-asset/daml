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
import com.daml.lf.language.Util.{dependenciesInTopologicalOrder}

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

  def topoSortArchives(
      archives: List[(Ref.PackageId, Ast.Package)]
  ): List[(Ref.PackageId, Ast.Package)] = {
    val archiveMap = archives.toMap
    for {
      pkgId <- dependenciesInTopologicalOrder(archives.map(_._1), archiveMap)
      pkg <- archiveMap.get(pkgId).toList
    } yield (pkgId, pkg)
  }

  def check(explicit: Boolean, paths: Array[String]): Int = {
    logger.debug(s"Called UpgradeCheckMain with paths: ${paths.toSeq.mkString("\n")}")

    val (failures, dars) = paths.partitionMap(decodeDar(_))
    if (failures.nonEmpty) {
      failures.foreach((e: CouldNotReadDar) => logger.error(e.message))
      1
    } else {
      dars.foreach(dar => validator.warnDamlScriptUpload(dar.main, dar.all))

      val validation = if (explicit) {
        val archives: List[(Ref.PackageId, Ast.Package)] = for {
          dar <- dars.toList
          archive <- topoSortArchives(dar.all)
        } yield {
          logger.debug(s"Package with ID ${archive._1} and metadata ${archive._2.metadata}")
          archive
        }
        validator.validateUpgradeInternal(archives.toMap, archives.map(_._1))
      } else {
        val archives: List[(Ref.PackageId, Ast.Package)] = for {
          dar <- dars.toList
          archive <- dar.all
        } yield {
          logger.debug(s"Package with ID ${archive._1} and metadata ${archive._2.metadata}")
          archive
        }
        validator.validateUpgrade(archives)
      }
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

  def main(args: Array[String]) = {
    val explicit = args.contains("--explicit")
    sys.exit(check(explicit, args.filter(_ != "--explicit")))
  }
}

object UpgradeCheckMain {
  lazy val default: UpgradeCheckMain = {
    UpgradeCheckMain(NamedLoggerFactory.root)
  }
}
