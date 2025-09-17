// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import java.io.File
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.archive.Dar
import com.digitalasset.daml.lf.archive.{Error => ArchiveError}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Util
import com.digitalasset.canton.platform.apiserver.services.admin.PackageUpgradeValidator

import scala.concurrent.ExecutionContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.config.CachingConfigs

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

  val validator =
    new PackageUpgradeValidator(CachingConfigs.defaultPackageUpgradeCache, loggerFactory)

  def check(paths: Array[String]): Int = {
    logger.debug(s"Called UpgradeCheckMain with paths: ${paths.toSeq.mkString("\n")}")

    val (failures, dars) = paths.partitionMap(decodeDar(_))
    if (failures.nonEmpty) {
      failures.foreach((e: CouldNotReadDar) => logger.error(e.message))
      1
    } else {
      val packageSigs = for {
        dar <- dars
        (pkgId, pkg) <- dar.all.toSeq
        if pkg.supportsUpgrades(pkgId)
      } yield {
        logger.debug(s"Package with ID $pkgId and metadata ${pkg.metadata}")
        pkgId -> Util.toSignature(pkg)
      }

      val validation = validator.validateUpgrade(
        allPackages = packageSigs.toList.distinct
      )
      validation match {
        case Left(err: TopologyManagerError.ParticipantTopologyManagerError.Upgradeability.Error) =>
          logger.error(s"Error while checking two DARs:\n${err.cause}")
          1
        case Left(err) =>
          logger.error(s"Error while checking two DARs:\n${err.cause}")
          1
        case Right(()) => 0
        case _ => 1
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
