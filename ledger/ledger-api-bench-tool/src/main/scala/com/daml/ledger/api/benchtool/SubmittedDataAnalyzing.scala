// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.config.WorkflowConfig
import com.daml.ledger.api.benchtool.services.PackageService
import com.daml.ledger.api.benchtool.submission.BenchtoolTestsPackageInfo.BenchtoolTestsPackageName
import com.daml.ledger.api.benchtool.submission.{
  AllocatedParties,
  BenchtoolTestsPackageInfo,
  PartyAllocating,
}
import com.daml.ledger.api.v1.package_service.GetPackageResponse
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}

/** Contains utilities for retrieving useful facts
  * from data already submitted to a Ledger API server.
  * (The motivating use case are the benchmarks that do not perform a submission step on their own
  * and for that reason cannot statically determine these facts.)
  */
object SubmittedDataAnalyzing {

  private[benchtool] val logger: Logger = LoggerFactory.getLogger(getClass)

  def determineAllocatedParties(
      workflowConfig: WorkflowConfig,
      partyAllocating: PartyAllocating,
  )(implicit ec: ExecutionContext): Future[AllocatedParties] = {
    logger.info("Analyzing existing parties..")
    for {
      existingParties <- {
        logger.info("Analyzing existing parties..")
        partyAllocating.lookupExistingParties()
      }
    } yield {
      AllocatedParties.forExistingParties(
        parties = existingParties.toList,
        partySetPrefixes = workflowConfig.streams.flatMap(_.partySetPrefixes.iterator).distinct,
      )
    }
  }

  def determineBenchtoolTestsPackageId(
      packageService: PackageService
  )(implicit ec: ExecutionContext): Future[BenchtoolTestsPackageInfo] = {
    logger.info("Analyzing existing Daml packages..")
    for {
      packageIds: Seq[String] <- packageService.listPackages().map(_.packageIds)
      getPackageResponses: Seq[GetPackageResponse] <- Future.sequence(
        packageIds.map(packageId => packageService.getPackage(packageId = packageId))
      )
    } yield {
      val packageNamesToPackageIds: Seq[(String, String)] = for {
        getPackageResponse <- getPackageResponses
      } yield {
        val packageId = getPackageResponse.hash
        val packageName = decodePackageName(
          archivePayloadBytes = getPackageResponse.archivePayload.toByteArray,
          pkgId = Ref.PackageId.assertFromString(packageId),
        )
        packageName -> packageId
      }
      val candidatesPackageIds =
        packageNamesToPackageIds.collect { case (BenchtoolTestsPackageName, pkgId) => pkgId }
      if (candidatesPackageIds.size > 1) {
        logger.warn(s"Found more than one Daml package with name '$BenchtoolTestsPackageName'")
      }
      val detectedPackageInfoO = candidatesPackageIds.headOption.map(BenchtoolTestsPackageInfo(_))
      detectedPackageInfoO.getOrElse {
        logger.info(
          s"Could not find a Daml package with name '$BenchtoolTestsPackageName'; defaulting its packageId to the static one"
        )
        BenchtoolTestsPackageInfo.StaticDefault
      }
    }
  }

  private def decodePackageName(archivePayloadBytes: Array[Byte], pkgId: Ref.PackageId): String = {
    val pkg: Ast.Package = com.daml.lf.archive
      .archivePayloadDecoder(pkgId, onlySerializableDataDefs = false)
      .assertFromByteArray(archivePayloadBytes)
      ._2
    pkg.metadata.fold[String]("")(_.name)
  }

}
