// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.implicits.toTraverseOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.ContractImportMode.Validation
import com.digitalasset.canton.participant.admin.data.{
  ContractImportMode,
  RepairContract,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.store.packagemeta.PackageMetadata.PackageResolution
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LfPackageId, LfPackageName}

/** Select new representative package IDs for ACS import contracts based on the precedence rules
  * defined in [[com.digitalasset.canton.admin.participant.v30.ImportAcsRequest]]
  *
  * @param representativePackageIdOverride
  *   The representative package-id overrides to be applied to the imported contracts
  * @param knownPackages
  *   All packages known to the participant at the time of the ACS import request
  * @param packageNameMap
  *   Mapping of package-names to package-resolutions known to the participant at the time of the
  *   ACS import request
  */
private[admin] class SelectRepresentativePackageIds(
    representativePackageIdOverride: RepresentativePackageIdOverride,
    knownPackages: Set[LfPackageId],
    packageNameMap: Map[LfPackageName, PackageResolution],
    contractImportMode: ContractImportMode,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def apply(
      contracts: Seq[RepairContract]
  )(implicit traceContext: TraceContext): Either[String, Seq[RepairContract]] =
    contracts.traverse(selectRpId)

  private def selectRpId(
      repairContract: RepairContract
  )(implicit traceContext: TraceContext): Either[String, RepairContract] = {
    import representativePackageIdOverride.*

    val rpIdSelectionPrecedence = Seq(
      RPIdCandidate(
        contractOverride.get(repairContract.contract.contractId),
        "Contract ID override",
      ),
      RPIdCandidate(
        packageIdOverride.get(repairContract.representativePackageId),
        "Override the exported representative package ID",
      ),
      RPIdCandidate(
        packageIdOverride.get(repairContract.contract.templateId.packageId),
        "Override the creation package ID",
      ),
      RPIdCandidate(
        Option(repairContract.representativePackageId),
        "Representative package ID from export",
      ),
      RPIdCandidate(
        Option(repairContract.contract.templateId.packageId),
        "Contract creation package ID",
      ),
      RPIdCandidate(
        packageNameOverride.get(repairContract.contract.packageName),
        "Package-name override",
      ),
      // TODO(#28075): Add package vetting-based override
      RPIdCandidate(
        packageNameMap.get(repairContract.contract.packageName).map(_.preference.packageId),
        "Highest versioned (package store) package-id for the contract's package-name",
      ),
    )

    rpIdSelectionPrecedence.view
      .map(_.evaluated)
      .collectFirst { case Some(rpIdSelection) => rpIdSelection }
      .toRight(
        show"Could not select a representative package-id for contract with id ${repairContract.contract.contractId}. No package in store for the contract's package-name '${repairContract.contract.packageName}'."
      )
      .flatMap { case RPIdSelection(selectedRpId, sourceDescription) =>
        if (selectedRpId != repairContract.representativePackageId) {
          if (contractImportMode == Validation) {
            logger.debug(
              show"Representative package-id changed from ${repairContract.representativePackageId} to $selectedRpId for contract ${repairContract.contract.contractId}. Reason: $sourceDescription"
            )
            Right(selectedRpId)
          } else
            Left(
              show"Contract import mode is '$contractImportMode' but the selected representative package-id $selectedRpId " +
                show"for contract with id ${repairContract.contract.contractId} differs from the exported representative package-id ${repairContract.representativePackageId}. " +
                show"Please use contract import mode '${ContractImportMode.Validation}' to change the representative package-id."
            )
        } else Right(selectedRpId)
      }
      .map(selectedRPId => repairContract.copy(representativePackageId = selectedRPId))
  }

  private case class RPIdSelection(packageId: LfPackageId, selectionSourceDescription: String)

  private case class RPIdCandidate(
      candidate: Option[LfPackageId],
      selectionSourceDescription: String,
  ) {
    val evaluated: Option[RPIdSelection] =
      candidate.filter(knownPackages.contains).map(RPIdSelection(_, selectionSourceDescription))
  }
}
