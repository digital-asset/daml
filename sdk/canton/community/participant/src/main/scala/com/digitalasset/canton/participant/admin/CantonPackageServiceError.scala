// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.error.{DamlError, ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.PackageServiceErrorGroup
import com.digitalasset.canton.error.{CantonError, ParentCantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.admin.PackageService.DarDescriptor
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.topology.DomainId
import io.grpc.StatusRuntimeException

import scala.annotation.nowarn

object CantonPackageServiceError extends PackageServiceErrorGroup {
  @nowarn("msg=early initializers are deprecated")
  final case class IdentityManagerParentError(parent: ParticipantTopologyManagerError)(implicit
      val loggingContext: ErrorLoggingContext,
      override val code: ErrorCode,
  ) extends {
        override val cause: String = parent.cause
      }
      with DamlError(parent.cause)
      with CantonError
      with ParentCantonError[ParticipantTopologyManagerError] {

    override def logOnCreation: Boolean = false

    override def asGrpcError: StatusRuntimeException = parent.asGrpcError

    override def mixinContext: Map[String, String] = Map("action" -> "package-vetting")
  }

  @Explanation(
    """The hash specified in the request does not match a DAR stored on the participant."""
  )
  @Resolution("""Check the provided hash and re-try the operation.""")
  object DarNotFound
      extends ErrorCode(
        id = "DAR_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Reject(operation: String, darHash: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"$operation operation failed due to non-existent DAR archive for hash $darHash"
        )
  }

  @Explanation(
    """An operation could not be executed due to a missing package dependency."""
  )
  @Resolution(
    """Fix the broken dependency state by re-uploading the missing package and retry the operation."""
  )
  object PackageMissingDependencies
      extends ErrorCode(
        id = "PACKAGE_MISSING_DEPENDENCIES",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {

    final case class Reject(mainPackage: String, missingDependency: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Package $mainPackage is missing dependency $missingDependency"
        )
  }

  @Explanation(
    """Errors raised by the Package Service on package removal."""
  )
  object PackageRemovalErrorCode
      extends ErrorCode(
        id = "PACKAGE_OR_DAR_REMOVAL_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    abstract class PackageRemovalError(override val cause: String)(
        override implicit val code: ErrorCode
    ) extends CantonError

    @Resolution(
      s"""To cleanly remove the package, you must archive all contracts from the package."""
    )
    class PackageInUse(val pkg: PackageId, val contract: ContractId, val domain: DomainId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(
          s"Package $pkg is currently in-use by contract $contract on domain $domain. " +
            s"It may also be in-use by other contracts."
        )

    @Resolution(
      s"""Packages needed for admin workflows cannot be removed."""
    )
    class CannotRemoveAdminWorkflowPackage(val pkg: PackageId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(
          s"""Removal of package $pkg failed as packages needed for admin workflows cannot be removed."""
        )

    @Resolution(
      s"""To cleanly remove the package, you must first revoke authorization for the package."""
    )
    class PackageVetted(pkg: PackageId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(s"Package $pkg is currently vetted and available to use.")

    @Resolution(
      s"""The DAR cannot be removed because a package in the DAR is in-use and is not found in any other DAR."""
    )
    class CannotRemoveOnlyDarForPackage(val pkg: PackageId, dar: DarDescriptor)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(
          cause =
            s"""The DAR $dar cannot be removed because it is the only DAR containing the used package $pkg.
               |Archive all contracts using the package $pkg,
               | or supply an alternative dar to $dar that contains $pkg.""".stripMargin
        )

    @Resolution(
      s"""Before removing a DAR, archive all contracts using its main package."""
    )
    class MainPackageInUse(
        val pkg: PackageId,
        dar: DarDescriptor,
        contractId: ContractId,
        domainId: DomainId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(
          s"""The DAR $dar cannot be removed because its main package $pkg is in-use by contract $contractId
         |on domain $domainId.""".stripMargin
        )

    @Resolution(
      resolution =
        s"Inspect the specific topology error, or manually revoke the package vetting transaction corresponding to" +
          s" the main package of the dar"
    )
    class DarUnvettingError(
        err: ParticipantTopologyManagerError,
        dar: DarDescriptor,
        mainPkg: PackageId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(
          s"An error was encountered whilst trying to unvet the DAR $dar with main package $mainPkg for DAR" +
            s" removal. Details: $err"
        )

  }

}
