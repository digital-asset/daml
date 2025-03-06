// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.error.*
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.PackageServiceErrorGroup
import com.digitalasset.canton.error.{CantonError, ContextualizedCantonError, ParentCantonError}
import com.digitalasset.canton.ledger.error.PackageServiceErrors
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.admin.PackageService.DarDescription
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.value.Value.ContractId
import io.grpc.StatusRuntimeException

object CantonPackageServiceError extends PackageServiceErrorGroup {
  final case class IdentityManagerParentError(parent: ParticipantTopologyManagerError)(implicit
      val loggingContext: ErrorLoggingContext,
      override val code: ErrorCode,
  ) extends ContextualizedDamlError(parent.cause)
      with ContextualizedCantonError
      with ParentCantonError[ParticipantTopologyManagerError] {

    override val cause: String = parent.cause

    override def logOnCreation: Boolean = false

    override def asGrpcError: StatusRuntimeException = parent.asGrpcError

    override def asGrpcStatus: com.google.rpc.Status = ErrorCode.asGrpcStatus(this)(loggingContext)

    override def mixinContext: Map[String, String] = Map("action" -> "package-vetting")
  }

  @Explanation("Package fetching errors")
  object Fetching extends ErrorGroup {

    @Explanation(
      """The id specified in the request does not match the main package-id of a DAR stored on the participant."""
    )
    @Resolution("""Check the provided package ID and re-try the operation.""")
    object DarNotFound
        extends ErrorCode(
          id = "DAR_NOT_FOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      final case class Reject(operation: String, mainPackageId: String)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause =
              s"$operation operation failed due to non-existent DAR archive with main package-id $mainPackageId"
          )
    }

    @Explanation(
      """This error indicates that the requested package does not exist in the package store."""
    )
    @Resolution("Provide an existing package id")
    object InvalidPackageId
        extends ErrorCode(
          id = "PACKAGE_CONTENT_NOT_FOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      final case class NotFound(packageId: PackageId)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = show"The package ${packageId.readableHash} is not known"
          )
    }

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
    ) extends ContextualizedCantonError

    @Resolution(
      s"""To cleanly remove the package, you must archive all contracts from the package."""
    )
    class PackageInUse(
        val pkg: PackageId,
        val contract: ContractId,
        val synchronizerId: SynchronizerId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(
          s"Package $pkg is currently in-use by contract $contract on synchronizer $synchronizerId. " +
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

    import com.digitalasset.canton.util.ShowUtil.*
    @Resolution(
      s"""The DAR cannot be removed because a package in the DAR is in-use and is not found in any other DAR."""
    )
    class CannotRemoveOnlyDarForPackage(val pkg: PackageId, dar: DarDescription)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(
          cause =
            show"""The DAR $dar cannot be removed because it is the only DAR containing the used package ${pkg.readableHash},
               |but there are contracts using it. Either archive them or upload another DAR that contains it.
              .""".stripMargin
        )

    @Resolution(
      s"""Before removing a DAR, archive all contracts using its main package."""
    )
    class MainPackageInUse(
        val pkg: PackageId,
        dar: DarDescription,
        contractId: ContractId,
        synchronizerId: SynchronizerId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends PackageRemovalError(
          s"""The DAR $dar cannot be removed because its main package $pkg is in-use by contract $contractId
         |on synchronizer $synchronizerId.""".stripMargin
        )

  }

  @Explanation(
    """An operation failed with an internal error."""
  )
  @Resolution(
    """Contact support."""
  )
  object InternalError {
    final case class Error(_reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(_reason)(PackageServiceErrors.InternalError)
  }

}
