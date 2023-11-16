// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.RepairServiceErrorGroup
import com.digitalasset.canton.error.{BaseCantonError, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.ProtocolVersion

sealed trait RepairServiceError extends Product with Serializable with BaseCantonError

object RepairServiceError extends RepairServiceErrorGroup {

  @Explanation("The participant does not support the requested protocol version.")
  @Resolution(
    "Specify a protocol version that the participant supports or upgrade the participant to a release that supports the requested protocol version."
  )
  object UnsupportedProtocolVersionParticipant
      extends ErrorCode(
        id = "UNSUPPORTED_PROTOCOL_VERSION_PARTICIPANT",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(
        requestedProtocolVersion: ProtocolVersion,
        supportedVersions: Seq[ProtocolVersion] = ProtocolVersion.supported,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The participant does not support the requested protocol version $requestedProtocolVersion"
        )
        with RepairServiceError
  }

  @Explanation(
    "The participant does not support serving an ACS snapshot at the requested timestamp, likely because some concurrent processing has not yet finished."
  )
  @Resolution(
    "Make sure that the specified timestamp has been obtained from the participant in some way. If so, retry after a bit (possibly repeatedly)."
  )
  object InvalidAcsSnapshotTimestamp
      extends ErrorCode(
        id = "INVALID_ACS_SNAPSHOT_TIMESTAMP",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    final case class Error(
        requestedTimestamp: CantonTimestamp,
        cleanTimestamp: CantonTimestamp,
        domainId: DomainId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The participant does not yet support serving an ACS snapshot at the requested timestamp $requestedTimestamp on domain $domainId"
        )
        with RepairServiceError
  }

  @Explanation(
    "The participant does not support serving an ACS snapshot at the requested timestamp because its database has already been pruned, e.g., by the continuous background pruning process."
  )
  @Resolution(
    "The snapshot at the requested timestamp is no longer available. Pick a more recent timestamp if possible."
  )
  object UnavailableAcsSnapshot
      extends ErrorCode(
        id = "UNAVAILABLE_ACS_SNAPSHOT",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(
        requestedTimestamp: CantonTimestamp,
        prunedTimestamp: CantonTimestamp,
        domainId: DomainId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The participant does not support serving an ACS snapshot at the requested timestamp $requestedTimestamp on domain $domainId"
        )
        with RepairServiceError
  }

  @Explanation(
    "The ACS snapshot cannot be returned because it contains inconsistencies. This is likely due to the request happening concurrently with pruning."
  )
  @Resolution(
    "Retry the operation"
  )
  object InconsistentAcsSnapshot
      extends ErrorCode(
        id = "INCONSISTENT_ACS_SNAPSHOT",
        ErrorCategory.TransientServerFailure,
      ) {
    final case class Error(domainId: DomainId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The ACS snapshot for $domainId cannot be returned because it contains inconsistencies."
        )
        with RepairServiceError
  }

  @Explanation(
    "A contract cannot be serialized due to an error."
  )
  @Resolution(
    "Retry after operator intervention."
  )
  object SerializationError
      extends ErrorCode(
        id = "CONTRACT_SERIALIZATION_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Error(domainId: DomainId, contractId: LfContractId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Serialization for contract $contractId in domain $domainId failed"
        )
        with RepairServiceError
  }

  object InvalidArgument
      extends ErrorCode(
        id = "INVALID_ARGUMENT_REPAIR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with RepairServiceError
  }

  @Explanation(
    "A contract cannot be purged due to an error."
  )
  @Resolution(
    "Retry after operator intervention."
  )
  object ContractPurgeError
      extends ErrorCode(
        id = "CONTRACT_PURGE_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(domain: DomainAlias, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = reason)
        with RepairServiceError
  }

  @Explanation(
    "Import Acs has failed."
  )
  @Resolution(
    "Retry after operator intervention."
  )
  object ImportAcsError
      extends ErrorCode(
        id = "IMPORT_ACS_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = reason)
        with RepairServiceError
  }

}
