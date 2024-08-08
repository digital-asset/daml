// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.error.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantErrorGroup
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.Ref.PackageId

trait ParticipantTopologyManagerOps {
  def allocateParty(
      validatedSubmissionId: String255,
      partyId: PartyId,
      participantId: ParticipantId,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]

}

sealed trait ParticipantTopologyManagerError extends CantonError
object ParticipantTopologyManagerError extends ParticipantErrorGroup {

  final case class IdentityManagerParentError(parent: TopologyManagerError)(implicit
      val loggingContext: ErrorLoggingContext
  ) extends ParticipantTopologyManagerError
      with ParentCantonError[TopologyManagerError] {
    override def logOnCreation: Boolean = false
  }

  @Explanation(
    """This error indicates a vetting request failed due to dependencies not being vetted.
      |On every vetting request, the set supplied packages is analysed for dependencies. The
      |system requires that not only the main packages are vetted explicitly but also all dependencies.
      |This is necessary as not all participants are required to have the same packages installed and therefore
      |not every participant can resolve the dependencies implicitly."""
  )
  @Resolution("Vet the dependencies first and then repeat your attempt.")
  object DependenciesNotVetted
      extends ErrorCode(
        id = "DEPENDENCIES_NOT_VETTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(unvetted: Set[PackageId])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Package vetting failed due to dependencies not being vetted"
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates that a request involving topology management was attempted on a participant that is not yet initialised.
      |During initialisation, only namespace and identifier delegations can be managed."""
  )
  @Resolution("Initialise the participant and retry.")
  object UninitializedParticipant
      extends ErrorCode(
        id = "UNINITIALIZED_PARTICIPANT",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(_cause: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = _cause
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates that a dangerous PartyToParticipant mapping deletion was rejected.
      |If the command is run and there are active contracts where the party is a stakeholder these contracts
      |will become inoperable and will never get pruned, leaking storage.
      | """
  )
  @Resolution("Set force=true if you really know what you are doing.")
  object DisablePartyWithActiveContractsRequiresForce
      extends ErrorCode(
        id = "DISABLE_PARTY_WITH_ACTIVE_CONTRACTS_REQUIRES_FORCE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(partyId: PartyId)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            show"Disable party $partyId failed because there are active contracts where the party is a stakeholder"
        )
        with ParticipantTopologyManagerError
  }

}
