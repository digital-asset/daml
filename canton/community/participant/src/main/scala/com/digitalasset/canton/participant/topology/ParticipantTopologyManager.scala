// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.error.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.TopologyManagerError.ParticipantErrorGroup
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion

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
    """This error indicates that a package vetting command failed due to packages not existing locally.
      |This can be due to either the packages not being present or their dependencies being missing.
      |When vetting a package, the package must exist on the participant, as otherwise the participant
      |will not be able to process a transaction relying on a particular package."""
  )
  @Resolution(
    "Ensure that the package exists locally before issuing such a transaction."
  )
  object CannotVetDueToMissingPackages
      extends ErrorCode(
        id = "CANNOT_VET_DUE_TO_MISSING_PACKAGES",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
      ) {
    final case class Missing(packages: PackageId)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Package vetting failed due to packages not existing on the local node"
        )
        with ParticipantTopologyManagerError
  }

  @Explanation(
    """This error indicates that a dangerous package vetting command was rejected.
      |This is the case if a vetting command, if not run correctly, could potentially lead to a ledger fork.
      |The vetting authorization checks the participant for the presence of the given set of
      |packages (including their dependencies) and allows only to vet for the given participant id.
      |In rare cases where a more centralised topology manager is used, this behaviour can be overridden
      |with force. However, if a package is vetted but not present on the participant, the participant will
      |refuse to process any transaction of the given domain until the problematic package has been uploaded."""
  )
  @Resolution("Set force=true if you really know what you are doing.")
  object DangerousVettingCommandsRequireForce
      extends ErrorCode(
        id = "DANGEROUS_VETTING_COMMANDS_REQUIRE_FORCE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "Package vetting failed due to packages not existing on the local node"
        )
        with ParticipantTopologyManagerError
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
    """This error indicates that a dangerous owner to key mapping authorization was rejected.
      |This is the case if a command is run that could break a participant.
      |If the command was run to assign a key for the given participant, then the command
      |was rejected because the key is not in the participants private store.
      |If the command is run on a participant to issue transactions for another participant,
      |then such commands must be run with force, as they are very dangerous and could easily break
      |the participant.
      |As an example, if we assign an encryption key to a participant that the participant does not
      |have, then the participant will be unable to process an incoming transaction. Therefore we must
      |be very careful to not create such situations.
      | """
  )
  @Resolution("Set force=true if you really know what you are doing.")
  object DangerousKeyUseCommandRequiresForce
      extends ErrorCode(
        id = "DANGEROUS_KEY_USE_COMMAND_REQUIRES_FORCE",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class AlienParticipant(participant: ParticipantId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Issuing owner to key mappings for alien participants requires force=yes"
        )
        with ParticipantTopologyManagerError
    final case class NoSuchKey(fingerprint: Fingerprint)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"Can not assign unknown key $fingerprint to this participant"
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
