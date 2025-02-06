// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{Fingerprint, SignatureCheckError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.OnboardingRestriction
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.transaction.TopologyMapping

sealed trait TopologyTransactionRejection extends PrettyPrinting with Product with Serializable {
  def asString: String
  def asString300: String300 =
    String300.tryCreate(asString.take(300), Some("topology transaction rejection"))

  def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError

  override protected def pretty: Pretty[this.type] = prettyOfString(_ => asString)
}
object TopologyTransactionRejection {

  final case class NoDelegationFoundForKeys(keys: Set[Fingerprint])
      extends TopologyTransactionRejection {
    override def asString: String = s"No delegation found for keys ${keys.mkString(", ")}"

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.UnauthorizedTransaction.Failure(asString)

  }
  case object NotAuthorized extends TopologyTransactionRejection {
    override def asString: String = "Not authorized"

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.UnauthorizedTransaction.Failure(asString)
  }

  final case class UnknownParties(parties: Seq[PartyId]) extends TopologyTransactionRejection {
    override def asString: String = s"Parties ${parties.sorted.mkString(", ")} are unknown."

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.UnknownParties.Failure(parties)
  }

  final case class OnboardingRestrictionInPlace(
      participant: ParticipantId,
      restriction: OnboardingRestriction,
      loginAfter: Option[CantonTimestamp],
  ) extends TopologyTransactionRejection {
    override def asString: String =
      s"Participant $participant onboarding rejected as restrictions $restriction are in place."

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.ParticipantOnboardingRefused.Reject(participant, restriction)
  }

  final case class NoCorrespondingActiveTxToRevoke(mapping: TopologyMapping)
      extends TopologyTransactionRejection {
    override def asString: String =
      s"There is no active topology transaction matching the mapping of the revocation request: $mapping"
    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.NoCorrespondingActiveTxToRevoke.Mapping(mapping)
  }

  final case class InvalidTopologyMapping(err: String) extends TopologyTransactionRejection {
    override def asString: String = s"Invalid mapping: $err"
    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.InvalidTopologyMapping.Reject(err)
  }

  final case class CannotRemoveMapping(mappingCode: TopologyMapping.Code)
      extends TopologyTransactionRejection {
    override def asString: String =
      s"Removal of $mappingCode is not supported. Use Replace instead."
    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.CannotRemoveMapping.Reject(mappingCode)
  }

  final case class RemoveMustNotChangeMapping(actual: TopologyMapping, expected: TopologyMapping)
      extends TopologyTransactionRejection {
    override def asString: String = "Remove operation must not change the mapping to remove."

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.RemoveMustNotChangeMapping.Reject(actual, expected)
  }

  final case class SignatureCheckFailed(err: SignatureCheckError)
      extends TopologyTransactionRejection {
    override def asString: String = err.toString
    override protected def pretty: Pretty[SignatureCheckFailed] = prettyOfClass(param("err", _.err))

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.InvalidSignatureError.Failure(err)
  }
  final case class InvalidSynchronizer(synchronizerId: SynchronizerId)
      extends TopologyTransactionRejection {
    override def asString: String = show"Invalid synchronizer $synchronizerId"
    override protected def pretty: Pretty[InvalidSynchronizer] = prettyOfClass(
      param("synchronizer", _.synchronizerId)
    )

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.InvalidSynchronizer.Failure(synchronizerId)
  }
  final case class SerialMismatch(expected: PositiveInt, actual: PositiveInt)
      extends TopologyTransactionRejection {
    override def asString: String =
      show"The given serial $actual does not match the expected serial $expected"
    override protected def pretty: Pretty[SerialMismatch] =
      prettyOfClass(param("expected", _.expected), param("actual", _.actual))
    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.SerialMismatch.Failure(expected, actual)
  }
  final case class AssumptionViolation(str: String) extends TopologyTransactionRejection {
    override def asString: String = str
    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.InternalError.AssumptionViolation(str)
  }

  final case class InsufficientKeys(members: Seq[Member]) extends TopologyTransactionRejection {
    override def asString: String =
      s"Members ${members.sorted.mkString(", ")} are missing a signing key or an encryption key or both."

    override protected def pretty: Pretty[InsufficientKeys] = prettyOfClass(
      param("members", _.members)
    )

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.InsufficientKeys.Failure(members)
  }

  final case class UnknownMembers(members: Seq[Member]) extends TopologyTransactionRejection {
    override def asString: String = s"Members ${members.sorted.mkString(", ")} are unknown."

    override protected def pretty: Pretty[UnknownMembers] = prettyOfClass(
      param("members", _.members)
    )

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.UnknownMembers.Failure(members)
  }

  final case class ParticipantStillHostsParties(participantId: ParticipantId, parties: Seq[PartyId])
      extends TopologyTransactionRejection {
    override def asString: String =
      s"Cannot remove synchronizer trust certificate for $participantId because it still hosts parties ${parties
          .mkString(",")}"

    override protected def pretty: Pretty[ParticipantStillHostsParties] =
      prettyOfClass(param("participantId", _.participantId), param("parties", _.parties))

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.IllegalRemovalOfSynchronizerTrustCertificate
        .ParticipantStillHostsParties(
          participantId,
          parties,
        )
  }

  final case class MissingSynchronizerParameters(effective: EffectiveTime)
      extends TopologyTransactionRejection {
    override def asString: String = s"Missing synchronizer parameters at $effective"

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.MissingTopologyMapping.MissingSynchronizerParameters(effective)
  }

  final case class NamespaceAlreadyInUse(namespace: Namespace)
      extends TopologyTransactionRejection {
    override def asString: String = s"The namespace $namespace is already used by another entity."

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.NamespaceAlreadyInUse.Reject(namespace)

    override protected def pretty: Pretty[NamespaceAlreadyInUse.this.type] = prettyOfClass(
      param("namespace", _.namespace)
    )
  }

  final case class PartyIdConflictWithAdminParty(partyId: PartyId)
      extends TopologyTransactionRejection {
    override def asString: String =
      s"The partyId $partyId is the same as an already existing admin party."

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.PartyIdConflictWithAdminParty.Reject(partyId)

    override protected def pretty: Pretty[PartyIdConflictWithAdminParty.this.type] = prettyOfClass(
      param("partyId", _.partyId)
    )
  }

  final case class ParticipantIdConflictWithPartyId(participantId: ParticipantId, partyId: PartyId)
      extends TopologyTransactionRejection {
    override def asString: String =
      s"Tried to onboard participant $participantId while party $partyId with the same UID already exists."

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.ParticipantIdConflictWithPartyId.Reject(participantId, partyId)

    override protected def pretty: Pretty[ParticipantIdConflictWithPartyId.this.type] =
      prettyOfClass(
        param("participantId", _.participantId),
        param("partyId", _.partyId),
      )
  }

  final case class MediatorsAlreadyInOtherGroups(
      group: NonNegativeInt,
      mediators: Map[MediatorId, NonNegativeInt],
  ) extends TopologyTransactionRejection {
    override def asString: String =
      s"Tried to add mediators to group $group, but they are already assigned to other groups: ${mediators.toSeq
          .sortBy(_._1.toProtoPrimitive)
          .mkString(", ")}"

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.MediatorsAlreadyInOtherGroups.Reject(group, mediators)
  }

  final case class MembersCannotRejoinSynchronizer(members: Seq[Member])
      extends TopologyTransactionRejection {
    override def asString: String =
      s"Member ${members.sorted} tried to rejoin a synchronizer from which they had previously left."

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.MemberCannotRejoinSynchronizer.Reject(members)
  }
}
