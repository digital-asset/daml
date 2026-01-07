// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{Fingerprint, PublicKey, SignatureCheckError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.OnboardingRestriction
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.transaction.TopologyMapping
import com.digitalasset.canton.topology.transaction.TopologyMapping.ReferencedAuthorizations
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  PositiveTopologyTransaction,
  TxHash,
}

sealed trait TopologyTransactionRejection extends PrettyPrinting with Product with Serializable {
  def asString: String
  def asString300: String300 =
    String300.tryCreate(asString.take(300), Some("topology transaction rejection"))

  def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError

  override protected def pretty: Pretty[this.type] = prettyOfString(_ => asString)
}
object TopologyTransactionRejection {

  /** list of rejections produced by state processor */
  object Processor {
    final case class SerialMismatch(actual: PositiveInt, expected: PositiveInt)
        extends TopologyTransactionRejection {
      override def asString: String =
        show"The given serial $expected does not match the actual serial $expected"
      override protected def pretty: Pretty[SerialMismatch] =
        prettyOfClass(param("expected", _.expected), param("actual", _.actual))
      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.SerialMismatch.Failure(Some(actual), Some(expected))
    }
    final case class InternalError(message: String) extends TopologyTransactionRejection {
      override def asString: String = message
      override protected def pretty: Pretty[InternalError] =
        prettyOfClass(param("message", _.message.singleQuoted))
      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.InternalError.Unexpected(message)
    }
  }

  /** list of rejections which are created due to authorization checks */
  object Authorization {

    /** Internal error, indicating a violation of the invariant that a signed transaction needs at
      * least one signature
      */
    case object NoSignatureProvided extends TopologyTransactionRejection {
      override def asString: String = "No signature provided to authentication check"

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.InternalError.Unexpected(asString)
    }

    case object NotAuthorizedByNamespaceKey extends TopologyTransactionRejection {
      override def asString: String = "Not authorized by any namespace key"

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.UnauthorizedTransaction.NoNamespaceAuth()
    }

    final case class NotFullyAuthorized(missing: ReferencedAuthorizations)
        extends TopologyTransactionRejection {
      override def asString: String = "Not fully authorized"

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.UnauthorizedTransaction.Missing(missing)
    }

    final case class NoDelegationFoundForKeys(keys: Set[Fingerprint])
        extends TopologyTransactionRejection {
      override def asString: String = s"No delegation found for keys ${keys.mkString(", ")}"

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.UnauthorizedTransaction.NoDelegation(keys)

    }
    final case class MultiTransactionHashMismatch(
        expected: TxHash,
        actual: NonEmpty[Set[TxHash]],
    ) extends TopologyTransactionRejection {
      override def asString: String =
        s"The given transaction hash set $actual did not contain the expected hash $expected of the transaction."
      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.MultiTransactionHashMismatch.Failure(expected, actual)
    }
    final case class SignatureCheckFailed(err: SignatureCheckError)
        extends TopologyTransactionRejection {
      override def asString: String = err.toString
      override protected def pretty: Pretty[SignatureCheckFailed] = prettyOfClass(
        param("err", _.err)
      )
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

  }

  /** list of rejections which are created due to invariants on the topology state */
  object RequiredMapping {

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

    final case class InvalidOwnerToKeyMapping(
        member: Member,
        keyType: String,
        provided: Seq[PublicKey],
        supported: Seq[String],
    ) extends TopologyTransactionRejection {
      override def asString: String = if (provided.isEmpty)
        s"No $keyType keys provided for $member, at least one is required. Supported key schemes: ${supported
            .mkString(",")}"
      else
        s"None of the ${provided.size} $keyType keys for $member supports the required key schemes (${supported
            .mkString(",")}): ${provided.mkString(",")}"

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.InvalidOwnerToKeyMapping.Reject(member, keyType, provided, supported)
    }

    final case class InvalidOwnerToKeyMappingRemoval(
        member: Member,
        inUseBy: PositiveTopologyTransaction,
    ) extends TopologyTransactionRejection {
      override def asString: String =
        s"Cannot remove owner to key mapping for $member as it is being used by $inUseBy"
      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.InvalidOwnerToKeyMappingRemoval.Reject(member, inUseBy)
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

    final case class InsufficientKeys(members: Seq[Member]) extends TopologyTransactionRejection {
      override def asString: String =
        s"Members ${members.sorted.mkString(", ")} are missing a valid owner to key mapping."

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

    final case class MissingSynchronizerParameters(effective: EffectiveTime)
        extends TopologyTransactionRejection {
      override def asString: String = s"Missing synchronizer parameters at $effective"

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.MissingTopologyMapping.MissingSynchronizerParameters(effective)
    }

    final case class NamespaceHasBeenRevoked(namespace: Namespace)
        extends TopologyTransactionRejection {
      override def asString: String = s"The namespace $namespace has previously been revoked."

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.NamespaceHasBeenRevoked.Reject(namespace)

      override protected def pretty: Pretty[NamespaceHasBeenRevoked.this.type] = prettyOfClass(
        param("namespace", _.namespace)
      )
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

      override protected def pretty: Pretty[PartyIdConflictWithAdminParty.this.type] =
        prettyOfClass(
          param("partyId", _.partyId)
        )
    }

    final case class ParticipantIdConflictWithPartyId(
        participantId: ParticipantId,
        partyId: PartyId,
    ) extends TopologyTransactionRejection {
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

    final case class OngoingSynchronizerUpgrade(synchronizerId: SynchronizerId)
        extends TopologyTransactionRejection {
      override def asString: String =
        s"The topology state of synchronizer $synchronizerId is frozen due to an ongoing synchronizer migration and no more topology changes are allowed."

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.OngoingSynchronizerUpgrade.Reject(synchronizerId)
    }

    final case class InvalidSynchronizerSuccessor(
        currentSynchronizerId: PhysicalSynchronizerId,
        successorSynchronizerId: PhysicalSynchronizerId,
    ) extends TopologyTransactionRejection {
      override def asString: String =
        s"The declared successor $successorSynchronizerId of synchronizer $currentSynchronizerId is not valid."

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.InvalidSynchronizerSuccessor.Reject.conflictWithCurrentPSId(
          currentSynchronizerId,
          successorSynchronizerId,
        )
    }

    final case class InvalidUpgradeTime(
        synchronizerId: SynchronizerId,
        effective: EffectiveTime,
        upgradeTime: CantonTimestamp,
    ) extends TopologyTransactionRejection {
      override def asString: String =
        s"The upgrade time $upgradeTime must be after the effective ${effective.value} of the synchronizer upgrade announcement for synchronizer $synchronizerId."

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.InvalidUpgradeTime.Reject(synchronizerId, effective, upgradeTime)
    }

    final case class ParticipantCannotRejoinSynchronizer(participantId: ParticipantId)
        extends TopologyTransactionRejection {
      override def asString: String =
        s"Participant $participantId tried to rejoin a synchronizer which it had previously left."

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.MemberCannotRejoinSynchronizer.Reject(Seq(participantId))
    }

    final case class CannotReregisterKeys(member: Member) extends TopologyTransactionRejection {
      override def asString: String =
        s"Member $member tried to re-register its keys which they have previously removed."

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.MemberCannotRejoinSynchronizer.RejectNewKeys(member)
    }

  }

  /** list of rejections which are created to help operators avoiding mistakes */
  object OptionalMapping {

    final case class ParticipantStillHostsParties(
        participantId: ParticipantId,
        parties: Seq[PartyId],
    ) extends TopologyTransactionRejection {
      override def asString: String =
        s"Cannot remove synchronizer trust certificate or owner to key mapping for $participantId because it still hosts parties ${parties
            .mkString(",")}"

      override protected def pretty: Pretty[ParticipantStillHostsParties] =
        prettyOfClass(param("participantId", _.participantId), param("parties", _.parties))

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.IllegalRemovalOfActiveTopologyTransactions
          .ParticipantStillHostsParties(
            participantId,
            parties,
          )
    }

    final case class MembersCannotRejoinSynchronizer(members: Seq[Member])
        extends TopologyTransactionRejection {
      override def asString: String =
        s"Member ${members.sorted} tried to rejoin a synchronizer from which they had previously left."

      override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
        TopologyManagerError.MemberCannotRejoinSynchronizer.Reject(members)
    }

  }

}
