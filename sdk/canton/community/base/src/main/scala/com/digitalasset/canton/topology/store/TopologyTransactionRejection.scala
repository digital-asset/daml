// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.config.CantonRequireTypes.String256M
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
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
  def asString1GB: String256M =
    String256M.tryCreate(asString, Some("topology transaction rejection"))

  def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError

  override def pretty: Pretty[this.type] = prettyOfString(_ => asString)
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

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) =
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
      s"Participant ${participant} onboarding rejected as restrictions ${restriction} are in place."

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) = {
      TopologyManagerError.ParticipantOnboardingRefused.Reject(participant, restriction)
    }
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
    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) =
      TopologyManagerError.InvalidTopologyMapping.Reject(err)
  }

  final case class SignatureCheckFailed(err: SignatureCheckError)
      extends TopologyTransactionRejection {
    override def asString: String = err.toString
    override def pretty: Pretty[SignatureCheckFailed] = prettyOfClass(param("err", _.err))

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) =
      TopologyManagerError.InvalidSignatureError.Failure(err)
  }
  final case class InvalidDomain(domain: DomainId) extends TopologyTransactionRejection {
    override def asString: String = show"Invalid domain $domain"
    override def pretty: Pretty[InvalidDomain] = prettyOfClass(param("domain", _.domain))

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) =
      TopologyManagerError.InvalidDomain.Failure(domain)
  }
  final case class Duplicate(old: CantonTimestamp) extends TopologyTransactionRejection {
    override def asString: String = show"Duplicate transaction from ${old}"
    override def pretty: Pretty[Duplicate] = prettyOfClass(param("old", _.old))
    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) =
      TopologyManagerError.DuplicateTransaction.ExistsAt(old)
  }
  final case class SerialMismatch(expected: PositiveInt, actual: PositiveInt)
      extends TopologyTransactionRejection {
    override def asString: String =
      show"The given serial $actual does not match the expected serial $expected"
    override def pretty: Pretty[SerialMismatch] =
      prettyOfClass(param("expected", _.expected), param("actual", _.actual))
    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) =
      TopologyManagerError.SerialMismatch.Failure(expected, actual)
  }
  final case class Other(str: String) extends TopologyTransactionRejection {
    override def asString: String = str
    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) =
      TopologyManagerError.InternalError.Other(str)
  }

  final case class ExtraTrafficLimitTooLow(
      member: Member,
      actual: PositiveLong,
      expectedMinimum: PositiveLong,
  ) extends TopologyTransactionRejection {
    override def asString: String =
      s"Extra traffic limit for $member should be at least $expectedMinimum, but was $actual."

    override def pretty: Pretty[ExtraTrafficLimitTooLow] =
      prettyOfClass(
        param("member", _.member),
        param("actual", _.actual),
        param("expectedMinimum", _.expectedMinimum),
      )

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.InvalidTrafficLimit.TrafficLimitTooLow(member, actual, expectedMinimum)
  }

  final case class InsufficientKeys(members: Seq[Member]) extends TopologyTransactionRejection {
    override def asString: String =
      s"Members ${members.sorted.mkString(", ")} are missing a signing key or an encryption key or both."

    override def pretty: Pretty[InsufficientKeys] = prettyOfClass(
      param("members", _.members)
    )

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.InsufficientKeys.Failure(members)
  }

  final case class UnknownMembers(members: Seq[Member]) extends TopologyTransactionRejection {
    override def asString: String = s"Members ${members.toSeq.sorted.mkString(", ")} are unknown."

    override def pretty: Pretty[UnknownMembers] = prettyOfClass(param("members", _.members))

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.UnknownMembers.Failure(members)
  }

  final case class ParticipantStillHostsParties(participantId: ParticipantId, parties: Seq[PartyId])
      extends TopologyTransactionRejection {
    override def asString: String =
      s"Cannot remove domain trust certificate for $participantId because it still hosts parties ${parties
          .mkString(",")}"

    override def pretty: Pretty[ParticipantStillHostsParties] =
      prettyOfClass(param("participantId", _.participantId), param("parties", _.parties))

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.IllegalRemovalOfDomainTrustCertificate.ParticipantStillHostsParties(
        participantId,
        parties,
      )
  }

  final case class PartyExceedsHostingLimit(
      partyId: PartyId,
      limit: Int,
      numParticipants: Int,
  ) extends TopologyTransactionRejection {
    override def asString: String =
      s"Party $partyId exceeds hosting limit of $limit with desired number of $numParticipants hosting participants."

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.PartyExceedsHostingLimit.Reject(partyId, limit, numParticipants)

    override def pretty: Pretty[PartyExceedsHostingLimit.this.type] =
      prettyOfClass(
        param("partyId", _.partyId),
        param("limit", _.limit),
        param("number of hosting participants", _.numParticipants),
      )
  }

  final case class MissingMappings(missing: Map[Member, Seq[TopologyMapping.Code]])
      extends TopologyTransactionRejection {
    override def asString: String = {
      val mappingString = missing.toSeq
        .sortBy(_._1)
        .map { case (member, mappings) =>
          s"$member: ${mappings.mkString(", ")}"
        }
        .mkString("; ")
      s"The following members are missing certain topology mappings: $mappingString"
    }

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.MissingTopologyMapping.Reject(missing)
  }

  final case class MissingDomainParameters(effective: EffectiveTime)
      extends TopologyTransactionRejection {
    override def asString: String = s"Missing domain parameters at $effective"

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.MissingTopologyMapping.MissingDomainParameters(effective)
  }

  final case class NamespaceAlreadyInUse(namespace: Namespace)
      extends TopologyTransactionRejection {
    override def asString: String = s"The namespace $namespace is already used by another entity."

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.NamespaceAlreadyInUse.Reject(namespace)

    override def pretty: Pretty[NamespaceAlreadyInUse.this.type] = prettyOfClass(
      param("namespace", _.namespace)
    )
  }

  final case class PartyIdIsAdminParty(partyId: PartyId) extends TopologyTransactionRejection {
    override def asString: String =
      s"The partyId $partyId is the same as an already existing admin party."

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.PartyIdIsAdminParty.Reject(partyId)

    override def pretty: Pretty[PartyIdIsAdminParty.this.type] = prettyOfClass(
      param("partyId", _.partyId)
    )
  }

  final case class ParticipantIdClashesWithPartyId(participantId: ParticipantId, partyId: PartyId)
      extends TopologyTransactionRejection {
    override def asString: String = ???

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.ParticipantIdClashesWithPartyId.Reject(participantId, partyId)

    override def pretty: Pretty[ParticipantIdClashesWithPartyId.this.type] =
      prettyOfClass(
        param("participantId", _.participantId),
        param("partyId", _.partyId),
      )
  }
}
