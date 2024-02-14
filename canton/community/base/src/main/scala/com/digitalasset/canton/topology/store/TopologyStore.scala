// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedString,
  String255,
  String256M,
}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.crypto.{Fingerprint, SignatureCheckError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.protocol.OnboardingRestriction
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.db.DbPartyMetadataStore
import com.digitalasset.canton.topology.store.memory.InMemoryPartyMetadataStore
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

// TODO(#15161): Rename file to PartyMetadata or split up helpers into multiple files
/** the party metadata used to inform the ledger api server
  *
  * the first class parameters correspond to the relevant information, whereas the
  * second class parameters are synchronisation information used during crash recovery.
  * we don't want these in an equality comparison.
  */
final case class PartyMetadata(
    partyId: PartyId,
    displayName: Option[DisplayName],
    participantId: Option[ParticipantId],
)(
    val effectiveTimestamp: CantonTimestamp,
    val submissionId: String255,
    val notified: Boolean = false,
)

trait PartyMetadataStore extends AutoCloseable {

  def metadataForParty(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): Future[Option[PartyMetadata]]

  final def insertOrUpdatePartyMetadata(metadata: PartyMetadata)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    insertOrUpdatePartyMetadata(
      partyId = metadata.partyId,
      participantId = metadata.participantId,
      displayName = metadata.displayName,
      effectiveTimestamp = metadata.effectiveTimestamp,
      submissionId = metadata.submissionId,
    )
  }

  def insertOrUpdatePartyMetadata(
      partyId: PartyId,
      participantId: Option[ParticipantId],
      displayName: Option[DisplayName],
      effectiveTimestamp: CantonTimestamp,
      submissionId: String255,
  )(implicit traceContext: TraceContext): Future[Unit]

  /** mark the given metadata as having been successfully forwarded to the domain */
  def markNotified(metadata: PartyMetadata)(implicit traceContext: TraceContext): Future[Unit]

  /** fetch the current set of party data which still needs to be notified */
  def fetchNotNotified()(implicit traceContext: TraceContext): Future[Seq[PartyMetadata]]

}

object PartyMetadataStore {

  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): PartyMetadataStore =
    storage match {
      case _: MemoryStorage => new InMemoryPartyMetadataStore()
      case jdbc: DbStorage => new DbPartyMetadataStore(jdbc, timeouts, loggerFactory)
    }

}

sealed trait TopologyStoreId extends PrettyPrinting {
  def filterName: String = dbString.unwrap
  def dbString: LengthLimitedString
  def dbStringWithDaml2xUniquifier(uniquifier: String): LengthLimitedString
  def isAuthorizedStore: Boolean
  def isDomainStore: Boolean
}

object TopologyStoreId {

  /** A topology store storing sequenced topology transactions
    *
    * @param domainId the domain id of the store
    * @param discriminator the discriminator of the store. used for mediator request store
    *                      or in daml 2.x for embedded mediator topology stores
    */
  final case class DomainStore(domainId: DomainId, discriminator: String = "")
      extends TopologyStoreId {
    private val dbStringWithoutDiscriminator = domainId.toLengthLimitedString
    val dbString: LengthLimitedString = {
      if (discriminator.isEmpty) dbStringWithoutDiscriminator
      else
        LengthLimitedString
          .tryCreate(discriminator + "::", discriminator.length + 2)
          .tryConcatenate(dbStringWithoutDiscriminator)
    }

    override def pretty: Pretty[this.type] = {
      if (discriminator.nonEmpty) {
        prettyOfString(storeId =>
          show"${storeId.discriminator}${SafeSimpleString.delimiter}${storeId.domainId}"
        )
      } else {
        prettyOfParam(_.domainId)
      }
    }

    // The reason for this somewhat awkward method is backward compat with uniquifier inserted in the middle of
    // discriminator and domain id. Can be removed once fully on daml 3.0:
    override def dbStringWithDaml2xUniquifier(uniquifier: String): LengthLimitedString = {
      require(uniquifier.nonEmpty)
      LengthLimitedString
        .tryCreate(discriminator + uniquifier + "::", discriminator.length + uniquifier.length + 2)
        .tryConcatenate(dbStringWithoutDiscriminator)
    }

    override def isAuthorizedStore: Boolean = false
    override def isDomainStore: Boolean = true
  }

  // authorized transactions (the topology managers store)
  type AuthorizedStore = AuthorizedStore.type
  object AuthorizedStore extends TopologyStoreId {
    val dbString = String255.tryCreate("Authorized")
    override def dbStringWithDaml2xUniquifier(uniquifier: String): LengthLimitedString = {
      require(uniquifier.nonEmpty)
      LengthLimitedString
        .tryCreate(uniquifier + "::", uniquifier.length + 2)
        .tryConcatenate(dbString)
    }

    override def pretty: Pretty[AuthorizedStore.this.type] = prettyOfString(
      _.dbString.unwrap
    )

    override def isAuthorizedStore: Boolean = true
    override def isDomainStore: Boolean = false
  }

  def apply(fName: String): TopologyStoreId = fName match {
    case "Authorized" => AuthorizedStore
    case domain => DomainStore(DomainId(UniqueIdentifier.tryFromProtoPrimitive(domain)))
  }

  trait IdTypeChecker[A <: TopologyStoreId] {
    def isOfType(id: TopologyStoreId): Boolean
  }

  implicit val domainTypeChecker: IdTypeChecker[DomainStore] = new IdTypeChecker[DomainStore] {
    override def isOfType(id: TopologyStoreId): Boolean = id.isDomainStore
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectX[StoreId <: TopologyStoreId](store: TopologyStoreX[TopologyStoreId])(implicit
      checker: IdTypeChecker[StoreId]
  ): Option[TopologyStoreX[StoreId]] = if (checker.isOfType(store.storeId))
    Some(store.asInstanceOf[TopologyStoreX[StoreId]])
  else None

}

sealed trait TopologyTransactionRejection extends PrettyPrinting with Product with Serializable {
  def asString: String
  def asString1GB: String256M =
    String256M.tryCreate(asString, Some("topology transaction rejection"))

  def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError
}
object TopologyTransactionRejection {

  final case class NoDelegationFoundForKeys(keys: Set[Fingerprint])
      extends TopologyTransactionRejection {
    override def asString: String = s"No delegation found for keys ${keys.mkString(", ")}"
    override def pretty: Pretty[NoDelegationFoundForKeys] = prettyOfString(_ => asString)

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext): TopologyManagerError =
      TopologyManagerError.UnauthorizedTransaction.Failure(asString)

  }
  case object NotAuthorized extends TopologyTransactionRejection {
    override def asString: String = "Not authorized"
    override def pretty: Pretty[NotAuthorized.type] = prettyOfString(_ => asString)

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) =
      TopologyManagerError.UnauthorizedTransaction.Failure(asString)
  }

  final case class ThresholdTooHigh(actual: Int, mustBeAtMost: Int)
      extends TopologyTransactionRejection {
    override def asString: String =
      s"Threshold must not be higher than $mustBeAtMost, but was $actual."

    override def pretty: Pretty[ThresholdTooHigh] = prettyOfString(_ => asString)

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) = {
      TopologyManagerError.InvalidThreshold.ThresholdTooHigh(actual, mustBeAtMost)
    }
  }

  final case class OnboardingRestrictionInPlace(
      participant: ParticipantId,
      restriction: OnboardingRestriction,
      loginAfter: Option[CantonTimestamp],
  ) extends TopologyTransactionRejection {
    override def asString: String =
      s"Participant ${participant} onboarding rejected as restrictions ${restriction} are in place."

    override def pretty: Pretty[OnboardingRestrictionInPlace] = prettyOfString(_ => asString)

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) = {
      TopologyManagerError.ParticipantOnboardingRefused.Reject(participant, restriction)
    }
  }

  final case class SignatureCheckFailed(err: SignatureCheckError)
      extends TopologyTransactionRejection {
    override def asString: String = err.toString
    override def pretty: Pretty[SignatureCheckFailed] = prettyOfClass(param("err", _.err))

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) =
      TopologyManagerError.InvalidSignatureError.Failure(err)
  }
  final case class WrongDomain(wrong: DomainId) extends TopologyTransactionRejection {
    override def asString: String = show"Wrong domain $wrong"
    override def pretty: Pretty[WrongDomain] = prettyOfClass(param("wrong", _.wrong))

    override def toTopologyManagerError(implicit elc: ErrorLoggingContext) =
      TopologyManagerError.WrongDomain.Failure(wrong)
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
    override def pretty: Pretty[Other] = prettyOfString(_ => asString)

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
}
