// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedString,
  String255,
  String256M,
}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.crypto.{Fingerprint, PublicKey, SignatureCheckError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.OnboardingRestriction
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v30old as topoV30
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.processing.TransactionAuthorizationValidator.AuthorizationChain
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  SnapshotAuthorizationValidator,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.db.{DbPartyMetadataStore, DbTopologyStore}
import com.digitalasset.canton.topology.store.memory.{
  InMemoryPartyMetadataStore,
  InMemoryTopologyStore,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

final case class StoredTopologyTransaction[+Op <: TopologyChangeOp](
    sequenced: SequencedTime,
    validFrom: EffectiveTime,
    validUntil: Option[EffectiveTime],
    transaction: SignedTopologyTransaction[Op],
) extends PrettyPrinting {
  override def pretty: Pretty[StoredTopologyTransaction.this.type] =
    prettyOfClass(
      param("sequenced", _.sequenced.value),
      param("validFrom", _.validFrom.value),
      paramIfDefined("validUntil", _.validUntil.map(_.value)),
      param("op", _.transaction.transaction.op),
      param("mapping", _.transaction.transaction.element.mapping),
    )
}

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
  def select[StoreId <: TopologyStoreId](store: TopologyStore[TopologyStoreId])(implicit
      checker: IdTypeChecker[StoreId]
  ): Option[TopologyStore[StoreId]] = if (checker.isOfType(store.storeId))
    Some(store.asInstanceOf[TopologyStore[StoreId]])
  else None

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

final case class ValidatedTopologyTransaction(
    transaction: SignedTopologyTransaction[TopologyChangeOp],
    rejectionReason: Option[TopologyTransactionRejection],
)

object ValidatedTopologyTransaction {
  def valid(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  ): ValidatedTopologyTransaction =
    ValidatedTopologyTransaction(transaction, None)
}

sealed trait TimeQuery {
  def toProtoV30: topoV30.BaseQuery.TimeQuery
}

object TimeQuery {

  /** Determine the headstate.
    */
  object HeadState extends TimeQuery {
    override def toProtoV30: topoV30.BaseQuery.TimeQuery =
      topoV30.BaseQuery.TimeQuery.HeadState(com.google.protobuf.empty.Empty())
  }
  final case class Snapshot(asOf: CantonTimestamp) extends TimeQuery {
    override def toProtoV30: topoV30.BaseQuery.TimeQuery =
      topoV30.BaseQuery.TimeQuery.Snapshot(asOf.toProtoPrimitive)
  }
  final case class Range(from: Option[CantonTimestamp], until: Option[CantonTimestamp])
      extends TimeQuery {
    override def toProtoV30: topoV30.BaseQuery.TimeQuery = topoV30.BaseQuery.TimeQuery.Range(
      topoV30.BaseQuery.TimeRange(from.map(_.toProtoPrimitive), until.map(_.toProtoPrimitive))
    )
  }

  def fromProto(
      proto: topoV30.BaseQuery.TimeQuery,
      fieldName: String,
  ): ParsingResult[TimeQuery] =
    proto match {
      case topoV30.BaseQuery.TimeQuery.Empty =>
        Left(ProtoDeserializationError.FieldNotSet(fieldName))
      case topoV30.BaseQuery.TimeQuery.Snapshot(value) =>
        CantonTimestamp.fromProtoPrimitive(value).map(Snapshot)
      case topoV30.BaseQuery.TimeQuery.HeadState(_) => Right(HeadState)
      case topoV30.BaseQuery.TimeQuery.Range(value) =>
        for {
          fromO <- value.from.traverse(CantonTimestamp.fromProtoPrimitive)
          toO <- value.until.traverse(CantonTimestamp.fromProtoPrimitive)
        } yield Range(fromO, toO)
    }

}

trait TopologyStoreCommon[+StoreID <: TopologyStoreId, ValidTx, StoredTx, SignedTx]
    extends FlagCloseable {

  this: NamedLogging =>

  protected implicit def ec: ExecutionContext

  def storeId: StoreID

  /** fetch the effective time updates greater than or equal to a certain timestamp
    *
    * this function is used to recover the future effective timestamp such that we can reschedule "pokes" of the
    * topology client and updates of the acs commitment processor on startup
    */
  def findUpcomingEffectiveChanges(asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TopologyStore.Change]]

  def maxTimestamp()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]]

  /** returns the current dispatching watermark
    *
    * for topology transaction dispatching, we keep track up to which point in time
    * we have mirrored the authorized store to the remote store
    *
    * the timestamp always refers to the timestamp of the authorized store!
    */
  def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]]

  /** update the dispatching watermark for this target store */
  def updateDispatchingWatermark(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  protected def signedTxFromStoredTx(storedTx: StoredTx): SignedTx

  def providesAdditionalSignatures(transaction: SignedTx)(implicit
      traceContext: TraceContext
  ): Future[Boolean]
}

object TopologyStoreCommon {

  type DomainStoreCommon = TopologyStoreCommon[DomainStore, _, _, _]

}

abstract class TopologyStore[+StoreID <: TopologyStoreId](implicit
    ec: ExecutionContext
) extends AutoCloseable
    with TopologyStoreCommon[StoreID, ValidatedTopologyTransaction, StoredTopologyTransaction[
      TopologyChangeOp
    ], SignedTopologyTransaction[TopologyChangeOp]] {
  this: NamedLogging =>

  /** add validated topology transaction as is to the topology transaction table */
  def append(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      transactions: Seq[ValidatedTopologyTransaction],
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** returns transactions that should be dispatched to the domain */
  def findDispatchingTransactionsAfter(
      timestampExclusive: CantonTimestamp,
      limit: Option[Int] = None,
  )(implicit traceContext: TraceContext): Future[StoredTopologyTransactions[TopologyChangeOp]]

  /** returns initial set of onboarding transactions that should be dispatched to the domain */
  def findParticipantOnboardingTransactions(participantId: ParticipantId, domainId: DomainId)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[SignedTopologyTransaction[TopologyChangeOp]]]

  /** returns an descending ordered list of timestamps of when participant state changes occurred before a certain point in time */
  def findTsOfParticipantStateChangesBefore(
      beforeExclusive: CantonTimestamp,
      participantId: ParticipantId,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[CantonTimestamp]]

  /** Returns an ordered list of transactions from the transaction store within a certain range */
  def findTransactionsInRange(asOfExclusive: CantonTimestamp, upToExclusive: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]]

  def timestamp(useStateStore: Boolean = false)(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]]

  override def maxTimestamp()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] = timestamp(useStateStore = true)

  /** set of topology transactions which are active */
  def headTransactions(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp.Positive]]

  /** finds transactions in the local store that would remove the topology state elements */
  def findRemovalTransactionForMappings(
      mappings: Set[TopologyStateElement[TopologyMapping]]
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[SignedTopologyTransaction[TopologyChangeOp.Remove]]]

  def findPositiveTransactionsForMapping(mapping: TopologyMapping)(implicit
      traceContext: TraceContext
  ): Future[Seq[SignedTopologyTransaction[TopologyChangeOp.Positive]]]

  @VisibleForTesting
  def allTransactions(includeRejected: Boolean = false)(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]]

  def findStoredNoSignature(transaction: TopologyTransaction[TopologyChangeOp])(implicit
      traceContext: TraceContext
  ): Future[Seq[StoredTopologyTransaction[TopologyChangeOp]]]

  def findStoredForVersion(
      transaction: TopologyTransaction[TopologyChangeOp],
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransaction[TopologyChangeOp]]]

  /** Bootstrap a node state from a topology transaction collection */
  def bootstrap(
      collection: StoredTopologyTransactions[TopologyChangeOp.Positive]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val groupedBySequencedAndValidFrom = collection.result
      .groupBy(x => (x.sequenced, x.validFrom))
      .toList
      .sortBy { case ((_, validFrom), _) => validFrom }
    MonadUtil
      .sequentialTraverse_(groupedBySequencedAndValidFrom) {
        case ((sequenced, effective), transactions) =>
          val txs = transactions.map(tx => ValidatedTopologyTransaction(tx.transaction, None))
          for {
            _ <- append(sequenced, effective, txs)
            _ <- updateState(
              sequenced,
              effective,
              deactivate = Seq.empty,
              positive = transactions.map(_.transaction),
            )
          } yield ()
      }
  }

  /** returns the set of positive transactions
    *
    * this function is used by the topology processor to determine the set of transaction, such that
    * we can perform cascading updates if there was a certificate revocation
    *
    * @param asOfInclusive whether the search interval should include the current timepoint or not. the state at t is
    *                      defined as "exclusive" of t, whereas for updating the state, we need to be able to query inclusive.
    * @param includeSecondary some topology transactions have an "secondary" uid. currently, this only applies to the
    *                         party to participant mapping where the secondary uid is the participant uid.
    *                         we need this information during cascading updates of participant certificates.
    */
  def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit
      traceContext: TraceContext
  ): Future[PositiveStoredTopologyTransactions]

  /** query interface used by DomainTopologyManager to find the set of initial keys */
  def findInitialState(id: DomainTopologyManagerId)(implicit
      traceContext: TraceContext
  ): Future[Map[Member, Seq[PublicKey]]]

  /** update active topology transaction to the active topology transaction table
    *
    * active means that for the key authorizing the transaction, there is a connected path to reach the root certificate
    */
  def updateState(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      deactivate: Seq[UniquePath],
      positive: Seq[SignedTopologyTransaction[TopologyChangeOp.Positive]],
  )(implicit traceContext: TraceContext): Future[Unit]

  /** query optimized for inspection
    *
    * @param recentTimestampO if exists, use this timestamp for the head state to prevent race conditions on the console
    */
  def inspect(
      stateStore: Boolean,
      timeQuery: TimeQuery,
      recentTimestampO: Option[CantonTimestamp],
      ops: Option[TopologyChangeOp],
      typ: Option[DomainTopologyTransactionType],
      idFilter: String,
      namespaceOnly: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]]

  def inspectKnownParties(
      timestamp: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]]

  /** find active topology transactions
    *
    * active / state means that for the key authorizing the transaction, there is a connected path to reach the root certificate
    * this function is used for updating and by the lookup client [[com.digitalasset.canton.topology.client.StoreBasedTopologySnapshot]]
    *
    * @param asOfInclusive whether the search interval should include the current timepoint or not. the state at t is
    *                      defined as "exclusive" of t, whereas for updating the state, we need to be able to query inclusive.
    * @param includeSecondary some topology transactions have an "secondary" uid. currently, this only applies to the
    *                         party to participant mapping where the secondary uid is the participant uid.
    *                         we need this information during cascading updates of participant certificates.
    */
  def findStateTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit traceContext: TraceContext): Future[PositiveStoredTopologyTransactions]

  override protected def signedTxFromStoredTx(
      storedTx: StoredTopologyTransaction[TopologyChangeOp]
  ): SignedTopologyTransaction[TopologyChangeOp] = storedTx.transaction

  override def providesAdditionalSignatures(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): Future[Boolean] =
    exists(transaction).map(exists => !exists)

  final def exists(transaction: SignedTopologyTransaction[TopologyChangeOp])(implicit
      traceContext: TraceContext
  ): Future[Boolean] = findStored(transaction).map(
    _.exists(signedTxFromStoredTx(_) == transaction)
  )

  def findStored(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransaction[TopologyChangeOp]]]
}

object TopologyStore {

  def apply[StoreID <: TopologyStoreId](
      storeId: StoreID,
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      futureSupervisor: FutureSupervisor,
  )(implicit
      ec: ExecutionContext
  ): TopologyStore[StoreID] =
    storage match {
      case _: MemoryStorage =>
        (new InMemoryTopologyStore(storeId, loggerFactory, timeouts, futureSupervisor))

      case jdbc: DbStorage =>
        new DbTopologyStore(jdbc, storeId, timeouts, loggerFactory, futureSupervisor)

    }

  sealed trait Change extends Product with Serializable {
    def sequenced: SequencedTime
    def effective: EffectiveTime
  }

  object Change {
    final case class TopologyDelay(
        sequenced: SequencedTime,
        effective: EffectiveTime,
        epsilon: NonNegativeFiniteDuration,
    ) extends Change
    final case class Other(sequenced: SequencedTime, effective: EffectiveTime) extends Change

    def accumulateUpcomingEffectiveChanges(
        items: Seq[StoredTopologyTransaction[TopologyChangeOp]]
    ): Seq[TopologyStore.Change] = {
      items
        .map(x => (x, x.transaction.transaction.element.mapping))
        .map {
          case (tx, x: DomainParametersChange) =>
            TopologyDelay(tx.sequenced, tx.validFrom, x.domainParameters.topologyChangeDelay)
          case (tx, _) => Other(tx.sequenced, tx.validFrom)
        }
        .sortBy(_.effective)
        .distinct
    }

  }

  private[topology] final case class InsertTransaction(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      validUntil: Option[CantonTimestamp],
      rejectionReason: Option[TopologyTransactionRejection],
  ) {
    def op: TopologyChangeOp = transaction.transaction.op
  }

  /** collect all actions on the topology store during an append
    *
    * this function computes the paths of all transactions that will "expire" because of some
    * removal in this update block and calculates what needs to be inserted into the store.
    * we also insert transient data (for debugability and completeness).
    */
  private[topology] def appends(
      timestamp: CantonTimestamp,
      transactions: Seq[ValidatedTopologyTransaction],
  )(implicit loggingContext: ErrorLoggingContext): (
      Set[UniquePath], // updates
      Seq[InsertTransaction],
  ) = {
    val logger = loggingContext.logger
    implicit val traceContext: TraceContext = loggingContext.traceContext

    final case class PendingInsert(include: Boolean, entry: InsertTransaction)
    type PendingInsertIdx = Int

    // random access array in order to adjust pending insertions
    val inserts = new Array[PendingInsert](transactions.length)

    def adjustPending(
        index: PendingInsertIdx,
        pending: Map[UniquePath, Seq[PendingInsertIdx]],
        warnOnDuplicate: Boolean = true,
    ): Map[UniquePath, Seq[PendingInsertIdx]] = {
      val pendingInsert = inserts(index)
      val op = pendingInsert.entry.op
      val path = pendingInsert.entry.transaction.uniquePath

      val previous = pending.getOrElse(path, Seq())
      val previousI = previous.map(ii => (ii, inserts(ii))).filter { case (_, pendingInsert) =>
        pendingInsert.include
      }

      op match {
        // if this one is an add (resp., replace): only dedupe conflicting adds (resp., replaces)
        case TopologyChangeOp.Add | TopologyChangeOp.Replace =>
          previousI.foreach { case (ii, item) =>
            // ignore conflicting add
            if (item.entry.op == op) {
              inserts(ii) = item.copy(include = false)
              if (warnOnDuplicate)
                logger.warn(
                  s"Discarding duplicate ${op.toString} (#$ii): ${item.entry.transaction.uniquePath}"
                )
            }
          // malicious domain: theoretically we could check here if a certificate has already been revoked
          // previously. however, we assume that the domain topology manager would not do that generally (and we would
          // have to check this also against all revocations in the database as well).
          // TODO(i4933) check for permanent revocations
          }
        // if this one is a remove: deprecate pending adds and dedupe conflicting removes
        case TopologyChangeOp.Remove =>
          previousI.foreach { case (ii, item) =>
            if (item.entry.op == TopologyChangeOp.Remove) {
              // ignore conflicting remove
              inserts(ii) = item.copy(include = false)
              logger.info(
                s"Discarding conflicting removal (#$ii): ${item.entry.transaction.uniquePath}"
              )
            } else {
              // deprecate pending add
              inserts(ii) = item.copy(entry = item.entry.copy(validUntil = Some(timestamp)))
            }
          }
      }
      pending + (path -> (previous :+ index))
    }

    def validUntil(x: SignedTopologyTransaction[TopologyChangeOp]): Option[CantonTimestamp] =
      x.operation match {
        case TopologyChangeOp.Remove => Some(timestamp)
        case _ => None
      }

    // iterate over all transactions and adjust the validity period of any transient or special transaction
    val (updates, _) =
      transactions.zipWithIndex.foldLeft(
        (Set.empty[UniquePath], Map.empty[UniquePath, Seq[PendingInsertIdx]])
      ) {
        case (
              (updates, pending),
              (ValidatedTopologyTransaction(x: SignedTopologyTransaction[_], reason), index),
            ) =>
          inserts(index) = PendingInsert(
            include = true,
            InsertTransaction(x, validUntil(x), reason),
          )

          (x.transaction.op: TopologyChangeOp) match {
            case TopologyChangeOp.Remove | TopologyChangeOp.Replace =>
              // if this removal (or replace) is not authorized, then don't update the current exiting records
              val newUpdates =
                if (reason.isEmpty)
                  updates + x.uniquePath
                else updates
              (newUpdates, adjustPending(index, pending))

            case TopologyChangeOp.Add => (updates, adjustPending(index, pending))
          }
      }

    (
      updates,
      inserts.collect {
        case insert if insert.include =>
          val insertTx = insert.entry
          // mark all rejected transactions to be validFrom = validUntil
          insertTx.rejectionReason.fold(insertTx)(_ => insertTx.copy(validUntil = Some(timestamp)))
      }.toSeq,
    )
  }

  /** Initial state accumulator
    *
    * Initially, when bootstrapping a domain, we need to know the domain topology manager and the sequencer
    * key(s) before they have been sequenced. Therefore, we'll look at the couple of first transactions of the
    * authorized domain topology store.
    *
    * This accumulator should be iterated over until the boolean flag says its done.
    */
  private[topology] def findInitialStateAccumulator(
      uid: UniqueIdentifier,
      accumulated: Map[Member, Seq[PublicKey]],
      transaction: SignedTopologyTransaction[TopologyChangeOp],
  ): (Boolean, Map[Member, Seq[PublicKey]]) = {
    // we are done once we observe a transaction that does not act on our uid
    val done =
      transaction.uniquePath.maybeUid.nonEmpty && !transaction.uniquePath.maybeUid.contains(uid) &&
        accumulated.isDefinedAt(SequencerId(uid)) && accumulated.isDefinedAt(
          DomainTopologyManagerId(uid)
        )
    if (done || transaction.uniquePath.dbType != DomainTopologyTransactionType.OwnerToKeyMapping) {
      (done, accumulated)
    } else {
      transaction match {
        case SignedTopologyTransaction(
              TopologyStateUpdate(
                TopologyChangeOp.Add,
                TopologyStateUpdateElement(_, OwnerToKeyMapping(owner, key)),
              ),
              _,
              _,
            ) if owner.code == SequencerId.Code || owner.code == DomainTopologyManagerId.Code =>
          (false, accumulated.updated(owner, accumulated.getOrElse(owner, Seq()) :+ key))
        case _ => (false, accumulated)
      }
    }
  }

  lazy val initialParticipantDispatchingSet = Set(
    DomainTopologyTransactionType.ParticipantState,
    DomainTopologyTransactionType.OwnerToKeyMapping,
    DomainTopologyTransactionType.SignedLegalIdentityClaim,
  )

  def filterInitialParticipantDispatchingTransactions(
      participantId: ParticipantId,
      domainId: DomainId,
      store: TopologyStore[TopologyStoreId],
      loggerFactory: NamedLoggerFactory,
      transactions: StoredTopologyTransactions[TopologyChangeOp],
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Seq[SignedTopologyTransaction[TopologyChangeOp]]] = {
    val logger = loggerFactory.getLogger(getClass)
    def includeState(mapping: TopologyStateUpdateMapping): Boolean = mapping match {
      case NamespaceDelegation(_, _, _) | IdentifierDelegation(_, _) =>
        // note for future devs: this function here should only be supplied with core mappings that need to be
        // sent to the topology manager on bootstrapping. so the query that picks these transactions up should
        // not include namespace delegations and therelike
        // note that we'll pick up the necessary certificates further below
        logger.error("Initial dispatching should not include namespace or identifier delegations")
        false
      case OwnerToKeyMapping(pid, _) => pid == participantId
      case SignedLegalIdentityClaim(uid, _, _) => uid == participantId.uid
      case ParticipantState(_, _, pid, _, _) => pid == participantId
      case PartyToParticipant(_, _, _, _) => false
      case VettedPackages(_, _) => false
      case MediatorDomainState(_, _, _) => false
    }
    def include(mapping: TopologyMapping): Boolean = mapping match {
      case mapping: TopologyStateUpdateMapping => includeState(mapping)
      case _ => false
    }
    val validator =
      new SnapshotAuthorizationValidator(
        CantonTimestamp.MaxValue,
        store,
        timeouts,
        loggerFactory,
        futureSupervisor,
      )
    val filtered = transactions.result.filter(tx =>
      tx.transaction.transaction.element.mapping.restrictedToDomain
        .forall(_ == domainId) && include(tx.transaction.transaction.element.mapping)
    )
    val authF = filtered.toList
      .parFlatTraverse(tx =>
        validator
          .authorizedBy(tx.transaction)
          .map(_.toList)
      )
      .map(_.foldLeft(AuthorizationChain.empty) { case (acc, elem) => acc.merge(elem) })
    authF.map { chain =>
      // put all transactions into the correct order to ensure that the authorizations come first
      chain.namespaceDelegations.map(_.transaction) ++ chain.identifierDelegation.map(
        _.transaction
      ) ++ filtered.map(_.transaction)
    }
  }

  /** convenience method waiting until the last eligible transaction inserted into the source store has been dispatched successfully to the target domain */
  def awaitTxObserved(
      client: DomainTopologyClient,
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      target: TopologyStore[DomainStore],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Boolean] = {
    client.await(
      // we know that the transaction is stored and effective once we find it in the target
      // domain store and once the effective time (valid from) is smaller than the client timestamp
      sp => target.findStored(transaction).map(_.exists(_.validFrom.value < sp.timestamp)),
      timeout,
    )
  }

}
