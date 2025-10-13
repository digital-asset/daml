// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.Monoid
import cats.data.EitherT
import cats.implicits.catsSyntaxParallelTraverse1
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedString,
  String185,
  String255,
  String300,
}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.parallelInstanceFutureUnlessShutdown
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v30 as adminTopoV30
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.{
  GenericStoredTopologyTransactions,
  PositiveStoredTopologyTransactions,
}
import com.digitalasset.canton.topology.store.TopologyStore.EffectiveStateChange
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.db.DbTopologyStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  GenericTopologyTransaction,
  TxHash,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

sealed trait TopologyStoreId extends PrettyPrinting with Product with Serializable {
  def dbString: LengthLimitedString
  def isAuthorizedStore: Boolean = false
  def isSynchronizerStore: Boolean = false
  def isTemporaryStore: Boolean = false

  def forSynchronizer: Option[PhysicalSynchronizerId] = None
}
object TopologyStoreId {

  /** A topology store storing sequenced topology transactions
    *
    * @param psid
    *   the synchronizer id of the store
    */
  final case class SynchronizerStore(psid: PhysicalSynchronizerId) extends TopologyStoreId {
    override val dbString = psid.toLengthLimitedString

    override protected def pretty: Pretty[this.type] =
      prettyOfParam(_.psid)

    override def isSynchronizerStore: Boolean = true

    override def forSynchronizer: Option[PhysicalSynchronizerId] = Some(psid)
  }

  // authorized transactions (the topology managers store)
  type AuthorizedStore = AuthorizedStore.type
  case object AuthorizedStore extends TopologyStoreId {
    val dbString: String255 = String255.tryCreate("Authorized")

    override protected def pretty: Pretty[AuthorizedStore.this.type] = prettyOfString(
      _.dbString.unwrap
    )

    override def isAuthorizedStore: Boolean = true
  }

  final case class TemporaryStore(name: String185) extends TopologyStoreId {
    override def dbString: LengthLimitedString = TemporaryStore.withTempMarker(name)

    override def isTemporaryStore: Boolean = true

    override protected def pretty: Pretty[TemporaryStore.this.type] =
      prettyOfString(_.dbString.unwrap)
  }

  object TemporaryStore {
    // add a prefix and suffix to not accidentally interpret a synchronizer store with the name 'temp' as temporary store
    val marker = "temp"
    val prefix = s"$marker${UniqueIdentifier.delimiter}"
    val suffix = s"${UniqueIdentifier.delimiter}$marker"
    private[TemporaryStore] def withTempMarker(name: String185): String185 =
      String185.tryCreate(s"$prefix$name$suffix")

    def create(name: String): Either[String, TemporaryStore] =
      String185.create(name).map(TemporaryStore(_))

    def tryCreate(name: String): TemporaryStore =
      create(name).valueOr(err => throw new IllegalArgumentException(err))
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def select[StoreId <: TopologyStoreId: ClassTag](
      store: TopologyStore[TopologyStoreId]
  ): Option[TopologyStore[StoreId]] = store.storeId match {
    // this typecheck is safe to do, because we have a ClassTag in scope
    case _: StoreId => Some(store.asInstanceOf[TopologyStore[StoreId]])
    case _ => None
  }
}

final case class StoredTopologyTransaction[+Op <: TopologyChangeOp, +M <: TopologyMapping](
    sequenced: SequencedTime,
    validFrom: EffectiveTime,
    validUntil: Option[EffectiveTime],
    transaction: SignedTopologyTransaction[Op, M],
    rejectionReason: Option[String300],
) extends DelegatedTopologyTransactionLike[Op, M]
    with HasVersionedWrapper[StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]]
    with PrettyPrinting {

  override protected def companionObj: StoredTopologyTransaction.type = StoredTopologyTransaction

  def toAdminProtoV30: adminTopoV30.TopologyTransactions.Item =
    adminTopoV30.TopologyTransactions.Item(
      sequenced = Some(sequenced.value.toProtoTimestamp),
      validFrom = Some(validFrom.value.toProtoTimestamp),
      validUntil = validUntil.map(_.value.toProtoTimestamp),
      transaction = transaction.toByteString,
      rejectionReason = rejectionReason.map(_.str),
    )

  override protected def transactionLikeDelegate: TopologyTransactionLike[Op, M] = transaction

  override protected def pretty: Pretty[StoredTopologyTransaction.this.type] =
    prettyOfClass(
      unnamedParam(_.transaction),
      param("sequenced", _.sequenced.value),
      param("validFrom", _.validFrom.value),
      paramIfDefined("validUntil", _.validUntil.map(_.value)),
      paramIfDefined("rejectionReason", _.rejectionReason.map(_.str.singleQuoted)),
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectMapping[TargetMapping <: TopologyMapping: ClassTag] = transaction
    .selectMapping[TargetMapping]
    .map(_ => this.asInstanceOf[StoredTopologyTransaction[Op, TargetMapping]])

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectOp[TargetOp <: TopologyChangeOp: ClassTag] = transaction
    .selectOp[TargetOp]
    .map(_ => this.asInstanceOf[StoredTopologyTransaction[TargetOp, M]])
}

object StoredTopologyTransaction
    extends HasVersionedMessageCompanion[
      StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]
    ] {

  override def name: String = "stored topology transaction"

  def fromProtoV30(
      proto: adminTopoV30.TopologyTransactions.Item
  ): ParsingResult[StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]] =
    for {
      sequenced <- ProtoConverter
        .parseRequired(SequencedTime.fromProtoPrimitive, "sequenced", proto.sequenced)
      validFrom <- ProtoConverter
        .parseRequired(EffectiveTime.fromProtoPrimitive, "valid_from", proto.validFrom)
      validUntil <- proto.validUntil.traverse(EffectiveTime.fromProtoPrimitive)
      rejectionReason <- proto.rejectionReason.traverse(
        String300.fromProtoPrimitive(_, "rejection_reason")
      )
      signedTx <- SignedTopologyTransaction.fromTrustedByteStringPVV(proto.transaction)
    } yield StoredTopologyTransaction(sequenced, validFrom, validUntil, signedTx, rejectionReason)

  override def supportedProtoVersions: StoredTopologyTransaction.SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(30) -> ProtoCodec(
        ProtocolVersion.v34,
        supportedProtoVersion(adminTopoV30.TopologyTransactions.Item)(fromProtoV30),
        _.toAdminProtoV30,
      )
    )

  type GenericStoredTopologyTransaction =
    StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]

  /** @return
    *   `true` if both transactions are the same without comparing the signatures, `false` otherwise
    */
  def equalIgnoringSignatures(
      a: GenericStoredTopologyTransaction,
      b: GenericStoredTopologyTransaction,
  ): Boolean = a match {
    case StoredTopologyTransaction(
          b.sequenced,
          b.validFrom,
          b.validUntil,
          SignedTopologyTransaction(
            b.transaction.transaction,
            _ignoreSignatures,
            b.transaction.isProposal,
          ),
          b.rejectionReason,
        ) =>
      true
    case _ => false
  }
}

final case class ValidatedTopologyTransaction[+Op <: TopologyChangeOp, +M <: TopologyMapping](
    transaction: SignedTopologyTransaction[Op, M],
    rejectionReason: Option[TopologyTransactionRejection] = None,
    expireImmediately: Boolean = false,
) extends DelegatedTopologyTransactionLike[Op, M]
    with PrettyPrinting {

  override protected def transactionLikeDelegate: TopologyTransactionLike[Op, M] = transaction

  def collectOfMapping[TargetM <: TopologyMapping: ClassTag]
      : Option[ValidatedTopologyTransaction[Op, TargetM]] =
    transaction.selectMapping[TargetM].map(tx => copy[Op, TargetM](transaction = tx))

  def collectOf[TargetO <: TopologyChangeOp: ClassTag, TargetM <: TopologyMapping: ClassTag]
      : Option[ValidatedTopologyTransaction[TargetO, TargetM]] =
    transaction.select[TargetO, TargetM].map(tx => copy[TargetO, TargetM](transaction = tx))

  override protected def pretty: Pretty[ValidatedTopologyTransaction.this.type] =
    prettyOfClass(
      unnamedParam(_.transaction),
      paramIfDefined("rejectionReason", _.rejectionReason),
      paramIfTrue("expireImmediately", _.expireImmediately),
    )
}

object ValidatedTopologyTransaction {
  type GenericValidatedTopologyTransaction =
    ValidatedTopologyTransaction[TopologyChangeOp, TopologyMapping]
}

abstract class TopologyStore[+StoreID <: TopologyStoreId](implicit
    protected val ec: ExecutionContext
) extends FlagCloseable {
  this: NamedLogging =>

  def storeId: StoreID

  def protocolVersion: ProtocolVersion

  /** fetch the effective time updates greater than or equal to a certain timestamp
    *
    * this function is used to recover the future effective timestamp such that we can reschedule
    * "pokes" of the topology client and updates of the acs commitment processor on startup
    */
  def findUpcomingEffectiveChanges(asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyStore.Change]]

  /** Finds the transaction with maximum effective time that has been sequenced at or before
    * `sequencedTime` and yields the sequenced / effective time of that transaction.
    *
    * @param includeRejected
    *   whether to include rejected transactions
    */
  def maxTimestamp(sequencedTime: SequencedTime, includeRejected: Boolean)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]]

  /** returns the current dispatching watermark
    *
    * for topology transaction dispatching, we keep track up to which point in time we have mirrored
    * the authorized store to the remote store
    *
    * the timestamp always refers to the timestamp of the authorized store!
    */
  def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]]

  /** update the dispatching watermark for this target store */
  def updateDispatchingWatermark(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  def findLatestTransactionsAndProposalsByTxHash(hashes: Set[TxHash])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]]

  def findProposalsByTxHash(asOfExclusive: EffectiveTime, hashes: NonEmpty[Set[TxHash]])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]]

  def findTransactionsForMapping(asOfExclusive: EffectiveTime, hashes: NonEmpty[Set[MappingHash]])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]]

  /** returns the set of positive transactions
    *
    * this function is used by the topology processor to determine the set of transaction, such that
    * we can perform cascading updates if there was a certificate revocation
    *
    * @param asOfInclusive
    *   whether the search interval should include the current timepoint or not. the state at t is
    *   defined as "exclusive" of t, whereas for updating the state, we need to be able to query
    *   inclusive.
    */
  def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMapping.Code],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]],
      filterNamespace: Option[NonEmpty[Seq[Namespace]]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[PositiveStoredTopologyTransactions]

  /** Updates topology transactions. The method proceeds as follows:
    *   1. It expires all transactions `tx` with
    *      `removeMapping.get(tx.mapping.uniqueKey).exists(tx.serial <= _)`. By `expire` we mean
    *      that `valid_until` is set to `effective`, provided `valid_until` was previously `NULL`
    *      and `valid_from < effective`. 2. It expires all transactions `tx` with `tx.hash` in
    *      `removeTxs`. 3. It adds all transactions in additions. Thereby: 3.1. It sets valid_until
    *      to effective, if there is a rejection reason or if `expireImmediately`.
    */
  def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removeMapping: Map[MappingHash, PositiveInt],
      removeTxs: Set[TxHash],
      additions: Seq[GenericValidatedTopologyTransaction],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  def bulkInsert(
      initialSnapshot: GenericStoredTopologyTransactions
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  @VisibleForTesting
  protected[topology] def dumpStoreContent()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions]

  /** query optimized for inspection
    *
    * @param proposals
    *   if true, query only for proposals instead of approved transaction mappings
    * @param asOfExclusiveO
    *   if exists, use this timestamp for the head state to prevent race conditions on the console
    */
  def inspect(
      proposals: Boolean,
      timeQuery: TimeQuery,
      asOfExclusiveO: Option[CantonTimestamp],
      op: Option[TopologyChangeOp],
      types: Seq[TopologyMapping.Code],
      idFilter: Option[String],
      namespaceFilter: Option[String],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]]

  def inspectKnownParties(
      asOfExclusive: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]]

  /** Finds the topology transaction that first onboarded the sequencer with ID `sequencerId`
    */
  def findFirstSequencerStateForSequencer(
      sequencerId: SequencerId
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Option[StoredTopologyTransaction[TopologyChangeOp.Replace, SequencerSynchronizerState]]
  ]

  /** Finds the topology transaction that first onboarded the mediator with ID `mediatorId`
    */
  def findFirstMediatorStateForMediator(
      mediatorId: MediatorId
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Option[StoredTopologyTransaction[TopologyChangeOp.Replace, MediatorSynchronizerState]]
  ]

  /** Finds the topology transaction that first onboarded the participant with ID `participantId`
    */
  def findFirstTrustCertificateForParticipant(
      participant: ParticipantId
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Option[StoredTopologyTransaction[TopologyChangeOp.Replace, SynchronizerTrustCertificate]]
  ]

  /** Yields all transactions with sequenced time less than or equal to `asOfInclusive`. Sets
    * `validUntil` to `None`, if `validUntil` is strictly greater than the maximum value of
    * `validFrom`.
    */
  def findEssentialStateAtSequencedTime(
      asOfInclusive: SequencedTime,
      includeRejected: Boolean,
  )(implicit traceContext: TraceContext): Source[GenericStoredTopologyTransaction, NotUsed]

  /** Checks whether the given signed topology transaction has signatures (at this point still
    * unvalidated) from signing keys, for which there aren't yet signatures in the store.
    */
  def providesAdditionalSignatures(
      transaction: GenericSignedTopologyTransaction
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    findStored(CantonTimestamp.MaxValue, transaction).map(_.forall { inStore =>
      // check whether source still could provide an additional signature
      transaction.signatures
        .map(_.authorizingLongTermKey)
        .diff(inStore.transaction.signatures.map(_.authorizingLongTermKey))
        .nonEmpty &&
      // but only if the transaction in the target store is a valid proposal
      inStore.transaction.isProposal &&
      inStore.validUntil.isEmpty
    })

  /** Returns initial set of onboarding transactions that should be dispatched to the synchronizer.
    * Includes:
    *   - SynchronizerTrustCertificates for the given participantId
    *   - OwnerToKeyMappings for the given participantId
    *   - NamespaceDelegations for the participantId's namespace
    *   - The above even if they are expired.
    *   - Transactions with operation REMOVE. Excludes:
    *   - Proposals
    *   - Rejected transactions
    *   - Transactions that are not valid for synchronizerId
    */
  def findParticipantOnboardingTransactions(
      participantId: ParticipantId,
      synchronizerId: SynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]]

  /** Yields transactions with `validFrom` strictly greater than `timestampExclusive`. Excludes
    * rejected transactions and expired proposals.
    */
  def findDispatchingTransactionsAfter(
      timestampExclusive: CantonTimestamp,
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions]

  /** Finds the last (i.e. highest id) stored transaction with `validFrom` strictly before
    * `asOfExclusive` that has the same hash as `transaction` and the representative protocol
    * version for the given `protocolVersion`. Excludes rejected transactions.
    */
  def findStoredForVersion(
      asOfExclusive: CantonTimestamp,
      transaction: GenericTopologyTransaction,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]]

  /** Finds the last (i.e. highest id) stored transaction with `validFrom` strictly before
    * `asOfExclusive` that has the same hash as `transaction`.
    */
  def findStored(
      asOfExclusive: CantonTimestamp,
      transaction: GenericSignedTopologyTransaction,
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]]

  /** Find all effective-time state changes. One EffectiveStateChange contains all positive
    * transactions which have valid_from == fromEffectiveInclusive for the new state, and all
    * positive transactions which have valid_until == fromEffectiveInclusive for the old/previous,
    * but none of those transactions which meet both criteria (transient topology changes). This
    * query does not return proposals, rejected transactions or removals.
    *
    * @param fromEffectiveInclusive
    *   If onlyAtEffective is true, look up state change for a single effective time, which should
    *   produce at most one result per mapping unique key. If onlyAtEffective is false, this defines
    *   the inclusive lower bound for effective time: lookup up all state changes for all effective
    *   times bigger than or equal to this.
    * @param filterTypes
    *   If defined, restrict the query to specific mappings.
    * @param onlyAtEffective
    *   Controls whether fromEffectiveInclusive defines a single effective time, or an inclusive
    *   lower bound for the query.
    * @return
    *   A sequence of EffectiveStateChange. Neither the sequence, nor the before/after fields in the
    *   results are ordered.
    */
  def findEffectiveStateChanges(
      fromEffectiveInclusive: CantonTimestamp,
      filterTypes: Option[Seq[TopologyMapping.Code]] = None,
      onlyAtEffective: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[EffectiveStateChange]]

  def deleteAllData()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]
}

object TopologyStore {

  sealed trait Change extends Product with Serializable {
    def sequenced: SequencedTime
    def validFrom: EffectiveTime
  }

  object Change {
    final case class Other(sequenced: SequencedTime, validFrom: EffectiveTime) extends Change

    def selectChange(tx: GenericStoredTopologyTransaction): Change =
      Change.Other(tx.sequenced, tx.validFrom)
  }

  def apply[StoreID <: TopologyStoreId](
      storeId: StoreID,
      storage: Storage,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): TopologyStore[StoreID] = {
    val storeLoggerFactory = loggerFactory.append("store", storeId.toString)
    storage match {
      case _: MemoryStorage =>
        new InMemoryTopologyStore(storeId, protocolVersion, storeLoggerFactory, timeouts)
      case dbStorage: DbStorage =>
        new DbTopologyStore(dbStorage, storeId, protocolVersion, timeouts, storeLoggerFactory)
    }
  }

  lazy val initialParticipantDispatchingSet: Set[TopologyMapping.Code] = Set(
    TopologyMapping.Code.SynchronizerTrustCertificate,
    TopologyMapping.Code.OwnerToKeyMapping,
    TopologyMapping.Code.NamespaceDelegation,
  )

  def filterInitialParticipantDispatchingTransactions(
      participantId: ParticipantId,
      synchronizerId: SynchronizerId,
      transactions: Seq[GenericStoredTopologyTransaction],
  ): Seq[GenericSignedTopologyTransaction] =
    transactions.map(_.transaction).filter { signedTx =>
      initialParticipantDispatchingSet.contains(signedTx.mapping.code) &&
      signedTx.mapping.maybeUid.forall(_ == participantId.uid) &&
      signedTx.mapping.namespace == participantId.namespace &&
      signedTx.mapping.restrictedToSynchronizer.forall(_ == synchronizerId)
    }

  /** convenience method waiting until the last eligible transaction inserted into the source store
    * has been dispatched successfully to the target synchronizer
    */
  def awaitTxObserved(
      client: SynchronizerTopologyClient,
      transaction: GenericSignedTopologyTransaction,
      target: TopologyStore[?],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Boolean] =
    client.awaitUS(
      // we know that the transaction is stored and effective once we find it in the target
      // synchronizer store and once the effective time (valid from) is smaller than the client timestamp
      sp =>
        target
          .findStored(sp.timestamp, transaction, includeRejected = true)
          .map(_.nonEmpty),
      timeout,
    )

  final case class EffectiveStateChange(
      effectiveTime: EffectiveTime,
      sequencedTime: SequencedTime,
      before: PositiveStoredTopologyTransactions,
      after: PositiveStoredTopologyTransactions,
  )

  /** determine valid parties within the given mappings (requires p2p, otk and stc) */
  private[store] def determineValidParties(
      mappings: Seq[TopologyMapping],
      filterParty: String,
      filterParticipant: String,
  ): Set[PartyId] = {
    val (filterPartyIdentifier, filterPartyNamespaceO) =
      UniqueIdentifier.splitFilter(filterParty)
    val (
      filterParticipantIdentifier,
      filterParticipantNamespaceO,
    ) =
      UniqueIdentifier.splitFilter(filterParticipant)
    val validParticipants = determineValidParticipants(mappings)
    val validParties = mutable.HashSet[PartyId]()
    mappings.foreach {
      case ptp: PartyToParticipant
          if (filterParty.isEmpty || ptp.partyId.uid
            .matchesFilters(filterPartyIdentifier, filterPartyNamespaceO)) &&
            (filterParticipant.isEmpty || ptp.participants
              .exists(
                _.participantId.uid
                  .matchesFilters(filterParticipantIdentifier, filterParticipantNamespaceO)
              )) && ptp.participants.exists(h => validParticipants.contains(h.participantId)) =>
        validParties.add(ptp.partyId).discard
      case cert: SynchronizerTrustCertificate
          if (filterParty.isEmpty || cert.participantId.adminParty.uid
            .matchesFilters(filterPartyIdentifier, filterPartyNamespaceO))
            && (filterParticipant.isEmpty || cert.participantId.adminParty.uid.matchesFilters(
              filterParticipantIdentifier,
              filterParticipantNamespaceO,
            ))
            && validParticipants
              .contains(cert.participantId) =>
        validParties.add(cert.participantId.adminParty).discard
      case _ => ()
    }
    validParties.toSet
  }

  /** Given a series of topology transactions, determine the participant ids that have OTK and STC
    */
  private def determineValidParticipants(
      txs: Iterable[TopologyMapping]
  ): Set[ParticipantId] = {
    val validParticipants = mutable.Map[ParticipantId, (Boolean, Boolean)]()
    txs.foreach {
      case OwnerToKeyMapping(
            pid: ParticipantId,
            _,
          ) => // assumption: keys is checked as a state variant
        validParticipants
          .updateWith(pid) {
            case None => Some((true, false))
            case Some((_, stc)) => Some((true, stc))
          }
          .discard
      case SynchronizerTrustCertificate(pid, _, _) =>
        validParticipants
          .updateWith(pid) {
            case None => Some((false, true))
            case Some((otk, _)) => Some((otk, true))
          }
          .discard
      case _ => ()
    }
    validParticipants.filter { case (_, (otk, stc)) => otk && stc }.keySet.toSet
  }

}

sealed trait TimeQuery {
  def toProtoV30: adminTopoV30.BaseQuery.TimeQuery
}

object TimeQuery {
  case object HeadState extends TimeQuery {
    override def toProtoV30: adminTopoV30.BaseQuery.TimeQuery =
      adminTopoV30.BaseQuery.TimeQuery.HeadState(com.google.protobuf.empty.Empty())
  }
  final case class Snapshot(asOf: CantonTimestamp) extends TimeQuery {
    override def toProtoV30: adminTopoV30.BaseQuery.TimeQuery =
      adminTopoV30.BaseQuery.TimeQuery.Snapshot(asOf.toProtoTimestamp)
  }
  final case class Range(from: Option[CantonTimestamp], until: Option[CantonTimestamp])
      extends TimeQuery {
    override def toProtoV30: adminTopoV30.BaseQuery.TimeQuery =
      adminTopoV30.BaseQuery.TimeQuery.Range(
        adminTopoV30.BaseQuery
          .TimeRange(from.map(_.toProtoTimestamp), until.map(_.toProtoTimestamp))
      )
  }

  def fromProto(
      proto: adminTopoV30.BaseQuery.TimeQuery,
      fieldName: String,
  ): ParsingResult[TimeQuery] =
    proto match {
      case adminTopoV30.BaseQuery.TimeQuery.Empty =>
        Left(ProtoDeserializationError.FieldNotSet(fieldName))
      case adminTopoV30.BaseQuery.TimeQuery.Snapshot(value) =>
        CantonTimestamp.fromProtoTimestamp(value).map(Snapshot.apply)
      case adminTopoV30.BaseQuery.TimeQuery.HeadState(_) => Right(HeadState)
      case adminTopoV30.BaseQuery.TimeQuery.Range(value) =>
        for {
          fromO <- value.from.traverse(CantonTimestamp.fromProtoTimestamp)
          toO <- value.until.traverse(CantonTimestamp.fromProtoTimestamp)
        } yield Range(fromO, toO)
    }
}

object UnknownOrUnvettedPackages {

  val empty: UnknownOrUnvettedPackages = UnknownOrUnvettedPackages(Map.empty, Map.empty)

  implicit val monoid: Monoid[UnknownOrUnvettedPackages] =
    new Monoid[UnknownOrUnvettedPackages] {
      override def empty: UnknownOrUnvettedPackages = UnknownOrUnvettedPackages.empty
      override def combine(
          x: UnknownOrUnvettedPackages,
          y: UnknownOrUnvettedPackages,
      ): UnknownOrUnvettedPackages =
        UnknownOrUnvettedPackages(
          unknown = MapsUtil.mergeMapsOfSets(x.unknown, y.unknown),
          unvetted = MapsUtil.mergeMapsOfSets(x.unvetted, y.unvetted),
        )

    }

  def unknown(participantId: ParticipantId, packageId: PackageId): UnknownOrUnvettedPackages =
    empty.copy(unknown = Map(participantId -> Set(packageId)))
  def unvetted(participantId: ParticipantId, packageId: PackageId): UnknownOrUnvettedPackages =
    empty.copy(unvetted = Map(participantId -> Set(packageId)))
  def unvetted(
      participantId: ParticipantId,
      packageIds: Set[PackageId],
  ): UnknownOrUnvettedPackages =
    if (packageIds.isEmpty) empty else empty.copy(unvetted = Map(participantId -> packageIds))
}

final case class UnknownOrUnvettedPackages(
    unknown: Map[ParticipantId, Set[PackageId]],
    unvetted: Map[ParticipantId, Set[PackageId]],
) {
  def isEmpty: Boolean = unknown.isEmpty && unvetted.isEmpty
  def unknownOrUnvetted: Map[ParticipantId, Set[PackageId]] =
    MapsUtil.mergeMapsOfSets(unknown, unvetted)
}

trait PackageDependencyResolver {

  def packageDependencies(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, (PackageId, ParticipantId), Set[PackageId]]

  def packageDependencies(packages: List[PackageId])(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, (PackageId, ParticipantId), Set[PackageId]] =
    packages
      .parTraverse(packageDependencies)
      .map(_.flatten.toSet -- packages)

}
