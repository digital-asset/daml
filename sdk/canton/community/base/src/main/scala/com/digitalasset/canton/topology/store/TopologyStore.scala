// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.data.EitherT
import cats.syntax.traverse.*
import com.daml.lf.data.Ref.PackageId
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.{LengthLimitedString, String255}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v30 as topoV30
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.{
  GenericStoredTopologyTransactions,
  PositiveStoredTopologyTransactions,
}
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.Duplicate
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.db.DbTopologyStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  GenericTopologyTransaction,
  TxHash,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

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
    * @param discriminator the discriminator of the store. used for mediator confirmation request store
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
          show"${storeId.discriminator}${UniqueIdentifier.delimiter}${storeId.domainId}"
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

  def apply(fName: String): TopologyStoreId = fName.toLowerCase match {
    case "authorized" => AuthorizedStore
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

}

final case class StoredTopologyTransaction[+Op <: TopologyChangeOp, +M <: TopologyMapping](
    sequenced: SequencedTime,
    validFrom: EffectiveTime,
    validUntil: Option[EffectiveTime],
    transaction: SignedTopologyTransaction[Op, M],
) extends DelegatedTopologyTransactionLike[Op, M]
    with PrettyPrinting {
  override protected def transactionLikeDelegate: TopologyTransactionLike[Op, M] = transaction

  override def pretty: Pretty[StoredTopologyTransaction.this.type] =
    prettyOfClass(
      unnamedParam(_.transaction),
      param("sequenced", _.sequenced.value),
      param("validFrom", _.validFrom.value),
      paramIfDefined("validUntil", _.validUntil.map(_.value)),
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

object StoredTopologyTransaction {
  type GenericStoredTopologyTransaction =
    StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]
}

final case class ValidatedTopologyTransaction[+Op <: TopologyChangeOp, +M <: TopologyMapping](
    transaction: SignedTopologyTransaction[Op, M],
    rejectionReason: Option[TopologyTransactionRejection] = None,
    expireImmediately: Boolean = false,
) extends DelegatedTopologyTransactionLike[Op, M]
    with PrettyPrinting {

  override protected def transactionLikeDelegate: TopologyTransactionLike[Op, M] = transaction

  def nonDuplicateRejectionReason: Option[TopologyTransactionRejection] = rejectionReason match {
    case Some(Duplicate(_)) => None
    case otherwise => otherwise
  }

  def collectOfMapping[TargetM <: TopologyMapping: ClassTag]
      : Option[ValidatedTopologyTransaction[Op, TargetM]] =
    transaction.selectMapping[TargetM].map(tx => copy[Op, TargetM](transaction = tx))

  def collectOf[TargetO <: TopologyChangeOp: ClassTag, TargetM <: TopologyMapping: ClassTag]
      : Option[ValidatedTopologyTransaction[TargetO, TargetM]] =
    transaction.select[TargetO, TargetM].map(tx => copy[TargetO, TargetM](transaction = tx))

  override def pretty: Pretty[ValidatedTopologyTransaction.this.type] =
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

  def findTransactionsByTxHash(asOfExclusive: EffectiveTime, hashes: Set[TxHash])(implicit
      traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransaction]]

  def findProposalsByTxHash(asOfExclusive: EffectiveTime, hashes: NonEmpty[Set[TxHash]])(implicit
      traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransaction]]

  def findTransactionsForMapping(asOfExclusive: EffectiveTime, hashes: NonEmpty[Set[MappingHash]])(
      implicit traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransaction]]

  /** returns the set of positive transactions
    *
    * this function is used by the topology processor to determine the set of transaction, such that
    * we can perform cascading updates if there was a certificate revocation
    *
    * @param asOfInclusive whether the search interval should include the current timepoint or not. the state at t is
    *                      defined as "exclusive" of t, whereas for updating the state, we need to be able to query inclusive.
    */
  def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMapping.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit
      traceContext: TraceContext
  ): Future[PositiveStoredTopologyTransactions]

  /** add validated topology transaction as is to the topology transaction table */
  def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removeMapping: Map[MappingHash, PositiveInt],
      removeTxs: Set[TxHash],
      additions: Seq[GenericValidatedTopologyTransaction],
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  @VisibleForTesting
  protected[topology] def dumpStoreContent()(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactions]

  /** store an initial set of topology transactions as given into the store */
  def bootstrap(snapshot: GenericStoredTopologyTransactions)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** query optimized for inspection
    *
    * @param proposals if true, query only for proposals instead of approved transaction mappings
    * @param recentTimestampO if exists, use this timestamp for the head state to prevent race conditions on the console
    */
  def inspect(
      proposals: Boolean,
      timeQuery: TimeQuery,
      recentTimestampO: Option[CantonTimestamp],
      op: Option[TopologyChangeOp],
      types: Seq[TopologyMapping.Code],
      idFilter: Option[String],
      namespaceFilter: Option[String],
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]]

  def inspectKnownParties(
      timestamp: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]]

  /** Finds the topology transaction that first onboarded the sequencer with ID `sequencerId`
    */
  def findFirstSequencerStateForSequencer(
      sequencerId: SequencerId
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransaction[TopologyChangeOp.Replace, SequencerDomainState]]]

  /** Finds the topology transaction that first onboarded the mediator with ID `mediatorId`
    */
  def findFirstMediatorStateForMediator(
      mediatorId: MediatorId
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransaction[TopologyChangeOp.Replace, MediatorDomainState]]]

  /** Finds the topology transaction that first onboarded the participant with ID `participantId`
    */
  def findFirstTrustCertificateForParticipant(
      participant: ParticipantId
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransaction[TopologyChangeOp.Replace, DomainTrustCertificate]]]

  def findEssentialStateAtSequencedTime(
      asOfInclusive: SequencedTime,
      excludeMappings: Seq[TopologyMapping.Code] = Nil,
  )(implicit traceContext: TraceContext): Future[GenericStoredTopologyTransactions]

  protected def signedTxFromStoredTx(
      storedTx: GenericStoredTopologyTransaction
  ): SignedTopologyTransaction[TopologyChangeOp, TopologyMapping] = storedTx.transaction

  def providesAdditionalSignatures(
      transaction: GenericSignedTopologyTransaction
  )(implicit traceContext: TraceContext): Future[Boolean] = {
    findStored(CantonTimestamp.MaxValue, transaction).map(_.forall { inStore =>
      // check whether source still could provide an additional signature
      transaction.signatures.diff(inStore.transaction.signatures.forgetNE).nonEmpty &&
      // but only if the transaction in the target store is a valid proposal
      inStore.transaction.isProposal &&
      inStore.validUntil.isEmpty
    })
  }

  /** returns initial set of onboarding transactions that should be dispatched to the domain */
  def findParticipantOnboardingTransactions(participantId: ParticipantId, domainId: DomainId)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]]

  def findDispatchingTransactionsAfter(
      timestampExclusive: CantonTimestamp,
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactions]

  def findStoredForVersion(
      asOfExclusive: CantonTimestamp,
      transaction: GenericTopologyTransaction,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransaction]]

  final def exists(transaction: GenericSignedTopologyTransaction)(implicit
      traceContext: TraceContext
  ): Future[Boolean] = findStored(CantonTimestamp.MaxValue, transaction).map(
    _.exists(signedTxFromStoredTx(_) == transaction)
  )

  def findStored(
      asOfExclusive: CantonTimestamp,
      transaction: GenericSignedTopologyTransaction,
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransaction]]
}

object TopologyStore {

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
  }

  def accumulateUpcomingEffectiveChanges(
      items: Seq[StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]]
  ): Seq[Change] = {
    items
      .map(x => (x, x.mapping))
      .map {
        case (tx, x: DomainParametersState) =>
          Change.TopologyDelay(tx.sequenced, tx.validFrom, x.parameters.topologyChangeDelay)
        case (tx, _) => Change.Other(tx.sequenced, tx.validFrom)
      }
      .sortBy(_.effective)
      .distinct
  }

  def apply[StoreID <: TopologyStoreId](
      storeId: StoreID,
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): TopologyStore[StoreID] = {
    val storeLoggerFactory = loggerFactory.append("store", storeId.toString)
    storage match {
      case _: MemoryStorage =>
        new InMemoryTopologyStore(storeId, storeLoggerFactory, timeouts)
      case dbStorage: DbStorage =>
        new DbTopologyStore(dbStorage, storeId, timeouts, storeLoggerFactory)
    }
  }

  lazy val initialParticipantDispatchingSet = Set(
    TopologyMapping.Code.DomainTrustCertificate,
    TopologyMapping.Code.OwnerToKeyMapping,
    // TODO(#14060) - potentially revisit this once we implement TopologyStore.filterInitialParticipantDispatchingTransactions
    TopologyMapping.Code.NamespaceDelegation,
    TopologyMapping.Code.IdentifierDelegation,
    TopologyMapping.Code.DecentralizedNamespaceDefinition,
  )

  def filterInitialParticipantDispatchingTransactions(
      participantId: ParticipantId,
      domainId: DomainId,
      transactions: Seq[GenericStoredTopologyTransaction],
  ): Seq[GenericSignedTopologyTransaction] = {
    // TODO(#14060): Extend filtering along the lines of:
    //  TopologyStore.filterInitialParticipantDispatchingTransactions
    transactions.map(_.transaction).collect {
      case tx @ SignedTopologyTransaction(
            TopologyTransaction(_, _, DomainTrustCertificate(`participantId`, `domainId`, _, _)),
            _,
            _,
          ) =>
        tx
      case tx @ SignedTopologyTransaction(
            TopologyTransaction(_, _, OwnerToKeyMapping(`participantId`, _, _)),
            _,
            _,
          ) =>
        tx
      case tx @ SignedTopologyTransaction(
            TopologyTransaction(_, _, NamespaceDelegation(ns, _, _)),
            _,
            _,
          ) if ns == participantId.namespace =>
        tx
      case tx @ SignedTopologyTransaction(
            TopologyTransaction(_, _, IdentifierDelegation(uid, _)),
            _,
            _,
          ) if uid == participantId.uid =>
        tx
      case tx @ SignedTopologyTransaction(
            TopologyTransaction(_, _, _: DecentralizedNamespaceDefinition),
            _,
            _,
          ) =>
        tx
    }
  }

  /** convenience method waiting until the last eligible transaction inserted into the source store has been dispatched successfully to the target domain */
  def awaitTxObserved(
      client: DomainTopologyClient,
      transaction: GenericSignedTopologyTransaction,
      target: TopologyStore[?],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Boolean] = {
    client.await(
      // we know that the transaction is stored and effective once we find it in the target
      // domain store and once the effective time (valid from) is smaller than the client timestamp
      sp => target.findStored(sp.timestamp, transaction, includeRejected = true).map(_.nonEmpty),
      timeout,
    )
  }
}

sealed trait TimeQuery {
  def toProtoV30: topoV30.BaseQuery.TimeQuery
}

object TimeQuery {
  case object HeadState extends TimeQuery {
    override def toProtoV30: topoV30.BaseQuery.TimeQuery =
      topoV30.BaseQuery.TimeQuery.HeadState(com.google.protobuf.empty.Empty())
  }
  final case class Snapshot(asOf: CantonTimestamp) extends TimeQuery {
    override def toProtoV30: topoV30.BaseQuery.TimeQuery =
      topoV30.BaseQuery.TimeQuery.Snapshot(asOf.toProtoTimestamp)
  }
  final case class Range(from: Option[CantonTimestamp], until: Option[CantonTimestamp])
      extends TimeQuery {
    override def toProtoV30: topoV30.BaseQuery.TimeQuery = topoV30.BaseQuery.TimeQuery.Range(
      topoV30.BaseQuery.TimeRange(from.map(_.toProtoTimestamp), until.map(_.toProtoTimestamp))
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
        CantonTimestamp.fromProtoTimestamp(value).map(Snapshot)
      case topoV30.BaseQuery.TimeQuery.HeadState(_) => Right(HeadState)
      case topoV30.BaseQuery.TimeQuery.Range(value) =>
        for {
          fromO <- value.from.traverse(CantonTimestamp.fromProtoTimestamp)
          toO <- value.until.traverse(CantonTimestamp.fromProtoTimestamp)
        } yield Range(fromO, toO)
    }
}

trait PackageDependencyResolverUS {

  def packageDependencies(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]]

}
