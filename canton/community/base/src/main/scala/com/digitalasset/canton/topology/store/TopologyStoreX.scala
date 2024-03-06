// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
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
import com.digitalasset.canton.topology.store.StoredTopologyTransactionX.GenericStoredTopologyTransactionX
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.{
  GenericStoredTopologyTransactionsX,
  PositiveStoredTopologyTransactionsX,
}
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.Duplicate
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.db.DbTopologyStoreX
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreX
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.{
  GenericTopologyTransactionX,
  TxHash,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

final case class StoredTopologyTransactionX[+Op <: TopologyChangeOpX, +M <: TopologyMappingX](
    sequenced: SequencedTime,
    validFrom: EffectiveTime,
    validUntil: Option[EffectiveTime],
    transaction: SignedTopologyTransactionX[Op, M],
) extends DelegatedTopologyTransactionLike[Op, M]
    with PrettyPrinting {
  override protected def transactionLikeDelegate: TopologyTransactionLike[Op, M] = transaction

  override def pretty: Pretty[StoredTopologyTransactionX.this.type] =
    prettyOfClass(
      unnamedParam(_.transaction),
      param("sequenced", _.sequenced.value),
      param("validFrom", _.validFrom.value),
      paramIfDefined("validUntil", _.validUntil.map(_.value)),
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectMapping[TargetMapping <: TopologyMappingX: ClassTag] = transaction
    .selectMapping[TargetMapping]
    .map(_ => this.asInstanceOf[StoredTopologyTransactionX[Op, TargetMapping]])

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectOp[TargetOp <: TopologyChangeOpX: ClassTag] = transaction
    .selectOp[TargetOp]
    .map(_ => this.asInstanceOf[StoredTopologyTransactionX[TargetOp, M]])
}

object StoredTopologyTransactionX {
  type GenericStoredTopologyTransactionX =
    StoredTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]
}

final case class ValidatedTopologyTransactionX[+Op <: TopologyChangeOpX, +M <: TopologyMappingX](
    transaction: SignedTopologyTransactionX[Op, M],
    rejectionReason: Option[TopologyTransactionRejection] = None,
    expireImmediately: Boolean = false,
) extends DelegatedTopologyTransactionLike[Op, M]
    with PrettyPrinting {

  override protected def transactionLikeDelegate: TopologyTransactionLike[Op, M] = transaction

  def nonDuplicateRejectionReason: Option[TopologyTransactionRejection] = rejectionReason match {
    case Some(Duplicate(_)) => None
    case otherwise => otherwise
  }

  def collectOfMapping[TargetM <: TopologyMappingX: ClassTag]
      : Option[ValidatedTopologyTransactionX[Op, TargetM]] =
    transaction.selectMapping[TargetM].map(tx => copy[Op, TargetM](transaction = tx))

  def collectOf[TargetO <: TopologyChangeOpX: ClassTag, TargetM <: TopologyMappingX: ClassTag]
      : Option[ValidatedTopologyTransactionX[TargetO, TargetM]] =
    transaction.select[TargetO, TargetM].map(tx => copy[TargetO, TargetM](transaction = tx))

  override def pretty: Pretty[ValidatedTopologyTransactionX.this.type] =
    prettyOfClass(
      unnamedParam(_.transaction),
      paramIfDefined("rejectionReason", _.rejectionReason),
      paramIfTrue("expireImmediately", _.expireImmediately),
    )
}

object ValidatedTopologyTransactionX {
  type GenericValidatedTopologyTransactionX =
    ValidatedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]
}
abstract class TopologyStoreX[+StoreID <: TopologyStoreId](implicit
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
  ): Future[Seq[TopologyStoreX.Change]]

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
  ): Future[Seq[GenericSignedTopologyTransactionX]]

  def findProposalsByTxHash(asOfExclusive: EffectiveTime, hashes: NonEmpty[Set[TxHash]])(implicit
      traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransactionX]]

  def findTransactionsForMapping(asOfExclusive: EffectiveTime, hashes: NonEmpty[Set[MappingHash]])(
      implicit traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransactionX]]

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
      types: Seq[TopologyMappingX.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit
      traceContext: TraceContext
  ): Future[PositiveStoredTopologyTransactionsX]

  /** add validated topology transaction as is to the topology transaction table */
  def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removeMapping: Map[MappingHash, PositiveInt],
      removeTxs: Set[TxHash],
      additions: Seq[GenericValidatedTopologyTransactionX],
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  @VisibleForTesting
  protected[topology] def dumpStoreContent()(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX]

  /** store an initial set of topology transactions as given into the store */
  def bootstrap(snapshot: GenericStoredTopologyTransactionsX)(implicit
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
      // TODO(#14048) - consider removing `recentTimestampO` and moving callers to TimeQueryX.Snapshot
      recentTimestampO: Option[CantonTimestamp],
      op: Option[TopologyChangeOpX],
      typ: Option[TopologyMappingX.Code],
      idFilter: String,
      namespaceOnly: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]]

  def inspectKnownParties(
      timestamp: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]]

  def findFirstMediatorStateForMediator(
      mediatorId: MediatorId
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransactionX[TopologyChangeOpX.Replace, MediatorDomainStateX]]]

  def findFirstTrustCertificateForParticipant(
      participant: ParticipantId
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransactionX[TopologyChangeOpX.Replace, DomainTrustCertificateX]]]

  def findEssentialStateForMember(
      member: Member,
      asOfInclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[GenericStoredTopologyTransactionsX]

  protected def signedTxFromStoredTx(
      storedTx: GenericStoredTopologyTransactionX
  ): SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX] = storedTx.transaction

  def providesAdditionalSignatures(
      transaction: GenericSignedTopologyTransactionX
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
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransactionX]]

  def findDispatchingTransactionsAfter(
      timestampExclusive: CantonTimestamp,
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX]

  def findStoredForVersion(
      asOfExclusive: CantonTimestamp,
      transaction: GenericTopologyTransactionX,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransactionX]]

  final def exists(transaction: GenericSignedTopologyTransactionX)(implicit
      traceContext: TraceContext
  ): Future[Boolean] = findStored(CantonTimestamp.MaxValue, transaction).map(
    _.exists(signedTxFromStoredTx(_) == transaction)
  )

  def findStored(
      asOfExclusive: CantonTimestamp,
      transaction: GenericSignedTopologyTransactionX,
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransactionX]]
}

object TopologyStoreX {

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
      items: Seq[StoredTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
  ): Seq[Change] = {
    items
      .map(x => (x, x.mapping))
      .map {
        case (tx, x: DomainParametersStateX) =>
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
  ): TopologyStoreX[StoreID] = {
    val storeLoggerFactory = loggerFactory.append("store", storeId.toString)
    storage match {
      case _: MemoryStorage =>
        new InMemoryTopologyStoreX(storeId, storeLoggerFactory, timeouts)
      case dbStorage: DbStorage =>
        new DbTopologyStoreX(dbStorage, storeId, timeouts, storeLoggerFactory)
    }
  }

  lazy val initialParticipantDispatchingSet = Set(
    TopologyMappingX.Code.DomainTrustCertificateX,
    TopologyMappingX.Code.OwnerToKeyMappingX,
    // TODO(#14060) - potentially revisit this once we implement TopologyStoreX.filterInitialParticipantDispatchingTransactions
    TopologyMappingX.Code.NamespaceDelegationX,
    TopologyMappingX.Code.IdentifierDelegationX,
    TopologyMappingX.Code.DecentralizedNamespaceDefinitionX,
  )

  def filterInitialParticipantDispatchingTransactions(
      participantId: ParticipantId,
      domainId: DomainId,
      transactions: Seq[GenericStoredTopologyTransactionX],
  ): Seq[GenericSignedTopologyTransactionX] = {
    // TODO(#14060): Extend filtering along the lines of:
    //  TopologyStore.filterInitialParticipantDispatchingTransactions
    transactions.map(_.transaction).collect {
      case tx @ SignedTopologyTransactionX(
            TopologyTransactionX(_, _, DomainTrustCertificateX(`participantId`, `domainId`, _, _)),
            _,
            _,
          ) =>
        tx
      case tx @ SignedTopologyTransactionX(
            TopologyTransactionX(_, _, OwnerToKeyMappingX(`participantId`, _, _)),
            _,
            _,
          ) =>
        tx
      case tx @ SignedTopologyTransactionX(
            TopologyTransactionX(_, _, NamespaceDelegationX(ns, _, _)),
            _,
            _,
          ) if ns == participantId.uid.namespace =>
        tx
      case tx @ SignedTopologyTransactionX(
            TopologyTransactionX(_, _, IdentifierDelegationX(uid, _)),
            _,
            _,
          ) if uid == participantId.uid =>
        tx
      case tx @ SignedTopologyTransactionX(
            TopologyTransactionX(_, _, _: DecentralizedNamespaceDefinitionX),
            _,
            _,
          ) =>
        tx
    }
  }

  /** convenience method waiting until the last eligible transaction inserted into the source store has been dispatched successfully to the target domain */
  def awaitTxObserved(
      client: DomainTopologyClient,
      transaction: GenericSignedTopologyTransactionX,
      target: TopologyStoreX[?],
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
  object HeadState extends TimeQuery {
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
