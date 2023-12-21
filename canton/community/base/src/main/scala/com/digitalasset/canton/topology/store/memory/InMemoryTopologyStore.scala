// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import cats.syntax.functorFilter.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.PublicKey
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStore.InsertTransaction
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Add, Positive, Remove}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, blocking}

class InMemoryPartyMetadataStore extends PartyMetadataStore {

  private val store = TrieMap[PartyId, PartyMetadata]()

  override def insertOrUpdatePartyMetadata(
      partyId: PartyId,
      participantId: Option[ParticipantId],
      displayName: Option[DisplayName],
      effectiveTimestamp: CantonTimestamp,
      submissionId: String255,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    store
      .put(
        partyId,
        PartyMetadata(partyId, displayName, participantId)(
          effectiveTimestamp = effectiveTimestamp,
          submissionId = submissionId,
        ),
      )
      .discard
    Future.unit

  }

  override def metadataForParty(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): Future[Option[PartyMetadata]] =
    Future.successful(store.get(partyId))

  override def markNotified(
      metadata: PartyMetadata
  )(implicit traceContext: TraceContext): Future[Unit] = {
    store.get(metadata.partyId) match {
      case Some(cur) if cur.effectiveTimestamp == metadata.effectiveTimestamp =>
        store
          .put(
            metadata.partyId,
            metadata.copy()(
              effectiveTimestamp = metadata.effectiveTimestamp,
              submissionId = metadata.submissionId,
              notified = true,
            ),
          )
          .discard
      case _ => ()
    }
    Future.unit
  }

  override def fetchNotNotified()(implicit traceContext: TraceContext): Future[Seq[PartyMetadata]] =
    Future.successful(store.values.filterNot(_.notified).toSeq)

  override def close(): Unit = ()
}

trait InMemoryTopologyStoreCommon[+StoreId <: TopologyStoreId] extends NamedLogging {
  this: TopologyStoreCommon[StoreId, ?, ?, ?] =>

  private val watermark = new AtomicReference[Option[CantonTimestamp]](None)

  @nowarn("cat=unused")
  override def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    Future.successful(watermark.get())

  override def updateDispatchingWatermark(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = {
    watermark.getAndSet(Some(timestamp)) match {
      case Some(old) if old > timestamp =>
        logger.error(
          s"Topology dispatching watermark is running backwards! new=$timestamp, old=${old}"
        )
      case _ => ()
    }
    Future.unit
  }

}

class InMemoryTopologyStore[+StoreId <: TopologyStoreId](
    val storeId: StoreId,
    val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
)(implicit val ec: ExecutionContext)
    extends TopologyStore[StoreId]
    with InMemoryTopologyStoreCommon[StoreId]
    with NamedLogging {

  private case class TopologyStoreEntry[+Op <: TopologyChangeOp](
      operation: Op,
      transaction: SignedTopologyTransaction[Op],
      sequenced: SequencedTime,
      from: EffectiveTime,
      until: Option[EffectiveTime],
      rejected: Option[String],
  ) {

    def toStoredTransaction: StoredTopologyTransaction[Op] =
      StoredTopologyTransaction(sequenced, from, until, transaction)

    def secondaryUid: Option[UniqueIdentifier] =
      transaction.transaction.element.mapping.secondaryUid

  }

  // contains Add, Remove and Replace
  private val topologyTransactionStore = ArrayBuffer[TopologyStoreEntry[TopologyChangeOp]]()
  // contains only (Add, Replace) transactions that are authorized
  private val topologyStateStore = ArrayBuffer[TopologyStoreEntry[Positive]]()

  override def append(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      transactions: Seq[ValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): Future[Unit] = blocking(synchronized {

    val (updates, appends) = TopologyStore.appends(effective.value, transactions)

    // UPDATE topology_transactions SET valid_until = ts WHERE store_id = ... AND valid_until is NULL AND valid_from < ts AND path_id IN (updates)
    updates.foreach { upd =>
      val idx =
        topologyTransactionStore.indexWhere(x =>
          x.transaction.uniquePath == upd && x.until.isEmpty && x.from.value < effective.value
        )
      if (idx > -1) {
        val item = topologyTransactionStore(idx)
        topologyTransactionStore.update(idx, item.copy(until = Some(effective)))
      }
    }
    // INSERT INTO topology_transactions (path_id, store_id, valid_from, transaction_type, operation, instance) VALUES inserts ON CONFLICT DO NOTHING
    appends.foreach { case InsertTransaction(trans, validUntil, rejectionReason) =>
      val operation = trans.operation

      // be idempotent
      if (
        !topologyTransactionStore.exists(x =>
          x.transaction.uniquePath == trans.uniquePath && x.from == effective && x.operation == operation
        )
      ) {
        topologyTransactionStore.append(
          TopologyStoreEntry(
            operation,
            trans,
            sequenced,
            effective,
            validUntil.map(EffectiveTime(_)),
            rejectionReason.map(_.asString),
          )
        )
      }
    }
    Future.unit
  })

  private def asOfFilter(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
  ): (CantonTimestamp, Option[CantonTimestamp]) => Boolean =
    if (asOfInclusive) { case (validFrom, validUntil) =>
      validFrom <= asOf && validUntil.forall(until => asOf < until)
    }
    else { case (validFrom, validUntil) =>
      validFrom < asOf && validUntil.forall(until => asOf <= until)
    }

  override def timestamp(
      useStateStore: Boolean
  )(implicit traceContext: TraceContext): Future[Option[(SequencedTime, EffectiveTime)]] =
    Future.successful(
      (if (useStateStore) topologyStateStore else topologyTransactionStore).lastOption.map(x =>
        (x.sequenced, x.from)
      )
    )

  private def filteredState(
      table: Seq[TopologyStoreEntry[TopologyChangeOp]],
      filter: TopologyStoreEntry[TopologyChangeOp] => Boolean,
      includeRejected: Boolean = false,
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] =
    Future.successful(
      StoredTopologyTransactions(
        table.collect {
          case entry if filter(entry) && (entry.rejected.isEmpty || includeRejected) =>
            entry.toStoredTransaction
        }
      )
    )

  override def headTransactions(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[Positive]] =
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      x => x.until.isEmpty,
    ).map(_.collectOfType[Positive])

  /** finds transactions in the local store that would remove the topology state elements
    */
  override def findRemovalTransactionForMappings(
      mappings: Set[TopologyStateElement[TopologyMapping]]
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[SignedTopologyTransaction[Remove]]] =
    Future.successful(
      blocking(synchronized(topologyTransactionStore.toSeq))
        .map(_.transaction)
        .mapFilter(TopologyChangeOp.select[Remove])
        .collect {
          case sit @ SignedTopologyTransaction(TopologyStateUpdate(_, element), _, _)
              if mappings.contains(element) =>
            sit
        }
    )

  override def findPositiveTransactionsForMapping(
      mapping: TopologyMapping
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[SignedTopologyTransaction[Positive]]] =
    Future.successful(
      blocking(synchronized(topologyTransactionStore.toSeq))
        .collect { case entry if entry.until.isEmpty => entry.transaction }
        .mapFilter(TopologyChangeOp.select[Positive])
        .collect {
          case sit if sit.transaction.element.mapping == mapping => sit
        }
    )

  override def allTransactions(includeRejected: Boolean = false)(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] =
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      _ => true,
      includeRejected,
    )

  override def findStored(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransaction[TopologyChangeOp]]] =
    allTransactions(includeRejected).map(_.result.find(_.transaction == transaction))

  override def findStoredNoSignature(transaction: TopologyTransaction[TopologyChangeOp])(implicit
      traceContext: TraceContext
  ): Future[Seq[StoredTopologyTransaction[TopologyChangeOp]]] =
    allTransactions().map(
      _.result.filter(_.transaction.transaction.element.mapping == transaction.element.mapping)
    )

  override def findStoredForVersion(
      transaction: TopologyTransaction[TopologyChangeOp],
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransaction[TopologyChangeOp]]] =
    allTransactions().map(
      _.result.find(tx =>
        tx.transaction.transaction == transaction && tx.transaction.representativeProtocolVersion == TopologyTransaction
          .protocolVersionRepresentativeFor(protocolVersion)
      )
    )

  override def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit traceContext: TraceContext): Future[PositiveStoredTopologyTransactions] =
    findPositiveTransactionsInStore(
      topologyTransactionStore,
      asOf,
      asOfInclusive,
      includeSecondary,
      types,
      filterUid,
      filterNamespace,
    )

  /** query interface used by [[com.digitalasset.canton.topology.client.StoreBasedTopologySnapshot]] */
  override def findStateTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit
      traceContext: TraceContext
  ): Future[PositiveStoredTopologyTransactions] =
    findPositiveTransactionsInStore(
      topologyStateStore,
      asOf,
      asOfInclusive,
      includeSecondary,
      types,
      filterUid,
      filterNamespace,
    )

  private def findTransactionsInStore[Op <: TopologyChangeOp](
      store: ArrayBuffer[TopologyStoreEntry[Op]],
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] = {
    val timeFilter = asOfFilter(asOf, asOfInclusive)
    def pathFilter(path: UniquePath): Boolean = {
      if (filterUid.isEmpty && filterNamespace.isEmpty)
        true
      else {
        path.maybeUid.exists(uid => filterUid.exists(_.contains(uid))) ||
        filterNamespace.exists(_.contains(path.namespace))
      }
    }
    // filter for secondary uids (required for cascading updates)
    def secondaryFilter(entry: TopologyStoreEntry[TopologyChangeOp]): Boolean =
      includeSecondary &&
        entry.secondaryUid.exists(uid =>
          filterNamespace.exists(_.contains(uid.namespace)) ||
            filterUid.exists(_.contains(uid))
        )

    filteredState(
      blocking(synchronized { store.toSeq }),
      entry => {
        timeFilter(entry.from.value, entry.until.map(_.value)) &&
        types.contains(entry.transaction.uniquePath.dbType) &&
        (pathFilter(entry.transaction.uniquePath) || secondaryFilter(entry))
      },
    )
  }

  private def findPositiveTransactionsInStore[Op <: TopologyChangeOp](
      store: ArrayBuffer[TopologyStoreEntry[Op]],
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  ): Future[PositiveStoredTopologyTransactions] =
    findTransactionsInStore(
      store = store,
      asOf = asOf,
      asOfInclusive = asOfInclusive,
      includeSecondary = includeSecondary,
      types = types,
      filterUid = filterUid,
      filterNamespace = filterNamespace,
    ).map(_.positiveTransactions)

  /** query interface used by DomainTopologyManager to find the set of initial keys */
  override def findInitialState(
      id: DomainTopologyManagerId
  )(implicit traceContext: TraceContext): Future[Map[Member, Seq[PublicKey]]] = {
    val res = topologyTransactionStore.foldLeft((false, Map.empty[Member, Seq[PublicKey]])) {
      case ((false, acc), TopologyStoreEntry(Add, transaction, _, _, _, None)) =>
        TopologyStore.findInitialStateAccumulator(id.uid, acc, transaction)
      case (acc, _) => acc
    }
    Future.successful(res._2)
  }

  /** update active topology transaction to the active topology transaction table
    *
    * active means that for the key authorizing the transaction, there is a connected path to reach the root certificate
    */
  override def updateState(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      deactivate: Seq[UniquePath],
      positive: Seq[SignedTopologyTransaction[Positive]],
  )(implicit traceContext: TraceContext): Future[Unit] = {

    blocking(synchronized {
      val deactivateS = deactivate.toSet
      // UPDATE topology_state SET valid_until = ts WHERE store_id = ... AND valid_from < ts AND valid_until is NULL and path_id in Deactivate)
      deactivate.foreach { _up =>
        val idx = topologyStateStore.indexWhere(entry =>
          entry.from.value < effective.value && entry.until.isEmpty &&
            deactivateS.contains(entry.transaction.uniquePath)
        )
        if (idx != -1) {
          val item = topologyStateStore(idx)
          topologyStateStore.update(idx, item.copy(until = Some(effective)))
        }
      }

      // INSERT IGNORE (sit)
      positive.foreach { sit =>
        if (
          !topologyStateStore.exists(x =>
            x.transaction.uniquePath == sit.uniquePath && x.from.value == effective.value && x.operation == sit.operation
          )
        ) {
          topologyStateStore.append(
            TopologyStoreEntry(
              sit.operation,
              sit,
              sequenced,
              effective,
              None,
              None,
            )
          )
        }
      }
    })
    Future.unit
  }

  override def inspectKnownParties(
      timestamp: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]] = {
    def filter(entry: TopologyStoreEntry[Positive]): Boolean = {
      // active
      entry.from.value < timestamp && entry.until.forall(until => timestamp <= until.value) &&
      // not rejected
      entry.rejected.isEmpty &&
      // matches either a party to participant mapping (with appropriate filters)
      ((entry.transaction.uniquePath.dbType == DomainTopologyTransactionType.PartyToParticipant &&
        entry.transaction.uniquePath.maybeUid.exists(_.toProtoPrimitive.startsWith(filterParty)) &&
        entry.secondaryUid.exists(_.toProtoPrimitive.startsWith(filterParticipant))) ||
        // or matches a participant with appropriate filters
        (entry.transaction.uniquePath.dbType == DomainTopologyTransactionType.ParticipantState &&
          entry.transaction.uniquePath.maybeUid
            .exists(_.toProtoPrimitive.startsWith(filterParty)) &&
          entry.transaction.uniquePath.maybeUid
            .exists(_.toProtoPrimitive.startsWith(filterParticipant))))
    }
    val topologyStateStoreSeq = blocking(synchronized(topologyStateStore.toSeq))
    Future.successful(
      topologyStateStoreSeq
        .foldLeft(Set.empty[PartyId]) {
          case (acc, elem) if acc.size >= limit || !filter(elem) => acc
          case (acc, elem) => elem.transaction.uniquePath.maybeUid.fold(acc)(x => acc + PartyId(x))
        }
    )
  }

  /** query optimized for inspection */
  override def inspect(
      stateStore: Boolean,
      timeQuery: TimeQuery,
      recentTimestampO: Option[CantonTimestamp],
      ops: Option[TopologyChangeOp],
      typ: Option[DomainTopologyTransactionType],
      idFilter: String,
      namespaceOnly: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] = {
    val store = if (stateStore) topologyStateStore else topologyTransactionStore
    def mkAsOfFlt(asOf: CantonTimestamp): TopologyStoreEntry[TopologyChangeOp] => Boolean = entry =>
      asOfFilter(asOf, asOfInclusive = false)(entry.from.value, entry.until.map(_.value))
    val filter1: TopologyStoreEntry[TopologyChangeOp] => Boolean = timeQuery match {
      case TimeQuery.HeadState =>
        // use recent timestamp to avoid race conditions (as we are looking
        // directly into the store, while the recent time still needs to propagate)
        recentTimestampO.map(mkAsOfFlt).getOrElse(entry => entry.until.isEmpty)
      case TimeQuery.Snapshot(asOf) => mkAsOfFlt(asOf)
      case TimeQuery.Range(from, until) =>
        entry =>
          from.forall(ts => entry.from.value >= ts) && until.forall(ts => entry.from.value <= ts)
    }

    val filter2: TopologyStoreEntry[TopologyChangeOp] => Boolean = entry =>
      ops.forall(_ == entry.operation)

    val filter3: TopologyStoreEntry[TopologyChangeOp] => Boolean = {
      if (idFilter.isEmpty) _ => true
      else if (namespaceOnly) { entry =>
        entry.transaction.uniquePath.namespace.fingerprint.unwrap.startsWith(idFilter)
      } else {
        val splitted = idFilter.split(SafeSimpleString.delimiter)
        val prefix = splitted(0)
        if (splitted.lengthCompare(1) > 0) {
          val suffix = splitted(1)
          (entry: TopologyStoreEntry[TopologyChangeOp]) =>
            entry.transaction.uniquePath.maybeUid.forall(_.id.unwrap.startsWith(prefix)) &&
              entry.transaction.uniquePath.namespace.fingerprint.unwrap.startsWith(suffix)
        } else { entry =>
          entry.transaction.uniquePath.maybeUid.forall(_.id.unwrap.startsWith(prefix))
        }
      }
    }
    filteredState(
      blocking(synchronized(store.toSeq)),
      entry =>
        typ.forall(_ == entry.transaction.uniquePath.dbType) && filter1(entry) && filter2(
          entry
        ) && filter3(entry),
    )
  }

  override def findUpcomingEffectiveChanges(asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TopologyStore.Change]] =
    Future.successful(
      TopologyStore.Change.accumulateUpcomingEffectiveChanges(
        blocking(synchronized(topologyTransactionStore.toSeq))
          .filter(_.from.value >= asOfInclusive)
          .map(_.toStoredTransaction)
      )
    )

  override def findDispatchingTransactionsAfter(
      timestampExclusive: CantonTimestamp,
      limit: Option[Int],
  )(implicit traceContext: TraceContext): Future[StoredTopologyTransactions[TopologyChangeOp]] =
    blocking(synchronized {
      val selected = topologyTransactionStore
        .filter(x =>
          x.from.value > timestampExclusive && (x.until.isEmpty || x.operation == TopologyChangeOp.Remove) && x.rejected.isEmpty
        )
        .map(_.toStoredTransaction)
        .toSeq
      Future.successful(StoredTopologyTransactions(limit.fold(selected)(selected.take)))
    })

  override def findParticipantOnboardingTransactions(
      participantId: ParticipantId,
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[SignedTopologyTransaction[TopologyChangeOp]]] = {
    val res = blocking(synchronized {
      topologyTransactionStore.filter(x =>
        x.until.isEmpty && TopologyStore.initialParticipantDispatchingSet.contains(
          x.transaction.uniquePath.dbType
        )
      )
    })

    TopologyStore.filterInitialParticipantDispatchingTransactions(
      participantId,
      domainId,
      this,
      loggerFactory,
      StoredTopologyTransactions(res.map(_.toStoredTransaction).toSeq),
      timeouts,
      futureSupervisor,
    )
  }

  override def findTsOfParticipantStateChangesBefore(
      beforeExclusive: CantonTimestamp,
      participantId: ParticipantId,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[CantonTimestamp]] = blocking(synchronized {
    val ret = topologyTransactionStore
      .filter(x =>
        x.from.value < beforeExclusive &&
          x.transaction.transaction.element.mapping.dbType == DomainTopologyTransactionType.ParticipantState &&
          x.transaction.uniquePath.maybeUid.contains(participantId.uid)
      )
      .map(_.from.value)
      .sorted(CantonTimestamp.orderCantonTimestamp.toOrdering.reverse)
      .take(limit)
    Future.successful(ret.toSeq)
  })

  override def findTransactionsInRange(
      asOfExclusive: CantonTimestamp,
      upToExclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[StoredTopologyTransactions[TopologyChangeOp]] =
    blocking(synchronized {
      val ret = topologyTransactionStore
        .filter(x =>
          x.from.value > asOfExclusive && x.from.value < upToExclusive && x.rejected.isEmpty
        )
        .map(_.toStoredTransaction)
      Future.successful(StoredTopologyTransactions(ret.toSeq))
    })

  override def onClosed(): Unit = ()

}
