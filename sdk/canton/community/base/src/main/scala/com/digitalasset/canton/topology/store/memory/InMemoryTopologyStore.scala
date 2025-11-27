// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.crypto.topology.TopologyStateHash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.{
  GenericStoredTopologyTransactions,
  NegativeStoredTopologyTransactions,
  PositiveStoredTopologyTransactions,
}
import com.digitalasset.canton.topology.store.TopologyStore.{
  EffectiveStateChange,
  TopologyStoreDeactivations,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  GenericTopologyTransaction,
  TxHash,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, blocking}
import scala.math.Ordering.Implicits.*

class InMemoryTopologyStore[+StoreId <: TopologyStoreId](
    val storeId: StoreId,
    override val protocolVersion: ProtocolVersion,
    val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit ec: ExecutionContext)
    extends TopologyStore[StoreId]
    with NamedLogging {

  override def onClosed(): Unit = ()

  private case class TopologyStoreEntry(
      transaction: GenericSignedTopologyTransaction,
      sequenced: SequencedTime,
      from: EffectiveTime,
      batchIdx: Int,
      rejected: Option[String300],
      until: Option[EffectiveTime],
  ) extends DelegatedTopologyTransactionLike[TopologyChangeOp, TopologyMapping] {

    override protected def transactionLikeDelegate
        : TopologyTransactionLike[TopologyChangeOp, TopologyMapping] = transaction

    def toStoredTransaction: StoredTopologyTransaction[TopologyChangeOp, TopologyMapping] =
      StoredTopologyTransaction(sequenced, from, until, transaction, rejected)
  }

  private val topologyTransactionStore = ArrayBuffer[TopologyStoreEntry]()
  // the unique key is defined in the database migration file for the common_topology_transactions table
  private val topologyTransactionsStoreUniqueIndex = mutable.Set.empty[
    (
        EffectiveTime,
        Int,
    )
  ]
  private val watermark = new AtomicReference[Option[CantonTimestamp]](None)

  def findLatestTransactionsAndProposalsByTxHash(hashes: Set[TxHash])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] =
    if (hashes.isEmpty) FutureUnlessShutdown.pure(Seq.empty)
    else
      filteredState(
        blocking(synchronized(topologyTransactionStore.toSeq)),
        filter = entry => hashes.contains(entry.hash),
      ).map(_.collectLatestByTxHash.result.map(_.transaction))

  override def findProposalsByTxHash(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[TxHash]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] =
    findFilter(
      asOfExclusive,
      entry => hashes.contains(entry.hash) && entry.transaction.isProposal,
    )

  private def findFilter(
      asOfExclusive: EffectiveTime,
      filter: TopologyStoreEntry => Boolean,
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] = FutureUnlessShutdown.pure {
    blocking {
      synchronized {
        topologyTransactionStore
          .filter(x =>
            x.from.value < asOfExclusive.value
              && x.rejected.isEmpty
              && x.until.forall(_.value >= asOfExclusive.value)
              && filter(x)
          )
          .map(_.transaction)
          .toSeq
      }
    }
  }

  override def findTransactionsForMapping(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[MappingHash]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] =
    findFilter(
      asOfExclusive,
      entry =>
        !entry.transaction.isProposal && hashes.contains(
          entry.mapping.uniqueKey
        ),
    )

  override def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removals: TopologyStoreDeactivations,
      additions: Seq[GenericValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    def mustRemove(tx: TopologyStoreEntry): Boolean =
      removals.get(tx.mapping.uniqueKey).fold(false) { case (serialO, txSet) =>
        serialO.exists(_ >= tx.serial) || txSet.contains(tx.hash)
      }

    blocking {
      synchronized {
        // transactionally
        // UPDATE txs SET valid_until = effective WHERE effective < $effective AND valid_from is NULL
        //    AND ((mapping_key_hash IN $removeMapping AND serial_counter <= $serial) OR (tx_hash IN $removeTxs))
        // INSERT IGNORE DUPLICATES (...)
        topologyTransactionStore.zipWithIndex.foreach { case (tx, idx) =>
          if (tx.from.value < effective.value && tx.until.isEmpty && mustRemove(tx)) {
            topologyTransactionStore.update(idx, tx.copy(until = Some(effective)))
          }
        }
        additions.zipWithIndex.foreach { case (tx, batchIdx) =>
          val uniqueKey = (
            effective,
            batchIdx,
          )
          if (topologyTransactionsStoreUniqueIndex.add(uniqueKey))
            topologyTransactionStore.append(
              TopologyStoreEntry(
                tx.transaction,
                sequenced,
                from = effective,
                batchIdx = batchIdx,
                rejected = tx.rejectionReason.map(_.asString300),
                until = Option.when(
                  tx.rejectionReason.nonEmpty || tx.expireImmediately
                )(effective),
              )
            )
          else {
            topologyTransactionStore
              .find(tt => tt.from == effective && tt.batchIdx == batchIdx)
              .foreach { entry =>
                if (
                  entry.transaction != tx.transaction || entry.rejected != tx.rejectionReason
                    .map(_.asString300)
                ) {
                  throw new IllegalArgumentException(
                    s"Corrupt topology store.\nFOUND: $entry\nADDING$tx"
                  )
                }
              }
          }
        }
      }
    }
    FutureUnlessShutdown.unit
  }

  override def bulkInsert(
      initialSnapshot: GenericStoredTopologyTransactions
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    initialSnapshot.result
      .foldLeft((CantonTimestamp.MinValue, -1)) { case ((prevTs, prevBatch), tx) =>
        val batchIdx = if (prevTs < tx.validFrom.value || prevBatch == -1) 0 else prevBatch + 1
        val uniqueKey = (
          tx.validFrom,
          batchIdx,
        )
        if (topologyTransactionsStoreUniqueIndex.add(uniqueKey)) {
          topologyTransactionStore.append(
            TopologyStoreEntry(
              tx.transaction,
              tx.sequenced,
              from = tx.validFrom,
              batchIdx = batchIdx,
              until = tx.validUntil,
              rejected = tx.rejectionReason,
            )
          )
        }
        (tx.validFrom.value, batchIdx)
      }
      .discard
    FutureUnlessShutdown.unit
  }

  @VisibleForTesting
  override protected[topology] def dumpStoreContent()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = {
    val entries = blocking {
      synchronized {
        logger.debug(
          topologyTransactionStore
            .map(_.toString)
            .mkString("Topology Store Content[", ", ", "]")
        )
        topologyTransactionStore.toSeq

      }
    }
    FutureUnlessShutdown.pure(
      StoredTopologyTransactions(
        entries.map(e =>
          StoredTopologyTransaction(e.sequenced, e.from, e.until, e.transaction, e.rejected)
        )
      )
    )
  }

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

  private def filteredState(
      table: Seq[TopologyStoreEntry],
      filter: TopologyStoreEntry => Boolean,
      includeRejected: Boolean = false,
  ): FutureUnlessShutdown[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]] =
    FutureUnlessShutdown.pure(
      StoredTopologyTransactions(
        table.collect {
          case entry if filter(entry) && (entry.rejected.isEmpty || includeRejected) =>
            entry.toStoredTransaction
        }
      )
    )

  override def inspectKnownParties(
      asOfExclusive: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] = {

    def filter(entry: TopologyStoreEntry): Boolean =
      // active
      entry.from.value < asOfExclusive &&
        entry.until.forall(until => asOfExclusive <= until.value) &&
        // not rejected
        entry.rejected.isEmpty &&
        // is not a proposal
        !entry.transaction.isProposal &&
        // is of type Replace
        entry.operation == TopologyChangeOp.Replace

    val transactions = blocking(synchronized(topologyTransactionStore.toSeq))
    val mappings = transactions.filter(filter).map(_.mapping)

    FutureUnlessShutdown.pure(
      TopologyStore.determineValidParties(
        mappings,
        filterParty,
        filterParticipant,
        limit,
      )
    )

  }

  override def inspect(
      proposals: Boolean,
      timeQuery: TimeQuery,
      asOfExclusiveO: Option[CantonTimestamp],
      op: Option[TopologyChangeOp],
      types: Seq[TopologyMapping.Code],
      idFilter: Option[String],
      namespaceFilter: Option[String],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]] = {
    def mkAsOfFilter(asOf: CantonTimestamp): TopologyStoreEntry => Boolean = entry =>
      asOfFilter(asOf, asOfInclusive = false)(entry.from.value, entry.until.map(_.value))

    val filter1: TopologyStoreEntry => Boolean = timeQuery match {
      case TimeQuery.HeadState =>
        // use recent timestamp to avoid race conditions (as we are looking
        // directly into the store, while the recent time still needs to propagate)
        asOfExclusiveO.map(mkAsOfFilter).getOrElse(entry => entry.until.isEmpty)
      case TimeQuery.Snapshot(asOf) => mkAsOfFilter(asOf)
      case TimeQuery.Range(from, until) =>
        entry =>
          from.forall(ts => entry.from.value >= ts) && until.forall(ts => entry.from.value <= ts)
    }

    val filter2: TopologyStoreEntry => Boolean = entry => op.forall(_ == entry.operation)

    val filter3: TopologyStoreEntry => Boolean =
      idFilter match {
        case Some(value) if value.nonEmpty =>
          (entry: TopologyStoreEntry) =>
            entry.mapping.maybeUid.exists(_.identifier.unwrap.startsWith(value))
        case _ => _ => true
      }

    val filter4: TopologyStoreEntry => Boolean =
      namespaceFilter match {
        case Some(value) if value.nonEmpty =>
          (entry: TopologyStoreEntry) =>
            entry.mapping.namespace.fingerprint.unwrap.startsWith(value)
        case _ => _ => true
      }

    val filter0: TopologyStoreEntry => Boolean = entry =>
      types.isEmpty || types.contains(entry.mapping.code)

    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      entry =>
        filter0(entry) && (entry.transaction.isProposal == proposals) && filter1(entry) && filter2(
          entry
        ) && filter3(entry) && filter4(entry),
    )
  }

  override def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMapping.Code],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]],
      filterNamespace: Option[NonEmpty[Seq[Namespace]]],
      pagination: Option[(Option[UniqueIdentifier], Int)] = None,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PositiveStoredTopologyTransactions] =
    findTransactionsInStore(
      asOf,
      asOfInclusive,
      isProposal,
      types,
      filterUid,
      filterNamespace,
      pagination,
    ).map(
      _.collectOfType[TopologyChangeOp.Replace]
    )

  override def findNegativeTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMapping.Code],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]],
      filterNamespace: Option[NonEmpty[Seq[Namespace]]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[NegativeStoredTopologyTransactions] =
    findTransactionsInStore(asOf, asOfInclusive, isProposal, types, filterUid, filterNamespace).map(
      _.collectOfType[TopologyChangeOp.Remove]
    )

  private def findTransactionsInStore(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMapping.Code],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]],
      filterNamespace: Option[NonEmpty[Seq[Namespace]]],
      pagination: Option[(Option[UniqueIdentifier], Int)] = None,
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] = {
    val timeFilter = asOfFilter(asOf, asOfInclusive)
    def pathFilter(mapping: TopologyMapping): Boolean =
      if (filterUid.isEmpty && filterNamespace.isEmpty)
        true
      else {
        mapping.maybeUid.exists(uid => filterUid.exists(_.contains(uid))) ||
        filterNamespace.exists(_.contains(mapping.namespace))
      }

    implicit val orderingUid = UniqueIdentifier.orderingIdentifierThenNamespace
    implicit val orderingParticipantId = ParticipantId.orderingIdentifierThenNamespace

    def paginationFilter(mapping: TopologyMapping): Boolean = {
      val participantStartExclusive = pagination.flatMap(_._1)
      val matchesPage =
        participantStartExclusive.forall(bound => mapping.maybeUid.exists(_ > bound))
      matchesPage
    }

    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      entry => {
        timeFilter(entry.from.value, entry.until.map(_.value)) &&
        types.contains(entry.mapping.code) &&
        pathFilter(entry.mapping) &&
        entry.transaction.isProposal == isProposal &&
        paginationFilter(entry.mapping)
      },
    ).map { transactions =>
      pagination match {
        case None => transactions
        case Some((_, pageLimit)) =>
          StoredTopologyTransactions(
            transactions
              .collectOfMapping[VettedPackages]
              .collectLatestByUniqueKey
              .result
              .sortBy(_.mapping.participantId)
              .take(pageLimit)
          )
      }
    }
  }

  override def findFirstSequencerStateForSequencer(
      sequencerId: SequencerId
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Option[StoredTopologyTransaction[TopologyChangeOp.Replace, SequencerSynchronizerState]]
  ] =
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      entry =>
        !entry.transaction.isProposal &&
          entry.operation == TopologyChangeOp.Replace &&
          entry.mapping
            .select[SequencerSynchronizerState]
            .exists(m => m.allSequencers.contains(sequencerId)),
    ).map(
      _.collectOfType[TopologyChangeOp.Replace]
        .collectOfMapping[SequencerSynchronizerState]
        .result
        .minByOption(tx => (tx.serial, tx.validFrom))
    )

  override def findFirstMediatorStateForMediator(
      mediatorId: MediatorId
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Option[StoredTopologyTransaction[TopologyChangeOp.Replace, MediatorSynchronizerState]]
  ] =
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      entry =>
        !entry.transaction.isProposal &&
          entry.operation == TopologyChangeOp.Replace &&
          entry.mapping
            .select[MediatorSynchronizerState]
            .exists(m => m.observers.contains(mediatorId) || m.active.contains(mediatorId)),
    ).map(
      _.collectOfType[TopologyChangeOp.Replace]
        .collectOfMapping[MediatorSynchronizerState]
        .result
        .minByOption(tx => (tx.serial, tx.validFrom))
    )

  def findFirstTrustCertificateForParticipant(
      participant: ParticipantId
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Option[StoredTopologyTransaction[TopologyChangeOp.Replace, SynchronizerTrustCertificate]]
  ] =
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      entry =>
        !entry.transaction.isProposal &&
          entry.operation == TopologyChangeOp.Replace &&
          entry.mapping
            .select[SynchronizerTrustCertificate]
            .exists(_.participantId == participant),
    ).map(
      _.collectOfType[TopologyChangeOp.Replace]
        .collectOfMapping[SynchronizerTrustCertificate]
        .result
        .minByOption(tx => (tx.serial, tx.validFrom))
    )

  override def findEssentialStateAtSequencedTime(
      asOfInclusive: SequencedTime,
      includeRejected: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Source[GenericStoredTopologyTransaction, NotUsed] = {
    // asOfInclusive is the effective time of the transaction that onboarded the member.
    // 1. load all transactions with a sequenced time <= asOfInclusive, including proposals
    val dataF = filteredState(
      blocking(synchronized {
        topologyTransactionStore.toSeq
      }),
      entry => entry.sequenced <= asOfInclusive,
      includeRejected = includeRejected,
    ).map(
      // 2. transform the result such that the validUntil fields are set as they were at maxEffective time of the snapshot
      _.asSnapshotAtMaxEffectiveTime
    ).map(stored => Source(stored.result))
    PekkoUtil.futureSourceUS(dataF)
  }

  override def findEssentialStateHashAtSequencedTime(
      asOfInclusive: SequencedTime
  )(implicit materializer: Materializer, traceContext: TraceContext): FutureUnlessShutdown[Hash] =
    FutureUnlessShutdown.outcomeF(
      findEssentialStateAtSequencedTime(asOfInclusive, includeRejected = true)
        .runFold(TopologyStateHash.build())(_.add(_))
        .map(_.finish().hash)
    )

  override def findUpcomingEffectiveChanges(asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TopologyStore.Change]] =
    FutureUnlessShutdown.wrap {
      blocking {
        synchronized {
          topologyTransactionStore
            .filter(entry => entry.from.value >= asOfInclusive && entry.rejected.isEmpty)
            .map(_.toStoredTransaction)
            .map(TopologyStore.Change.selectChange)
            .toSeq
            .sortBy(_.validFrom)
            .distinct
        }
      }
    }

  override def maxTimestamp(
      sequencedTime: SequencedTime,
      includeRejected: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] = FutureUnlessShutdown.wrap {
    blocking {
      synchronized {
        topologyTransactionStore
          .findLast(entry =>
            entry.sequenced <= sequencedTime && (includeRejected || entry.rejected.isEmpty)
          )
          .map(x => (x.sequenced, x.from))
      }
    }
  }

  override def findTopologyIntervalForTimestamp(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(EffectiveTime, Option[EffectiveTime])]] =
    FutureUnlessShutdown.wrap {
      blocking {
        synchronized {
          val lowerBound = topologyTransactionStore
            .findLast(entry =>
              entry.from.value < timestamp && entry.rejected.isEmpty && !entry.transaction.isProposal
            )
            .map(_.from)
          val upperBound = topologyTransactionStore
            .find(entry =>
              entry.from.value >= timestamp && entry.rejected.isEmpty && !entry.transaction.isProposal
            )
            .map(_.from)
          lowerBound.map(_ -> upperBound)
        }
      }
    }

  override def findDispatchingTransactionsAfter(
      timestampExclusive: CantonTimestamp,
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] =
    FutureUnlessShutdown.pure(blocking(synchronized {
      val selected = topologyTransactionStore
        .filter(x =>
          x.from.value > timestampExclusive && (!x.transaction.isProposal || x.until.isEmpty) && x.rejected.isEmpty
        )
        .map(_.toStoredTransaction)
        .toSeq
      StoredTopologyTransactions(limit.fold(selected)(selected.take))
    }))

  private def allTransactions(
      includeRejected: Boolean = false
  ): FutureUnlessShutdown[GenericStoredTopologyTransactions] =
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      _ => true,
      includeRejected,
    )

  override def findStored(
      asOfExclusive: CantonTimestamp,
      transaction: GenericSignedTopologyTransaction,
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]] =
    allTransactions(includeRejected).map(
      _.result.findLast(tx => tx.hash == transaction.hash && tx.validFrom.value < asOfExclusive)
    )

  override def findStoredForVersion(
      asOfExclusive: CantonTimestamp,
      transaction: GenericTopologyTransaction,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[GenericStoredTopologyTransaction]] = {
    val rpv = TopologyTransaction.protocolVersionRepresentativeFor(protocolVersion)

    allTransactions().map(
      _.result.findLast(tx =>
        tx.transaction.transaction == transaction && tx.transaction.representativeProtocolVersion == rpv && tx.validFrom.value < asOfExclusive
      )
    )
  }

  override def findParticipantOnboardingTransactions(
      participantId: ParticipantId,
      synchronizerId: SynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransaction]] = {
    val res = blocking(synchronized {
      topologyTransactionStore.filter(x =>
        !x.transaction.isProposal && TopologyStore.initialParticipantDispatchingSet.contains(
          x.mapping.code
        )
      )
    })

    FutureUnlessShutdown.pure(
      TopologyStore.filterInitialParticipantDispatchingTransactions(
        participantId,
        synchronizerId,
        res.map(_.transaction).toSeq,
      )
    )
  }

  override def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    FutureUnlessShutdown.pure(watermark.get())

  override def updateDispatchingWatermark(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    watermark.getAndSet(Some(timestamp)) match {
      case Some(old) if old > timestamp =>
        logger.error(
          s"Topology dispatching watermark is running backwards! new=$timestamp, old=$old"
        )
      case _ => ()
    }
    FutureUnlessShutdown.unit
  }

  override def findEffectiveStateChanges(
      fromEffectiveInclusive: CantonTimestamp,
      filterTypes: Option[Seq[TopologyMapping.Code]],
      onlyAtEffective: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[EffectiveStateChange]] = {
    val inRange: EffectiveTime => Boolean =
      if (onlyAtEffective) _.value == fromEffectiveInclusive
      else _.value >= fromEffectiveInclusive

    def isEffective(x: TopologyStoreEntry) = inRange(x.from) || x.until.exists(inRange)
    def hasCorrectType(x: TopologyStoreEntry) = filterTypes.fold(true)(_.contains(x.mapping.code))

    val res = blocking(synchronized {
      topologyTransactionStore.view
        .filter(x =>
          !x.transaction.isProposal &&
            isEffective(x) &&
            !x.until.contains(x.from) &&
            x.rejected.isEmpty && hasCorrectType(x)
        )
        .toSeq
    })
    FutureUnlessShutdown.pure(
      StoredTopologyTransactions(
        res.map(e =>
          StoredTopologyTransaction(e.sequenced, e.from, e.until, e.transaction, e.rejected)
        )
      ).toEffectiveStateChanges(fromEffectiveInclusive, onlyAtEffective)
    )
  }

  override def deleteAllData()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    blocking(synchronized {
      topologyTransactionStore.clear()
      topologyTransactionsStoreUniqueIndex.clear()
      watermark.set(None)
    })
    FutureUnlessShutdown.unit
  }
}
