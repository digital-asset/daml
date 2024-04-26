// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.{
  GenericStoredTopologyTransactions,
  PositiveStoredTopologyTransactions,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransaction.{
  GenericTopologyTransaction,
  TxHash,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{ProtocolVersion, RepresentativeProtocolVersion}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.math.Ordering.Implicits.*

class InMemoryTopologyStore[+StoreId <: TopologyStoreId](
    val storeId: StoreId,
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
      rejected: Option[String],
      until: Option[EffectiveTime],
  ) extends DelegatedTopologyTransactionLike[TopologyChangeOp, TopologyMapping] {

    override protected def transactionLikeDelegate
        : TopologyTransactionLike[TopologyChangeOp, TopologyMapping] = transaction

    def toStoredTransaction: StoredTopologyTransaction[TopologyChangeOp, TopologyMapping] =
      StoredTopologyTransaction(sequenced, from, until, transaction)
  }

  private val topologyTransactionStore = ArrayBuffer[TopologyStoreEntry]()
  // the unique key is defined in the database migration file for the common_topology_transactions table
  private val topologyTransactionsStoreUniqueIndex = mutable.Set.empty[
    (
        MappingHash,
        PositiveInt,
        EffectiveTime,
        TopologyChangeOp,
        RepresentativeProtocolVersion[SignedTopologyTransaction.type],
        Hash,
    )
  ]
  private val watermark = new AtomicReference[Option[CantonTimestamp]](None)

  def findTransactionsByTxHash(asOfExclusive: EffectiveTime, hashes: Set[TxHash])(implicit
      traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransaction]] =
    if (hashes.isEmpty) Future.successful(Seq.empty)
    else
      findFilter(
        asOfExclusive,
        entry => hashes.contains(entry.hash),
      )

  override def findProposalsByTxHash(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[TxHash]],
  )(implicit traceContext: TraceContext): Future[Seq[GenericSignedTopologyTransaction]] = {
    findFilter(
      asOfExclusive,
      entry => hashes.contains(entry.hash) && entry.transaction.isProposal,
    )
  }

  private def findFilter(
      asOfExclusive: EffectiveTime,
      filter: TopologyStoreEntry => Boolean,
  ): Future[Seq[GenericSignedTopologyTransaction]] = Future.successful {
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
  ): Future[Seq[GenericSignedTopologyTransaction]] = {
    findFilter(
      asOfExclusive,
      entry =>
        !entry.transaction.isProposal && hashes.contains(
          entry.mapping.uniqueKey
        ),
    )
  }

  override def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removeMapping: Map[TopologyMapping.MappingHash, PositiveInt],
      removeTxs: Set[TopologyTransaction.TxHash],
      additions: Seq[GenericValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    blocking {
      synchronized {
        // transactionally
        // UPDATE txs SET valid_until = effective WHERE effective < $effective AND valid_from is NULL
        //    AND ((mapping_key_hash IN $removeMapping AND serial_counter <= $serial) OR (tx_hash IN $removeTxs))
        // INSERT IGNORE DUPLICATES (...)
        topologyTransactionStore.zipWithIndex.foreach { case (tx, idx) =>
          if (
            tx.from.value < effective.value && tx.until.isEmpty &&
            (removeMapping
              .get(tx.mapping.uniqueKey)
              .exists(_ >= tx.serial)
              ||
                removeTxs.contains(tx.hash))
          ) {
            topologyTransactionStore.update(idx, tx.copy(until = Some(effective)))
          }
        }
        additions.foreach { tx =>
          val uniqueKey = (
            tx.mapping.uniqueKey,
            tx.serial,
            effective,
            tx.operation,
            tx.transaction.representativeProtocolVersion,
            tx.transaction.hashOfSignatures,
          )
          if (topologyTransactionsStoreUniqueIndex.add(uniqueKey)) {
            topologyTransactionStore.append(
              TopologyStoreEntry(
                tx.transaction,
                sequenced,
                from = effective,
                rejected = tx.rejectionReason.map(_.toString),
                until = Option.when(
                  tx.rejectionReason.nonEmpty || tx.expireImmediately
                )(effective),
              )
            )
          }
        }
      }
    }
    Future.unit
  }

  @VisibleForTesting
  override protected[topology] def dumpStoreContent()(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactions] = {
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
    Future.successful(
      StoredTopologyTransactions(
        entries.map(e => StoredTopologyTransaction(e.sequenced, e.from, e.until, e.transaction))
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
  ): Future[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]] =
    Future.successful(
      StoredTopologyTransactions(
        table.collect {
          case entry if filter(entry) && (entry.rejected.isEmpty || includeRejected) =>
            entry.toStoredTransaction
        }
      )
    )

  override def inspectKnownParties(
      timestamp: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]] = {
    val (prefixPartyIdentifier, prefixPartyNS) = UniqueIdentifier.splitFilter(filterParty)
    val (prefixParticipantIdentifier, prefixParticipantNS) =
      UniqueIdentifier.splitFilter(filterParticipant)

    def filter(entry: TopologyStoreEntry): Boolean = {
      // active
      entry.from.value < timestamp && entry.until.forall(until => timestamp <= until.value) &&
      // not rejected
      entry.rejected.isEmpty &&
      // is not a proposal
      !entry.transaction.isProposal &&
      // is of type Replace
      entry.operation == TopologyChangeOp.Replace &&
      // matches a party to participant mapping (with appropriate filters)
      (entry.mapping match {
        case ptp: PartyToParticipant =>
          ptp.partyId.uid.matchesPrefixes(prefixPartyIdentifier, prefixPartyNS) &&
          (filterParticipant.isEmpty ||
            ptp.participants.exists(
              _.participantId.uid
                .matchesPrefixes(prefixParticipantIdentifier, prefixParticipantNS)
            ))
        case cert: DomainTrustCertificate =>
          cert.participantId.adminParty.uid
            .matchesPrefixes(prefixPartyIdentifier, prefixPartyNS) &&
          cert.participantId.uid
            .matchesPrefixes(prefixParticipantIdentifier, prefixParticipantNS)
        case _ => false
      })
    }

    val topologyStateStoreSeq = blocking(synchronized(topologyTransactionStore.toSeq))
    Future.successful(
      topologyStateStoreSeq
        .foldLeft(Set.empty[PartyId]) {
          case (acc, elem) if acc.size >= limit || !filter(elem) => acc
          case (acc, elem) =>
            elem.mapping.maybeUid.fold(acc)(x => acc + PartyId(x))
        }
    )
  }

  override def inspect(
      proposals: Boolean,
      timeQuery: TimeQuery,
      recentTimestampO: Option[CantonTimestamp],
      op: Option[TopologyChangeOp],
      types: Seq[TopologyMapping.Code],
      idFilter: Option[String],
      namespaceFilter: Option[String],
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]] = {
    def mkAsOfFilter(asOf: CantonTimestamp): TopologyStoreEntry => Boolean = entry =>
      asOfFilter(asOf, asOfInclusive = false)(entry.from.value, entry.until.map(_.value))

    val filter1: TopologyStoreEntry => Boolean = timeQuery match {
      case TimeQuery.HeadState =>
        // use recent timestamp to avoid race conditions (as we are looking
        // directly into the store, while the recent time still needs to propagate)
        recentTimestampO.map(mkAsOfFilter).getOrElse(entry => entry.until.isEmpty)
      case TimeQuery.Snapshot(asOf) => mkAsOfFilter(asOf)
      case TimeQuery.Range(from, until) =>
        entry =>
          from.forall(ts => entry.from.value >= ts) && until.forall(ts => entry.from.value <= ts)
    }

    val filter2: TopologyStoreEntry => Boolean = entry => op.forall(_ == entry.operation)

    val filter3: TopologyStoreEntry => Boolean = {
      idFilter match {
        case Some(value) if value.nonEmpty =>
          (entry: TopologyStoreEntry) =>
            entry.mapping.maybeUid.exists(_.id.unwrap.startsWith(value))
        case _ => _ => true
      }
    }

    val filter4: TopologyStoreEntry => Boolean = {
      namespaceFilter match {
        case Some(value) if value.nonEmpty =>
          (entry: TopologyStoreEntry) =>
            entry.mapping.namespace.fingerprint.unwrap.startsWith(value)
        case _ => _ => true
      }
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
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit traceContext: TraceContext): Future[PositiveStoredTopologyTransactions] =
    findTransactionsInStore(asOf, asOfInclusive, isProposal, types, filterUid, filterNamespace).map(
      _.collectOfType[TopologyChangeOp.Replace]
    )

  private def findTransactionsInStore(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMapping.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  ): Future[GenericStoredTopologyTransactions] = {
    val timeFilter = asOfFilter(asOf, asOfInclusive)
    def pathFilter(mapping: TopologyMapping): Boolean = {
      if (filterUid.isEmpty && filterNamespace.isEmpty)
        true
      else {
        mapping.maybeUid.exists(uid => filterUid.exists(_.contains(uid))) ||
        filterNamespace.exists(_.contains(mapping.namespace))
      }
    }
    filteredState(
      blocking(synchronized { topologyTransactionStore.toSeq }),
      entry => {
        timeFilter(entry.from.value, entry.until.map(_.value)) &&
        types.contains(entry.mapping.code) &&
        (pathFilter(entry.mapping)) &&
        entry.transaction.isProposal == isProposal
      },
    )
  }

  override def findFirstSequencerStateForSequencer(
      sequencerId: SequencerId
  )(implicit
      traceContext: TraceContext
  ): Future[
    Option[StoredTopologyTransaction[TopologyChangeOp.Replace, SequencerDomainState]]
  ] = {
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      entry =>
        !entry.transaction.isProposal &&
          entry.operation == TopologyChangeOp.Replace &&
          entry.mapping
            .select[SequencerDomainState]
            .exists(m => m.allSequencers.contains(sequencerId)),
    ).map(
      _.collectOfType[TopologyChangeOp.Replace]
        .collectOfMapping[SequencerDomainState]
        .result
        .sortBy(_.serial)
        .headOption
    )
  }

  override def findFirstMediatorStateForMediator(
      mediatorId: MediatorId
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransaction[TopologyChangeOp.Replace, MediatorDomainState]]] = {
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      entry =>
        !entry.transaction.isProposal &&
          entry.operation == TopologyChangeOp.Replace &&
          entry.mapping
            .select[MediatorDomainState]
            .exists(m => m.observers.contains(mediatorId) || m.active.contains(mediatorId)),
    ).map(
      _.collectOfType[TopologyChangeOp.Replace]
        .collectOfMapping[MediatorDomainState]
        .result
        .sortBy(_.serial)
        .headOption
    )
  }

  def findFirstTrustCertificateForParticipant(
      participant: ParticipantId
  )(implicit
      traceContext: TraceContext
  ): Future[
    Option[StoredTopologyTransaction[TopologyChangeOp.Replace, DomainTrustCertificate]]
  ] = {
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      entry =>
        !entry.transaction.isProposal &&
          entry.operation == TopologyChangeOp.Replace &&
          entry.mapping
            .select[DomainTrustCertificate]
            .exists(_.participantId == participant),
    ).map(
      _.collectOfType[TopologyChangeOp.Replace]
        .collectOfMapping[DomainTrustCertificate]
        .result
        .sortBy(_.serial)
        .headOption
    )

  }

  override def findEssentialStateAtSequencedTime(
      asOfInclusive: SequencedTime
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactions] = {
    // asOfInclusive is the effective time of the transaction that onboarded the member.
    // 1. load all transactions with a sequenced time <= asOfInclusive, including proposals
    filteredState(
      blocking(synchronized {
        topologyTransactionStore.toSeq
      }),
      entry => entry.sequenced <= asOfInclusive,
    ).map(
      // 2. transform the result such that the validUntil fields are set as they were at maxEffective time of the snapshot
      _.asSnapshotAtMaxEffectiveTime
        // and remove proposals that have been superseded by full authorized transactions
        .retainAuthorizedHistoryAndEffectiveProposals
    )
  }

  /** store an initial set of topology transactions as given into the store */
  override def bootstrap(
      snapshot: GenericStoredTopologyTransactions
  )(implicit traceContext: TraceContext): Future[Unit] = Future {
    blocking {
      synchronized {
        topologyTransactionStore
          .appendAll(
            snapshot.result.map { tx =>
              TopologyStoreEntry(
                tx.transaction,
                tx.sequenced,
                tx.validFrom,
                rejected = None,
                until = tx.validUntil,
              )
            }
          )
          .discard
      }
    }
  }

  override def findUpcomingEffectiveChanges(asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TopologyStore.Change]] =
    Future {
      blocking {
        synchronized {
          TopologyStore.accumulateUpcomingEffectiveChanges(
            topologyTransactionStore
              .filter(_.from.value >= asOfInclusive)
              .map(_.toStoredTransaction)
              .toSeq
          )
        }
      }
    }

  override def maxTimestamp()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] = Future {
    blocking {
      synchronized {
        topologyTransactionStore.lastOption.map(x => (x.sequenced, x.from))
      }
    }
  }

  override def findDispatchingTransactionsAfter(
      timestampExclusive: CantonTimestamp,
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactions] =
    Future.successful(blocking(synchronized {
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
  ): Future[GenericStoredTopologyTransactions] =
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
  ): Future[Option[GenericStoredTopologyTransaction]] =
    allTransactions(includeRejected).map(
      _.result.findLast(tx => tx.hash == transaction.hash && tx.validFrom.value < asOfExclusive)
    )

  override def findStoredForVersion(
      asOfExclusive: CantonTimestamp,
      transaction: GenericTopologyTransaction,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransaction]] = {
    val rpv = TopologyTransaction.protocolVersionRepresentativeFor(protocolVersion)

    allTransactions().map(
      _.result.findLast(tx =>
        tx.transaction.transaction == transaction && tx.transaction.representativeProtocolVersion == rpv && tx.validFrom.value < asOfExclusive
      )
    )
  }

  override def findParticipantOnboardingTransactions(
      participantId: ParticipantId,
      domainId: DomainId,
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
        domainId,
        res.map(_.toStoredTransaction).toSeq,
      )
    )
  }

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
