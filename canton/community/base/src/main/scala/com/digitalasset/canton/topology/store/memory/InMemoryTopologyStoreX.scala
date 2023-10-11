// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionX.GenericStoredTopologyTransactionX
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.{
  GenericStoredTopologyTransactionsX,
  PositiveStoredTopologyTransactionsX,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.{
  GenericTopologyTransactionX,
  TxHash,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, blocking}

class InMemoryTopologyStoreX[+StoreId <: TopologyStoreId](
    val storeId: StoreId,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyStoreX[StoreId]
    with InMemoryTopologyStoreCommon[StoreId]
    with NamedLogging {

  override def close(): Unit = {}

  private case class TopologyStoreEntry(
      transaction: GenericSignedTopologyTransactionX,
      sequenced: SequencedTime,
      from: EffectiveTime,
      rejected: Option[String],
      until: Option[EffectiveTime],
  ) {
    def toStoredTransaction: StoredTopologyTransactionX[TopologyChangeOpX, TopologyMappingX] =
      StoredTopologyTransactionX(sequenced, from, until, transaction)
  }

  private val topologyTransactionStore = ArrayBuffer[TopologyStoreEntry]()

  def findTransactionsByTxHash(asOfExclusive: EffectiveTime, hashes: NonEmpty[Set[TxHash]])(implicit
      traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransactionX]] = findFilter(
    asOfExclusive,
    entry => hashes.contains(entry.transaction.transaction.hash),
  )

  override def findProposalsByTxHash(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[TxHash]],
  )(implicit traceContext: TraceContext): Future[Seq[GenericSignedTopologyTransactionX]] = {
    findFilter(
      asOfExclusive,
      entry => hashes.contains(entry.transaction.transaction.hash) && entry.transaction.isProposal,
    )
  }

  private def findFilter(
      asOfExclusive: EffectiveTime,
      filter: TopologyStoreEntry => Boolean,
  ): Future[Seq[GenericSignedTopologyTransactionX]] = {
    blocking {
      synchronized {
        val res = topologyTransactionStore
          .filter(x =>
            x.from.value < asOfExclusive.value
              && x.rejected.isEmpty
              && (x.until.forall(_.value >= asOfExclusive.value))
              && filter(x)
          )
          .map(_.transaction)
          .toSeq
        Future.successful(res)
      }
    }
  }

  override def findTransactionsForMapping(
      asOfExclusive: EffectiveTime,
      hashes: NonEmpty[Set[MappingHash]],
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransactionX]] = {
    findFilter(
      asOfExclusive,
      entry =>
        !entry.transaction.isProposal && hashes.contains(
          entry.transaction.transaction.mapping.uniqueKey
        ),
    )
  }

  override def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removeMapping: Set[TopologyMappingX.MappingHash],
      removeTxs: Set[TopologyTransactionX.TxHash],
      additions: Seq[GenericValidatedTopologyTransactionX],
  )(implicit traceContext: TraceContext): Future[Unit] =
    blocking {
      synchronized {
        // transactionally
        // UPDATE txs SET valid_until = effective WHERE effective < $effective AND valid_from is NULL
        //    AND ((mapping_key_hash IN $removeMapping) OR (tx_hash IN $removeTxs))
        // INSERT IGNORE DUPLICATES (...)
        topologyTransactionStore.zipWithIndex.foreach { case (tx, idx) =>
          if (
            tx.from.value < effective.value && tx.until.isEmpty && (removeMapping.contains(
              tx.transaction.transaction.mapping.uniqueKey
            ) || removeTxs.contains(tx.transaction.transaction.hash))
          ) {
            topologyTransactionStore.update(idx, tx.copy(until = Some(effective)))
          }
        }
        topologyTransactionStore.appendAll(
          additions.map(tx =>
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
        )
        Future.unit
      }
    }

  // TODO(#14048) only a temporary crutch to inspect the topology state
  override def dumpStoreContent()(implicit traceContext: TraceContext): Unit = {
    blocking {
      synchronized {
        logger.debug(
          topologyTransactionStore
            .map(_.toString)
            .mkString("Topology Store Content[", ", ", "]")
        )

      }
    }
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
  ): Future[StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]] =
    Future.successful(
      StoredTopologyTransactionsX(
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
      // matches a party to participant mapping (with appropriate filters)
      (entry.transaction.transaction.mapping match {
        case ptp: PartyToParticipantX =>
          ptp.partyId.uid.matchesPrefixes(prefixPartyIdentifier, prefixPartyNS) &&
          (filterParticipant.isEmpty ||
            ptp.participants.exists(
              _.participantId.uid
                .matchesPrefixes(prefixParticipantIdentifier, prefixParticipantNS)
            ))
        case cert: DomainTrustCertificateX =>
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
            elem.transaction.transaction.mapping.maybeUid.fold(acc)(x => acc + PartyId(x))
        }
    )
  }

  override def inspect(
      proposals: Boolean,
      timeQuery: TimeQueryX,
      recentTimestampO: Option[CantonTimestamp],
      op: Option[TopologyChangeOpX],
      typ: Option[TopologyMappingX.Code],
      idFilter: String,
      namespaceOnly: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]] = {
    def mkAsOfFilter(asOf: CantonTimestamp): TopologyStoreEntry => Boolean = entry =>
      asOfFilter(asOf, asOfInclusive = false)(entry.from.value, entry.until.map(_.value))

    val filter1: TopologyStoreEntry => Boolean = timeQuery match {
      case TimeQueryX.HeadState =>
        // use recent timestamp to avoid race conditions (as we are looking
        // directly into the store, while the recent time still needs to propagate)
        recentTimestampO.map(mkAsOfFilter).getOrElse(entry => entry.until.isEmpty)
      case TimeQueryX.Snapshot(asOf) => mkAsOfFilter(asOf)
      case TimeQueryX.Range(from, until) =>
        entry =>
          from.forall(ts => entry.from.value >= ts) && until.forall(ts => entry.from.value <= ts)
    }

    val filter2: TopologyStoreEntry => Boolean = entry =>
      op.forall(_ == entry.transaction.operation)

    val filter3: TopologyStoreEntry => Boolean = {
      if (idFilter.isEmpty) _ => true
      else if (namespaceOnly) { entry =>
        entry.transaction.transaction.mapping.namespace.fingerprint.unwrap.startsWith(idFilter)
      } else {
        val split = idFilter.split(SafeSimpleString.delimiter)
        val prefix = split(0)
        if (split.lengthCompare(1) > 0) {
          val suffix = split(1)
          entry: TopologyStoreEntry =>
            entry.transaction.transaction.mapping.maybeUid.exists(_.id.unwrap.startsWith(prefix)) &&
              entry.transaction.transaction.mapping.namespace.fingerprint.unwrap.startsWith(suffix)
        } else { entry =>
          entry.transaction.transaction.mapping.maybeUid.exists(_.id.unwrap.startsWith(prefix))
        }
      }
    }
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      entry =>
        typ.forall(
          _ == entry.transaction.transaction.mapping.code
        ) && (entry.transaction.isProposal == proposals) && filter1(entry) && filter2(
          entry
        ) && filter3(entry),
    )
  }

  override def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMappingX.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit traceContext: TraceContext): Future[PositiveStoredTopologyTransactionsX] =
    findTransactionsInStore(asOf, asOfInclusive, isProposal, types, filterUid, filterNamespace).map(
      _.collectOfType[TopologyChangeOpX.Replace]
    )

  private def findTransactionsInStore(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMappingX.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  ): Future[GenericStoredTopologyTransactionsX] = {
    val timeFilter = asOfFilter(asOf, asOfInclusive)
    def pathFilter(mapping: TopologyMappingX): Boolean = {
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
        types.contains(entry.transaction.transaction.mapping.code) &&
        (pathFilter(entry.transaction.transaction.mapping)) &&
        entry.transaction.isProposal == isProposal
      },
    )
  }

  override def findFirstMediatorStateForMediator(
      mediatorId: MediatorId
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransactionX[TopologyChangeOpX.Replace, MediatorDomainStateX]]] = {
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      entry =>
        !entry.transaction.isProposal &&
          entry.transaction.transaction.op == TopologyChangeOpX.Replace &&
          entry.transaction.transaction.mapping
            .select[MediatorDomainStateX]
            .exists(m => m.observers.contains(mediatorId) || m.active.contains(mediatorId)),
    ).map(
      _.collectOfType[TopologyChangeOpX.Replace]
        .collectOfMapping[MediatorDomainStateX]
        .result
        .sortBy(_.transaction.transaction.serial)
        .headOption
    )
  }

  def findFirstTrustCertificateForParticipant(
      participant: ParticipantId
  )(implicit
      traceContext: TraceContext
  ): Future[
    Option[StoredTopologyTransactionX[TopologyChangeOpX.Replace, DomainTrustCertificateX]]
  ] = {
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      entry =>
        !entry.transaction.isProposal &&
          entry.transaction.transaction.op == TopologyChangeOpX.Replace &&
          entry.transaction.transaction.mapping
            .select[DomainTrustCertificateX]
            .exists(_.participantId == participant),
    ).map(
      _.collectOfType[TopologyChangeOpX.Replace]
        .collectOfMapping[DomainTrustCertificateX]
        .result
        .sortBy(_.transaction.transaction.serial)
        .headOption
    )

  }

  override def findEssentialStateForMember(member: Member, asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[GenericStoredTopologyTransactionsX] = {
    // asOfInclusive is the effective time of the transaction that onboarded the member.
    // 1. load all transactions with a sequenced time <= asOfInclusive, including proposals
    filteredState(
      blocking(synchronized {
        topologyTransactionStore.toSeq
      }),
      entry => entry.sequenced.value <= asOfInclusive,
    ).map(
      // 2. transform the result such that the validUntil fields are set as they were at maxEffective time of the snapshot
      _.asSnapshotAtMaxEffectiveTime
    )
  }

  /** store an initial set of topology transactions as given into the store */
  override def bootstrap(
      snapshot: GenericStoredTopologyTransactionsX
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
          TopologyStoreX.accumulateUpcomingEffectiveChanges(
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
  ): Future[GenericStoredTopologyTransactionsX] =
    blocking(synchronized {
      val selected = topologyTransactionStore
        .filter(x =>
          x.from.value > timestampExclusive && (!x.transaction.isProposal || x.until.isEmpty) && x.rejected.isEmpty
        )
        .map(_.toStoredTransaction)
        .toSeq
      Future.successful(StoredTopologyTransactionsX(limit.fold(selected)(selected.take)))
    })

  private def allTransactions(
      includeRejected: Boolean = false
  ): Future[GenericStoredTopologyTransactionsX] =
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      _ => true,
      includeRejected,
    )

  override def findStored(
      transaction: GenericSignedTopologyTransactionX,
      includeRejected: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransactionX]] =
    allTransactions(includeRejected).map(
      _.result.findLast(_.transaction.transaction.hash == transaction.transaction.hash)
    )

  override def findStoredForVersion(
      transaction: GenericTopologyTransactionX,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[GenericStoredTopologyTransactionX]] =
    allTransactions().map(
      _.result.findLast(tx =>
        tx.transaction.transaction == transaction && tx.transaction.representativeProtocolVersion == TopologyTransactionX
          .protocolVersionRepresentativeFor(protocolVersion)
      )
    )

  override def findParticipantOnboardingTransactions(
      participantId: ParticipantId,
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[GenericSignedTopologyTransactionX]] = {
    val res = blocking(synchronized {
      topologyTransactionStore.filter(x =>
        !x.transaction.isProposal && TopologyStoreX.initialParticipantDispatchingSet.contains(
          x.transaction.transaction.mapping.code
        )
      )
    })

    FutureUnlessShutdown.pure(
      TopologyStoreX.filterInitialParticipantDispatchingTransactions(
        participantId,
        domainId,
        res.map(_.toStoredTransaction).toSeq,
      )
    )
  }

}
