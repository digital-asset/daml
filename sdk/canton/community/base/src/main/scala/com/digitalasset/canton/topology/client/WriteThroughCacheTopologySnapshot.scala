// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicSynchronizerParametersWithValidity
import com.digitalasset.canton.topology.cache.TopologyStateLookup
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.{
  PackageDependencyResolver,
  StoredTopologyTransactions,
  TimeQuery,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.{
  MediatorSynchronizerState,
  OwnerToKeyMapping,
  SequencerConnectionSuccessor,
  SequencerSynchronizerState,
  SynchronizerParametersState,
  SynchronizerTrustCertificate,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{
  KeyCollection,
  MediatorId,
  Member,
  MemberCode,
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
  SequencerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** A topology snapshot backed by the TopologyStateWriteThroughCache for most methods. Some data
  * cannot be served from the cache and is loaded from the store instead:
  *
  *   - allMembers
  *   - inspectKeys
  *   - inspectKnownParties
  */
class WriteThroughCacheTopologySnapshot(
    psid: PhysicalSynchronizerId,
    stateLookup: TopologyStateLookup,
    store: TopologyStore[TopologyStoreId],
    packageDependencyResolver: PackageDependencyResolver,
    val timestamp: CantonTimestamp,
    loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends BaseTopologySnapshot(psid, packageDependencyResolver, loggerFactory)(executionContext)
    with NamedLogging {

  // ===============================================
  // lookup methods
  // ===============================================

  override protected def findTransactionsByUids(
      types: Seq[TopologyMapping.Code],
      filterUid: NonEmpty[Seq[UniqueIdentifier]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[StoredTopologyTransactions[Replace, TopologyMapping]] =
    lookup(filterUid, types.toSet).map(result =>
      StoredTopologyTransactions(
        result.values.flatten.flatMap(_.selectOp[TopologyChangeOp.Replace]).toSeq
      )
    )

  protected def findTransactionsInStore(
      types: Seq[TopologyMapping.Code],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[StoredTopologyTransactions[Replace, TopologyMapping]] =
    store
      .findPositiveTransactions(
        timestamp,
        asOfInclusive = false,
        isProposal = false,
        types,
        filterUid = filterUid,
        filterNamespace = None,
      )

  private def lookup(uids: NonEmpty[Seq[UniqueIdentifier]], types: Set[TopologyMapping.Code])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Map[UniqueIdentifier, Seq[GenericStoredTopologyTransaction]]] =
    stateLookup.lookupForUids(EffectiveTime(timestamp), asOfInclusive = false, uids, types)

  // ===============================================
  // actual implementations specific to the
  // state write through cache
  // ===============================================

  override def memberFirstKnownAt(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    member match {
      case ParticipantId(uid) =>
        stateLookup
          .lookupHistoryForUid(
            EffectiveTime(timestamp),
            asOfInclusive = false,
            uid,
            TopologyMapping.Code.SynchronizerTrustCertificate,
          )
          .map(_.headOption.map(stored => (stored.sequenced, stored.validFrom)))
      case med @ MediatorId(_) =>
        stateLookup
          .lookupHistoryForUid(
            EffectiveTime(timestamp),
            asOfInclusive = false,
            psid.uid,
            TopologyMapping.Code.MediatorSynchronizerState,
          )
          .map(
            _.sortBy(_.validFrom).view
              .flatMap(_.selectMapping[MediatorSynchronizerState])
              .dropWhile(!_.mapping.allMediatorsInGroup.contains(med))
              .headOption
              .map(stored => (stored.sequenced, stored.validFrom))
          )
      case seq @ SequencerId(_) =>
        stateLookup
          .lookupHistoryForUid(
            EffectiveTime(timestamp),
            asOfInclusive = false,
            psid.uid,
            TopologyMapping.Code.SequencerSynchronizerState,
          )
          .map(
            _.sortBy(_.validFrom).view
              .flatMap(_.selectMapping[SequencerSynchronizerState])
              .dropWhile(!_.mapping.allSequencers.contains(seq))
              .headOption
              .map(stored => (stored.sequenced, stored.validFrom))
          )
    }

  override def listDynamicSynchronizerParametersChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DynamicSynchronizerParametersWithValidity]] =
    stateLookup
      .lookupHistoryForUid(
        EffectiveTime(timestamp),
        asOfInclusive = false,
        psid.uid,
        TopologyMapping.Code.SynchronizerParametersState,
      )
      .map(_.flatMap(_.selectMapping[SynchronizerParametersState]).map { storedTx =>
        val dps = storedTx.mapping
        DynamicSynchronizerParametersWithValidity(
          dps.parameters,
          storedTx.validFrom.value,
          storedTx.validUntil.map(_.value),
        )
      })

  override def allMembers()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] =
    findTransactionsInStore(
      types = Seq(
        SynchronizerTrustCertificate.code,
        MediatorSynchronizerState.code,
        SequencerSynchronizerState.code,
      ),
      filterUid = None,
    ).map { txs =>
      val mappings = txs.result.view.map(_.mapping)
      mappings.flatMap {
        case dtc: SynchronizerTrustCertificate => Seq(dtc.participantId)
        case mds: MediatorSynchronizerState => mds.active ++ mds.observers
        case sds: SequencerSynchronizerState => sds.active ++ sds.observers
        case _ => Seq.empty
      }.toSet
    }

  override def sequencerConnectionSuccessors()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SequencerId, SequencerConnectionSuccessor]] =
    // since the state lookup cannot look at the state just based on the type,
    // we first have to load the current registered sequencers in SequencerSynchronizerState,
    // and then lookup the corresponding SequencerConnectionSuccessor transactions.
    for {
      sequencerStateResult <- findTransactionsByUids(
        types = Seq(TopologyMapping.Code.SequencerSynchronizerState),
        filterUid = NonEmpty(Seq, psid.uid),
      )
      sequencerState = collectLatestMapping(
        TopologyMapping.Code.SequencerSynchronizerState,
        sequencerStateResult.result,
      )
      sequencers = NonEmpty.from(
        sequencerState
          .flatMap(_.select[SequencerSynchronizerState])
          .toList
          .flatMap(_.allSequencers.map(_.uid))
      )
      txs <- sequencers.fold(
        FutureUnlessShutdown.pure(StoredTopologyTransactions.empty[TopologyChangeOp.Replace])
      )(sequencersNE =>
        findTransactionsByUids(
          types = Seq(TopologyMapping.Code.SequencerConnectionSuccessor),
          filterUid = sequencersNE,
        )
      )
    } yield txs
      .collectOfMapping[SequencerConnectionSuccessor]
      .result
      .view
      .map(stored => stored.mapping.sequencerId -> stored.mapping)
      .toMap

  // ===============================================
  // methods implemented directly against the store
  // ===============================================

  /** Returns a list of owner's keys (at most limit) The state cache can only do lookups by uid or
    * namespace. Therefore this implementation goes directly to the store.
    */
  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[MemberCode],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]] = {
    val (idFilter, namespaceFilterO) = UniqueIdentifier.splitFilter(filterOwner)
    store
      .inspect(
        proposals = false,
        timeQuery = TimeQuery.Snapshot(timestamp),
        asOfExclusiveO = None,
        op = Some(TopologyChangeOp.Replace),
        types = Seq(TopologyMapping.Code.OwnerToKeyMapping),
        idFilter = Some(idFilter),
        namespaceFilter = namespaceFilterO,
      )
      .map(
        _.collectOfMapping[OwnerToKeyMapping]
          .collectOfType[TopologyChangeOp.Replace]
          .result
          .groupBy(_.mapping.member)
          .collect {
            case (owner, seq)
                if owner.filterString.startsWith(filterOwner) &&
                  filterOwnerType.forall(_ == owner.code) =>
              val keys = KeyCollection(Seq(), Seq())
              val okm =
                collectLatestMapping(
                  TopologyMapping.Code.OwnerToKeyMapping,
                  seq.sortBy(_.validFrom),
                )
              owner -> okm
                .fold(keys)(_.keys.take(limit).foldLeft(keys) { case (keys, key) =>
                  keys.add(key)
                })
          }
      )
  }

  /** The state cache can only do lookups by uid or namespace. Therefore this implementation goes
    * directly to the store.
    */
  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] =
    store.inspectKnownParties(timestamp, filterParty, filterParticipant, limit)

}
