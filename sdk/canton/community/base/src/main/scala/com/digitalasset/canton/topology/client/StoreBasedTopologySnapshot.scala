// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicSynchronizerParametersWithValidity
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Topology snapshot loader
  *
  * @param timestamp
  *   the asOf timestamp to use
  * @param store
  *   the db store to use
  * @param packageDependencyResolver
  *   provides a way determine the direct and indirect package dependencies.
  */
class StoreBasedTopologySnapshot(
    psid: PhysicalSynchronizerId,
    val timestamp: CantonTimestamp,
    store: TopologyStore[TopologyStoreId],
    packageDependencyResolver: PackageDependencyResolver,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends BaseTopologySnapshot(psid, packageDependencyResolver, loggerFactory)(executionContext)
    with NamedLogging {

  override protected def findTransactionsByUids(
      types: Seq[TopologyMapping.Code],
      filterUid: NonEmpty[Seq[UniqueIdentifier]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping]] =
    store
      .findPositiveTransactions(
        timestamp,
        asOfInclusive = false,
        isProposal = false,
        types,
        Some(filterUid),
        None,
      )

  private def findTransactionsByType(
      types: Seq[TopologyMapping.Code]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping]] =
    store
      .findPositiveTransactions(
        timestamp,
        asOfInclusive = false,
        isProposal = false,
        types,
        None,
        None,
      )

  /** List all the dynamic synchronizer parameters (past and current) */
  override def listDynamicSynchronizerParametersChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DynamicSynchronizerParametersWithValidity]] = store
    .inspect(
      proposals = false,
      timeQuery = TimeQuery.Range(None, Some(timestamp)),
      asOfExclusiveO = None,
      op = Some(TopologyChangeOp.Replace),
      types = Seq(TopologyMapping.Code.SynchronizerParametersState),
      idFilter = None,
      namespaceFilter = None,
    )
    .map {
      _.collectOfMapping[SynchronizerParametersState].result
        .map { storedTx =>
          val dps = storedTx.mapping
          DynamicSynchronizerParametersWithValidity(
            dps.parameters,
            storedTx.validFrom.value,
            storedTx.validUntil.map(_.value),
          )
        }
    }

  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] =
    store.inspectKnownParties(timestamp, filterParty, filterParticipant, limit)

  /** Returns a list of owner's keys (at most limit) */
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

  override def allMembers()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] =
    findTransactionsByType(
      types = Seq(
        SynchronizerTrustCertificate.code,
        MediatorSynchronizerState.code,
        SequencerSynchronizerState.code,
      )
    ).map { txs =>
      val mappings = txs.result.view.map(_.mapping)
      mappings.flatMap {
        case dtc: SynchronizerTrustCertificate => Seq(dtc.participantId)
        case mds: MediatorSynchronizerState => mds.active ++ mds.observers
        case sds: SequencerSynchronizerState => sds.active ++ sds.observers
        case _ => Seq.empty
      }.toSet
    }

  override def memberFirstKnownAt(
      member: Member
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    member match {
      case participantId: ParticipantId =>
        store
          .findFirstTrustCertificateForParticipant(participantId)
          .map(_.map(tx => (tx.sequenced, tx.validFrom)))
      case mediatorId: MediatorId =>
        store
          .findFirstMediatorStateForMediator(mediatorId)
          .map(_.map(tx => (tx.sequenced, tx.validFrom)))
      case sequencerId: SequencerId =>
        store
          .findFirstSequencerStateForSequencer(sequencerId)
          .map(_.map(tx => (tx.sequenced, tx.validFrom)))
      case _ =>
        FutureUnlessShutdown.failed(
          new IllegalArgumentException(
            s"Checking whether member is known for an unexpected member type: $member"
          )
        )
    }

  override def sequencerConnectionSuccessors()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SequencerId, SequencerConnectionSuccessor]] =
    findTransactionsByType(
      types = Seq(TopologyMapping.Code.SequencerConnectionSuccessor)
    ).map(txs =>
      txs
        .collectOfMapping[SequencerConnectionSuccessor]
        .toTopologyState
        .map(m => m.sequencerId -> m)
        .toMap
    )
}
