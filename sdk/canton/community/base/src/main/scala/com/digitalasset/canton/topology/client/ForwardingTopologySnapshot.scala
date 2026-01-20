// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.caching.ScaffeineCache.TracedAsyncLoadingCache
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs}
import com.digitalasset.canton.crypto.SigningKeysWithThreshold
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, PromiseUnlessShutdown}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DynamicSequencingParametersWithValidity,
  DynamicSynchronizerParametersWithValidity,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.processing.*
import com.digitalasset.canton.topology.store.UnknownOrUnvettedPackages
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.daml.lf.data.Ref.PackageId

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

/** A simple wrapper class in order to "override" the timestamp we are returning here when caching.
  */
class ForwardingTopologySnapshot(
    override val timestamp: CantonTimestamp,
    parent: TopologySnapshotLoader,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TopologySnapshotLoader {

  override def referenceTime: CantonTimestamp = parent.timestamp
  override def allKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[KeyCollection] =
    parent.allKeys(owner)
  override def allKeys(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, KeyCollection]] = parent.allKeys(members)
  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]] =
    parent.loadParticipantStates(participants)

  override private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PartyInfo] =
    parent.loadActiveParticipantsOf(party, participantStates)

  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[MemberCode],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]] =
    parent.inspectKeys(filterOwner, filterOwnerType, limit)
  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] =
    parent.inspectKnownParties(filterParty, filterParticipant, limit)

  override def loadVettedPackages(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PackageId, VettedPackage]] = parent.loadVettedPackages(participant)

  override def loadVettedPackages(participants: Set[ParticipantId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, Map[PackageId, VettedPackage]]] =
    parent.loadVettedPackages(participants)

  override private[client] def findUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packages: Set[PackageId],
      ledgerTime: CantonTimestamp,
      vettedPackages: Map[PackageId, VettedPackage],
  )(implicit traceContext: TraceContext): UnknownOrUnvettedPackages =
    parent.findUnvettedPackagesOrDependencies(participant, packages, ledgerTime, vettedPackages)

  /** returns the list of currently known mediators */
  override def mediatorGroups()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[MediatorGroup]] =
    parent.mediatorGroups()

  /** returns the sequencer group if known */
  override def sequencerGroup()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SequencerGroup]] = parent.sequencerGroup()

  override def allMembers()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] =
    parent.allMembers()

  override def isMemberKnown(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    parent.isMemberKnown(member)

  override def areMembersKnown(members: Set[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] = parent.areMembersKnown(members)

  override def memberFirstKnownAt(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    parent.memberFirstKnownAt(member)

  override def findDynamicSynchronizerParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSynchronizerParametersWithValidity]] =
    parent.findDynamicSynchronizerParameters()

  override def findDynamicSequencingParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSequencingParametersWithValidity]] =
    parent.findDynamicSequencingParameters()

  /** List all the dynamic synchronizer parameters (past and current) */
  override def listDynamicSynchronizerParametersChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DynamicSynchronizerParametersWithValidity]] =
    parent.listDynamicSynchronizerParametersChanges()

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext) =
    parent.loadBatchActiveParticipantsOf(parties, loadParticipantStates)

  override def signingKeysWithThreshold(party: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SigningKeysWithThreshold]] =
    parent.signingKeysWithThreshold(party)

  override def synchronizerUpgradeOngoing()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SynchronizerSuccessor, EffectiveTime)]] =
    parent.synchronizerUpgradeOngoing()

  override def sequencerConnectionSuccessors()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SequencerId, SequencerConnectionSuccessor]] =
    parent.sequencerConnectionSuccessors()
}

class CachingTopologySnapshot(
    parent: TopologySnapshotLoader,
    cachingConfigs: CachingConfigs,
    batchingConfig: BatchingConfig,
    val loggerFactory: NamedLoggerFactory,
    val futureSupervisor: FutureSupervisor,
)(implicit
    val executionContext: ExecutionContext
) extends TopologySnapshotLoader
    with NamedLogging {

  override def timestamp: CantonTimestamp = parent.timestamp

  private val partyCache: TracedAsyncLoadingCache[FutureUnlessShutdown, PartyId, PartyInfo] =
    ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, PartyId, PartyInfo](
      cache = cachingConfigs.partyCache.buildScaffeine(loggerFactory),
      loader = implicit traceContext =>
        party => parent.loadActiveParticipantsOf(party, loadParticipantStates(_)),
      allLoader = Some(implicit traceContext =>
        parties => parent.loadBatchActiveParticipantsOf(parties.toSeq, loadParticipantStates(_))
      ),
    )(logger, "partyCache")

  private val participantCache: TracedAsyncLoadingCache[FutureUnlessShutdown, ParticipantId, Option[
    ParticipantAttributes
  ]] =
    ScaffeineCache
      .buildTracedAsync[FutureUnlessShutdown, ParticipantId, Option[ParticipantAttributes]](
        cache = cachingConfigs.participantCache.buildScaffeine(loggerFactory),
        loader = implicit traceContext => pid => parent.findParticipantState(pid),
        allLoader = Some { implicit traceContext => pids =>
          parent
            .loadParticipantStates(pids.toSeq)
            .map(attributes =>
              // make sure that the returned map contains an entry for each input element
              pids.map(pid => pid -> attributes.get(pid)).toMap
            )
        },
      )(logger, "participantCache")

  private val keyCache: TracedAsyncLoadingCache[FutureUnlessShutdown, Member, KeyCollection] =
    ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, Member, KeyCollection](
      cache = cachingConfigs.keyCache.buildScaffeine(loggerFactory),
      loader = implicit traceContext => member => parent.allKeys(member),
      allLoader = Some(implicit traceContext =>
        members =>
          parent
            .allKeys(members.toSeq)
            .map(foundKeys =>
              // make sure that the returned map contains an entry for each input element
              members
                .map(member => member -> foundKeys.getOrElse(member, KeyCollection.empty))
                .toMap
            )
      ),
    )(logger, "keyCache")

  private val packageVettingCache: TracedAsyncLoadingCache[
    FutureUnlessShutdown,
    ParticipantId,
    Map[PackageId, VettedPackage],
  ] = ScaffeineCache
    .buildTracedAsync[FutureUnlessShutdown, ParticipantId, Map[PackageId, VettedPackage]](
      cache = cachingConfigs.packageVettingCache.buildScaffeine(loggerFactory),
      loader = implicit traceContext => x => parent.loadVettedPackages(x),
      allLoader =
        Some(implicit traceContext => participants => parent.loadVettedPackages(participants.toSet)),
    )(logger, "packageVettingCache")

  private val mediatorsCache =
    new AtomicReference[Option[FutureUnlessShutdown[Seq[MediatorGroup]]]](None)

  private val sequencerGroupCache =
    new AtomicReference[Option[FutureUnlessShutdown[Option[SequencerGroup]]]](None)

  private val allMembersCache = new AtomicReference[Option[FutureUnlessShutdown[Set[Member]]]](None)
  private val memberCache: TracedAsyncLoadingCache[FutureUnlessShutdown, Member, Boolean] =
    ScaffeineCache.buildTracedAsync[FutureUnlessShutdown, Member, Boolean](
      cache = cachingConfigs.memberCache.buildScaffeine(loggerFactory),
      loader = implicit traceContext => member => parent.isMemberKnown(member),
      allLoader = Some(implicit traceContext =>
        members =>
          parent
            .areMembersKnown(members.toSet)
            .map(knownMembers => members.map(m => m -> knownMembers.contains(m)).toMap)
      ),
    )(logger, "memberCache")

  private val synchronizerParametersCache =
    new AtomicReference[
      Option[FutureUnlessShutdown[Either[String, DynamicSynchronizerParametersWithValidity]]]
    ](None)

  private val sequencingDynamicParametersCache =
    new AtomicReference[
      Option[FutureUnlessShutdown[Either[String, DynamicSequencingParametersWithValidity]]]
    ](
      None
    )

  private val synchronizerParametersChangesCache =
    new AtomicReference[
      Option[FutureUnlessShutdown[Seq[DynamicSynchronizerParametersWithValidity]]]
    ](None)

  private val signingKeysWithThresholdCache: TracedAsyncLoadingCache[
    FutureUnlessShutdown,
    PartyId,
    Option[SigningKeysWithThreshold],
  ] = ScaffeineCache.buildTracedAsync[
    FutureUnlessShutdown,
    PartyId,
    Option[SigningKeysWithThreshold],
  ](
    cache = cachingConfigs.partyCache.buildScaffeine(loggerFactory),
    loader = implicit traceContext => party => parent.signingKeysWithThreshold(party),
  )(logger, "signingKeysWithThresholdCache")

  private val synchronizerUpgradeCache =
    new AtomicReference[
      Option[FutureUnlessShutdown[Option[(SynchronizerSuccessor, EffectiveTime)]]]
    ](None)

  override def allKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[KeyCollection] =
    keyCache.get(owner)

  override def allKeys(
      members: Seq[Member]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]] =
    keyCache.getAll(members)

  override def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PartyInfo] =
    partyCache.get(party)

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext) =
    getAllBatched(parties)(partyCache.getAll(_)(traceContext))

  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]] =
    participantCache.getAll(participants).map(_.collect { case (k, Some(v)) => (k, v) })

  override def loadVettedPackages(participant: ParticipantId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PackageId, VettedPackage]] =
    packageVettingCache.get(participant)

  override def loadVettedPackages(participants: Set[ParticipantId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, Map[PackageId, VettedPackage]]] =
    getAllBatched(participants.toSeq)(packageVettingCache.getAll(_)(traceContext))

  private[client] override def findUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packages: Set[PackageId],
      ledgerTime: CantonTimestamp,
      vettedPackages: Map[PackageId, VettedPackage],
  )(implicit traceContext: TraceContext): UnknownOrUnvettedPackages =
    parent.findUnvettedPackagesOrDependencies(participant, packages, ledgerTime, vettedPackages)

  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[MemberCode],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]] =
    parent.inspectKeys(filterOwner, filterOwnerType, limit)

  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[PartyId]] =
    parent.inspectKnownParties(filterParty, filterParticipant, limit)

  /** returns the list of currently known mediators */
  override def mediatorGroups()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[MediatorGroup]] =
    getAndCache(mediatorsCache, parent.mediatorGroups())

  /** returns the sequencer group if known */
  override def sequencerGroup()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SequencerGroup]] =
    getAndCache(sequencerGroupCache, parent.sequencerGroup())

  /** returns the set of all known members */
  override def allMembers()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] =
    getAndCache(allMembersCache, parent.allMembers())

  override def isMemberKnown(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    memberCache.get(member)

  override def areMembersKnown(members: Set[Member])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[Member]] =
    memberCache.getAll(members).map(_.collect { case (member, _isKnown @ true) => member }.toSet)

  override def memberFirstKnownAt(
      member: Member
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SequencedTime, EffectiveTime)]] =
    parent.memberFirstKnownAt(member)

  /** Returns the value if it is present in the cache. Otherwise, use the `getter` to fetch it and
    * cache the result.
    */
  private def getAndCache[T](
      cache: AtomicReference[Option[FutureUnlessShutdown[T]]],
      getter: => FutureUnlessShutdown[T],
  )(implicit errorLoggingContext: ErrorLoggingContext): FutureUnlessShutdown[T] = {
    val promise = PromiseUnlessShutdown.supervised[T]("getAndCache", futureSupervisor)
    val previousO = cache.getAndSet(Some(promise.futureUS))
    promise.completeWithUS(previousO.getOrElse(getter)).discard
    promise.futureUS
  }

  override def findDynamicSynchronizerParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSynchronizerParametersWithValidity]] =
    getAndCache(synchronizerParametersCache, parent.findDynamicSynchronizerParameters())

  override def findDynamicSequencingParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSequencingParametersWithValidity]] =
    getAndCache(sequencingDynamicParametersCache, parent.findDynamicSequencingParameters())

  /** List all the dynamic synchronizer parameters (past and current) */
  override def listDynamicSynchronizerParametersChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DynamicSynchronizerParametersWithValidity]] =
    getAndCache(
      synchronizerParametersChangesCache,
      parent.listDynamicSynchronizerParametersChanges(),
    )

  override def signingKeysWithThreshold(party: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SigningKeysWithThreshold]] =
    signingKeysWithThresholdCache.get(party)

  private def getAllBatched[K, V](
      keys: Seq[K]
  )(fetchAll: Seq[K] => FutureUnlessShutdown[Map[K, V]]): FutureUnlessShutdown[Map[K, V]] =
    // split up the request into separate chunks so that we don't block the cache for too long
    // when loading very large batches
    MonadUtil
      .batchedSequentialTraverse(batchingConfig.parallelism, batchingConfig.maxItemsInBatch)(keys)(
        fetchAll(_).map(_.toSeq)
      )
      .map(_.toMap)

  override def synchronizerUpgradeOngoing()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SynchronizerSuccessor, EffectiveTime)]] =
    getAndCache(synchronizerUpgradeCache, parent.synchronizerUpgradeOngoing())

  override def sequencerConnectionSuccessors()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SequencerId, SequencerConnectionSuccessor]] =
    parent.sequencerConnectionSuccessors()
}
