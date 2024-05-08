// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.data.EitherT
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParametersWithValidity
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.processing.*
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{TraceContext, TracedScaffeine}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, SequencerCounter}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}

final class CachingDomainTopologyClient(
    delegate: DomainTopologyClientWithInit,
    cachingConfigs: CachingConfigs,
    batchingConfig: BatchingConfig,
    val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends DomainTopologyClientWithInit
    with NamedLogging {

  override def updateHead(
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
      potentialTopologyChange: Boolean,
  )(implicit traceContext: TraceContext): Unit = {
    if (snapshots.get().isEmpty) {
      appendSnapshot(approximateTimestamp.value)
    }
    if (potentialTopologyChange)
      appendSnapshot(effectiveTimestamp.value)
    delegate.updateHead(effectiveTimestamp, approximateTimestamp, potentialTopologyChange)
  }

  // snapshot caching entry
  // this one is quite a special cache. generally, we want to avoid loading too much data from the database.
  // now, we know that if there was no identity update between tx and ty, then snapshot(ty) == snapshot(tx)
  // therefore, we remember the list of timestamps when updates happened and used that list in order to figure
  // out which snapshot we can use instead of loading the data again and again.
  // so we use the snapshots list to figure out the update timestamp and then we use the pointwise cache
  // to load that update timestamp.
  protected class SnapshotEntry(val timestamp: CantonTimestamp) {
    def get(): CachingTopologySnapshot = pointwise.get(timestamp.immediateSuccessor)
  }
  protected val snapshots = new AtomicReference[List[SnapshotEntry]](List.empty)

  private val pointwise = cachingConfigs.topologySnapshot
    .buildScaffeine()
    .build[CantonTimestamp, CachingTopologySnapshot] { (ts: CantonTimestamp) =>
      new CachingTopologySnapshot(
        delegate.trySnapshot(ts)(TraceContext.empty),
        cachingConfigs,
        batchingConfig,
        loggerFactory,
      )
    }

  protected def appendSnapshot(timestamp: CantonTimestamp): Unit = {
    val item = new SnapshotEntry(timestamp)
    val _ = snapshots.updateAndGet { cur =>
      if (cur.headOption.exists(_.timestamp > timestamp))
        cur
      else
        item :: (cur.filter(
          _.timestamp.plusMillis(
            cachingConfigs.topologySnapshot.expireAfterAccess.duration.toMillis
          ) > timestamp
        ))
    }
  }

  override def trySnapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): TopologySnapshotLoader = {
    ErrorUtil.requireArgument(
      timestamp <= topologyKnownUntilTimestamp,
      s"requested snapshot=$timestamp, available snapshot=$topologyKnownUntilTimestamp",
    )
    // find a matching existing snapshot
    val cur =
      snapshots.get().find(_.timestamp < timestamp) // note that timestamps are asOf exclusive
    cur match {
      // we'll use the cached snapshot client which defines the time-period this timestamp is in
      case Some(snapshotEntry) =>
        new ForwardingTopologySnapshotClient(timestamp, snapshotEntry.get(), loggerFactory)
      // this timestamp is outside of the window where we have tracked the timestamps of changes.
      // so let's do this pointwise
      case None =>
        pointwise.get(timestamp)
    }

  }

  override def domainId: DomainId = delegate.domainId

  override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
    delegate.snapshotAvailable(timestamp)
  override def awaitTimestamp(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[Future[Unit]] =
    delegate.awaitTimestamp(timestamp, waitForEffectiveTime)

  override def awaitTimestampUS(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[FutureUnlessShutdown[Unit]] =
    delegate.awaitTimestampUS(timestamp, waitForEffectiveTime)

  override def approximateTimestamp: CantonTimestamp = delegate.approximateTimestamp

  override def currentSnapshotApproximation(implicit
      traceContext: TraceContext
  ): TopologySnapshotLoader = trySnapshot(approximateTimestamp)

  override def topologyKnownUntilTimestamp: CantonTimestamp = delegate.topologyKnownUntilTimestamp

  override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Boolean
  ] = // we use our implementation such that we can benefit from cached data
    delegate.scheduleAwait(condition(currentSnapshotApproximation), timeout)

  override private[topology] def scheduleAwait(condition: => Future[Boolean], timeout: Duration) =
    delegate.scheduleAwait(condition, timeout)

  override def close(): Unit = {
    delegate.close()
  }

  override def numPendingChanges: Int = delegate.numPendingChanges

  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    if (transactions.nonEmpty) {
      // if there is a transaction, we insert the effective timestamp as a snapshot
      appendSnapshot(effectiveTimestamp.value)
    } else if (snapshots.get().isEmpty) {
      // if we haven't seen any snapshot yet, we use the sequencer time to seed the first snapshot
      appendSnapshot(sequencedTimestamp.value)
    }
    // make sure the inner client gets the tracker as well
    domainTimeTracker.get.foreach(delegate.setDomainTimeTracker)
    delegate.observed(sequencedTimestamp, effectiveTimestamp, sequencerCounter, transactions)
  }

}

object CachingDomainTopologyClient {

  def create(
      clock: Clock,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      store: TopologyStore[TopologyStoreId.DomainStore],
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
      cachingConfigs: CachingConfigs,
      batchingConfig: BatchingConfig,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[CachingDomainTopologyClient] = {
    val dbClient =
      new StoreBasedDomainTopologyClient(
        clock,
        domainId,
        protocolVersion,
        store,
        packageDependencies,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )
    val caching =
      new CachingDomainTopologyClient(
        dbClient,
        cachingConfigs,
        batchingConfig,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )
    store.maxTimestamp().map { x =>
      x.foreach { case (_, effective) =>
        caching
          .updateHead(effective, effective.toApproximate, potentialTopologyChange = true)
      }
      caching
    }
  }
}

/** simple wrapper class in order to "override" the timestamp we are returning here */
private class ForwardingTopologySnapshotClient(
    override val timestamp: CantonTimestamp,
    parent: TopologySnapshotLoader,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TopologySnapshotLoader {

  override def referenceTime: CantonTimestamp = parent.timestamp
  override def participants()(implicit
      traceContext: TraceContext
  ): Future[Seq[(ParticipantId, ParticipantPermission)]] =
    parent.participants()
  override def allKeys(owner: Member)(implicit traceContext: TraceContext): Future[KeyCollection] =
    parent.allKeys(owner)
  override def allKeys(members: Seq[Member])(implicit
      traceContext: TraceContext
  ): Future[Map[Member, KeyCollection]] = parent.allKeys(members)
  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  )(implicit traceContext: TraceContext): Future[Map[ParticipantId, ParticipantAttributes]] =
    parent.loadParticipantStates(participants)
  override private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  )(implicit traceContext: TraceContext): Future[PartyInfo] =
    parent.loadActiveParticipantsOf(party, participantStates)

  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[MemberCode],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Map[Member, KeyCollection]] =
    parent.inspectKeys(filterOwner, filterOwnerType, limit)
  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]] =
    parent.inspectKnownParties(filterParty, filterParticipant, limit)

  override def findUnvettedPackagesOrDependencies(
      participantId: ParticipantId,
      packages: Set[PackageId],
  )(implicit traceContext: TraceContext): EitherT[Future, PackageId, Set[PackageId]] =
    parent.findUnvettedPackagesOrDependencies(participantId, packages)

  override private[client] def loadUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packageId: PackageId,
  )(implicit traceContext: TraceContext): EitherT[Future, PackageId, Set[PackageId]] =
    parent.loadUnvettedPackagesOrDependencies(participant, packageId)

  /** returns the list of currently known mediators */
  override def mediatorGroups()(implicit traceContext: TraceContext): Future[Seq[MediatorGroup]] =
    parent.mediatorGroups()

  /** returns the sequencer group if known */
  override def sequencerGroup()(implicit
      traceContext: TraceContext
  ): Future[Option[SequencerGroup]] = parent.sequencerGroup()

  override def allMembers()(implicit traceContext: TraceContext): Future[Set[Member]] =
    parent.allMembers()

  override def isMemberKnown(member: Member)(implicit traceContext: TraceContext): Future[Boolean] =
    parent.isMemberKnown(member)

  override def findDynamicDomainParameters()(implicit
      traceContext: TraceContext
  ): Future[Either[String, DynamicDomainParametersWithValidity]] =
    parent.findDynamicDomainParameters()

  /** List all the dynamic domain parameters (past and current) */
  override def listDynamicDomainParametersChanges()(implicit
      traceContext: TraceContext
  ): Future[Seq[DynamicDomainParametersWithValidity]] =
    parent.listDynamicDomainParametersChanges()

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  )(implicit traceContext: TraceContext) =
    parent.loadBatchActiveParticipantsOf(parties, loadParticipantStates)

  /** Returns the Authority-Of delegations for consortium parties. Non-consortium parties delegate to themselves
    * with threshold one
    */
  override def authorityOf(
      parties: Set[LfPartyId]
  )(implicit traceContext: TraceContext): Future[PartyTopologySnapshotClient.AuthorityOfResponse] =
    parent.authorityOf(parties)
}

class CachingTopologySnapshot(
    parent: TopologySnapshotLoader,
    cachingConfigs: CachingConfigs,
    batchingConfig: BatchingConfig,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends TopologySnapshotLoader
    with NamedLogging {

  override def timestamp: CantonTimestamp = parent.timestamp

  private val partyCache =
    TracedScaffeine.buildTracedAsyncFuture[PartyId, PartyInfo](
      cache = cachingConfigs.partyCache.buildScaffeine(),
      loader = traceContext =>
        party =>
          parent
            .loadActiveParticipantsOf(party, loadParticipantStates(_)(traceContext))(traceContext),
      allLoader = Some(traceContext =>
        parties =>
          parent
            .loadBatchActiveParticipantsOf(parties.toSeq, loadParticipantStates(_)(traceContext))(
              traceContext
            )
      ),
    )(logger)

  private val participantCache = TracedScaffeine
    .buildTracedAsyncFuture[ParticipantId, Option[ParticipantAttributes]](
      cache = cachingConfigs.participantCache.buildScaffeine(),
      loader = traceContext => pid => parent.findParticipantState(pid)(traceContext),
      allLoader = Some(traceContext =>
        pids =>
          parent.loadParticipantStates(pids.toSeq)(traceContext).map { attributes =>
            // make sure that the returned map contains an entry for each input element
            pids.map(pid => pid -> attributes.get(pid)).toMap
          }
      ),
    )(logger)
  private val keyCache = TracedScaffeine
    .buildTracedAsyncFuture[Member, KeyCollection](
      cache = cachingConfigs.keyCache.buildScaffeine(),
      loader = traceContext => member => parent.allKeys(member)(traceContext),
      allLoader = Some(traceContext =>
        members =>
          parent
            .allKeys(members.toSeq)(traceContext)
            .map(foundKeys =>
              // make sure that the returned map contains an entry for each input element
              members
                .map(member => member -> foundKeys.getOrElse(member, KeyCollection.empty))
                .toMap
            )
      ),
    )(logger)

  private val packageVettingCache =
    TracedScaffeine
      .buildTracedAsyncFuture[(ParticipantId, PackageId), Either[PackageId, Set[PackageId]]](
        cache = cachingConfigs.packageVettingCache.buildScaffeine(),
        traceContext => x => loadUnvettedPackagesOrDependencies(x._1, x._2)(traceContext).value,
      )(logger)

  private val mediatorsCache = new AtomicReference[Option[Future[Seq[MediatorGroup]]]](None)

  private val sequencerGroupCache =
    new AtomicReference[Option[Future[Option[SequencerGroup]]]](None)

  private val allMembersCache = new AtomicReference[Option[Future[Set[Member]]]](None)
  private val memberCache =
    TracedScaffeine
      .buildTracedAsyncFuture[Member, Boolean](
        cache = cachingConfigs.memberCache.buildScaffeine(),
        traceContext => member => parent.isMemberKnown(member)(traceContext),
      )(logger)

  private val domainParametersCache =
    new AtomicReference[Option[Future[Either[String, DynamicDomainParametersWithValidity]]]](None)

  private val domainParametersChangesCache =
    new AtomicReference[
      Option[Future[Seq[DynamicDomainParametersWithValidity]]]
    ](None)

  private val authorityOfCache =
    TracedScaffeine
      .buildTracedAsyncFuture[Set[LfPartyId], PartyTopologySnapshotClient.AuthorityOfResponse](
        cache = cachingConfigs.partyCache.buildScaffeine(),
        loader = traceContext => party => parent.authorityOf(party)(traceContext),
      )(logger)

  override def participants()(implicit
      traceContext: TraceContext
  ): Future[Seq[(ParticipantId, ParticipantPermission)]] =
    parent.participants()

  override def allKeys(owner: Member)(implicit traceContext: TraceContext): Future[KeyCollection] =
    keyCache.get(owner)

  override def allKeys(
      members: Seq[Member]
  )(implicit traceContext: TraceContext): Future[Map[Member, KeyCollection]] =
    keyCache.getAll(members)

  override def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  )(implicit traceContext: TraceContext): Future[PartyInfo] =
    partyCache.get(party)

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  )(implicit traceContext: TraceContext) = {
    // split up the request into separate chunks so that we don't block the cache for too long
    // when loading very large batches
    MonadUtil
      .batchedSequentialTraverse(batchingConfig.parallelism, batchingConfig.maxItemsInSqlClause)(
        parties
      )(parties => partyCache.getAll(parties)(traceContext).map(_.toSeq))
      .map(_.toMap)
  }

  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  )(implicit traceContext: TraceContext): Future[Map[ParticipantId, ParticipantAttributes]] =
    participantCache.getAll(participants).map(_.collect { case (k, Some(v)) => (k, v) })
  override def findUnvettedPackagesOrDependencies(
      participantId: ParticipantId,
      packages: Set[PackageId],
  )(implicit traceContext: TraceContext): EitherT[Future, PackageId, Set[PackageId]] =
    findUnvettedPackagesOrDependenciesUsingLoader(
      participantId,
      packages,
      (x, y) => EitherT(packageVettingCache.get((x, y))),
    )

  private[client] def loadUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packageId: PackageId,
  )(implicit traceContext: TraceContext): EitherT[Future, PackageId, Set[PackageId]] =
    parent.loadUnvettedPackagesOrDependencies(participant, packageId)

  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[MemberCode],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Map[Member, KeyCollection]] =
    parent.inspectKeys(filterOwner, filterOwnerType, limit)

  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]] =
    parent.inspectKnownParties(filterParty, filterParticipant, limit)

  /** returns the list of currently known mediators */
  override def mediatorGroups()(implicit traceContext: TraceContext): Future[Seq[MediatorGroup]] =
    getAndCache(mediatorsCache, parent.mediatorGroups())

  /** returns the sequencer group if known */
  override def sequencerGroup()(implicit
      traceContext: TraceContext
  ): Future[Option[SequencerGroup]] =
    getAndCache(sequencerGroupCache, parent.sequencerGroup())

  /** returns the set of all known members */
  override def allMembers()(implicit traceContext: TraceContext): Future[Set[Member]] =
    getAndCache(allMembersCache, parent.allMembers())

  override def isMemberKnown(member: Member)(implicit traceContext: TraceContext): Future[Boolean] =
    memberCache.get(member)

  /** Returns the value if it is present in the cache. Otherwise, use the
    * `getter` to fetch it and cache the result.
    */
  private def getAndCache[T](
      cache: AtomicReference[Option[Future[T]]],
      getter: => Future[T],
  ): Future[T] = {
    val promise = Promise[T]()
    val previousO = cache.getAndSet(Some(promise.future))
    promise.completeWith(previousO.getOrElse(getter))
    promise.future
  }

  override def findDynamicDomainParameters()(implicit
      traceContext: TraceContext
  ): Future[Either[String, DynamicDomainParametersWithValidity]] =
    getAndCache(domainParametersCache, parent.findDynamicDomainParameters())

  /** List all the dynamic domain parameters (past and current) */
  override def listDynamicDomainParametersChanges()(implicit
      traceContext: TraceContext
  ): Future[Seq[DynamicDomainParametersWithValidity]] =
    getAndCache(domainParametersChangesCache, parent.listDynamicDomainParametersChanges())

  /** Returns the Authority-Of delegations for consortium parties. Non-consortium parties delegate to themselves
    * with threshold one
    */
  override def authorityOf(
      parties: Set[LfPartyId]
  )(implicit traceContext: TraceContext): Future[PartyTopologySnapshotClient.AuthorityOfResponse] =
    authorityOfCache.get(parties)
}
