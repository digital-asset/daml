// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{BatchingConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DynamicDomainParametersWithValidity
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.processing.*
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, SequencerCounter}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Promise}

sealed abstract class BaseCachingDomainTopologyClient(
    protected val clock: Clock,
    parent: DomainTopologyClientWithInit,
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
    parent.updateHead(effectiveTimestamp, approximateTimestamp, potentialTopologyChange)
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
        parent.trySnapshot(ts)(TraceContext.empty),
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

  override def domainId: DomainId = parent.domainId

  override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
    parent.snapshotAvailable(timestamp)
  override def awaitTimestamp(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[Future[Unit]] =
    parent.awaitTimestamp(timestamp, waitForEffectiveTime)

  override def awaitTimestampUS(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[FutureUnlessShutdown[Unit]] =
    parent.awaitTimestampUS(timestamp, waitForEffectiveTime)

  override def approximateTimestamp: CantonTimestamp = parent.approximateTimestamp

  override def currentSnapshotApproximation(implicit
      traceContext: TraceContext
  ): TopologySnapshotLoader = trySnapshot(approximateTimestamp)

  override def topologyKnownUntilTimestamp: CantonTimestamp = parent.topologyKnownUntilTimestamp

  override def await(condition: TopologySnapshot => Future[Boolean], timeout: Duration)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    Boolean
  ] = // we use our implementation such that we can benefit from cached data
    parent.scheduleAwait(condition(currentSnapshotApproximation), timeout)

  override private[topology] def scheduleAwait(condition: => Future[Boolean], timeout: Duration) =
    parent.scheduleAwait(condition, timeout)

  override def close(): Unit = {
    parent.close()
  }

  override def numPendingChanges: Int = parent.numPendingChanges
}

final class CachingDomainTopologyClientOld(
    clock: Clock,
    parent: DomainTopologyClientWithInitOld,
    cachingConfigs: CachingConfigs,
    batchingConfig: BatchingConfig,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends BaseCachingDomainTopologyClient(
      clock,
      parent,
      cachingConfigs,
      batchingConfig,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    with DomainTopologyClientWithInitOld {
  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    if (transactions.nonEmpty) {
      // if there is a transaction, we insert the effective timestamp as a snapshot
      appendSnapshot(effectiveTimestamp.value)
    } else if (snapshots.get().isEmpty) {
      // if we haven't seen any snapshot yet, we use the sequencer time to seed the first snapshot
      appendSnapshot(sequencedTimestamp.value)
    }
    parent.observed(sequencedTimestamp, effectiveTimestamp, sequencerCounter, transactions)
  }

}

final class CachingDomainTopologyClientX(
    clock: Clock,
    parent: DomainTopologyClientWithInitX,
    cachingConfigs: CachingConfigs,
    batchingConfig: BatchingConfig,
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends BaseCachingDomainTopologyClient(
      clock,
      parent,
      cachingConfigs,
      batchingConfig,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
    with DomainTopologyClientWithInitX {
  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    if (transactions.nonEmpty) {
      // if there is a transaction, we insert the effective timestamp as a snapshot
      appendSnapshot(effectiveTimestamp.value)
    } else if (snapshots.get().isEmpty) {
      // if we haven't seen any snapshot yet, we use the sequencer time to seed the first snapshot
      appendSnapshot(sequencedTimestamp.value)
    }
    parent.observed(sequencedTimestamp, effectiveTimestamp, sequencerCounter, transactions)
  }

}

object CachingDomainTopologyClient {

  def create(
      clock: Clock,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      store: TopologyStore[TopologyStoreId.DomainStore],
      initKeys: Map[KeyOwner, Seq[SigningPublicKey]],
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
      cachingConfigs: CachingConfigs,
      batchingConfig: BatchingConfig,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[CachingDomainTopologyClientOld] = {

    val dbClient =
      new StoreBasedDomainTopologyClient(
        clock,
        domainId,
        protocolVersion,
        store,
        initKeys,
        packageDependencies,
        timeouts,
        futureSupervisor,
        loggerFactory,
      )
    val caching =
      new CachingDomainTopologyClientOld(
        clock,
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
  def createX(
      clock: Clock,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      store: TopologyStoreX[TopologyStoreId.DomainStore],
      packageDependencies: PackageId => EitherT[Future, PackageId, Set[PackageId]],
      cachingConfigs: CachingConfigs,
      batchingConfig: BatchingConfig,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[CachingDomainTopologyClientX] = {
    val dbClient =
      new StoreBasedDomainTopologyClientX(
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
      new CachingDomainTopologyClientX(
        clock,
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
  override def participants(): Future[Seq[(ParticipantId, ParticipantPermission)]] =
    parent.participants()
  override def allKeys(owner: KeyOwner): Future[KeyCollection] = parent.allKeys(owner)
  override def findParticipantState(
      participantId: ParticipantId
  ): Future[Option[ParticipantAttributes]] = parent.findParticipantState(participantId)
  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  ): Future[Map[ParticipantId, ParticipantAttributes]] = parent.loadParticipantStates(participants)
  override private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ): Future[PartyInfo] =
    parent.loadActiveParticipantsOf(party, participantStates)
  override def findParticipantCertificate(participantId: ParticipantId)(implicit
      traceContext: TraceContext
  ): Future[Option[LegalIdentityClaimEvidence.X509Cert]] =
    parent.findParticipantCertificate(participantId)

  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[KeyOwnerCode],
      limit: Int,
  ): Future[Map[KeyOwner, KeyCollection]] =
    parent.inspectKeys(filterOwner, filterOwnerType, limit)
  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  ): Future[Set[PartyId]] =
    parent.inspectKnownParties(filterParty, filterParticipant, limit)

  override def findUnvettedPackagesOrDependencies(
      participantId: ParticipantId,
      packages: Set[PackageId],
  ): EitherT[Future, PackageId, Set[PackageId]] =
    parent.findUnvettedPackagesOrDependencies(participantId, packages)

  override private[client] def loadUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packageId: PackageId,
  ): EitherT[Future, PackageId, Set[PackageId]] =
    parent.loadUnvettedPackagesOrDependencies(participant, packageId)

  /** returns the list of currently known mediators */
  override def mediatorGroups(): Future[Seq[MediatorGroup]] = parent.mediatorGroups()

  /** returns the sequencer group if known */
  override def sequencerGroup(): Future[Option[SequencerGroup]] = parent.sequencerGroup()

  override def allMembers(): Future[Set[Member]] = parent.allMembers()

  override def isMemberKnown(member: Member): Future[Boolean] =
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
  ) = parent.loadBatchActiveParticipantsOf(parties, loadParticipantStates)

  override def trafficControlStatus(
      members: Seq[Member]
  ): Future[Map[Member, Option[MemberTrafficControlState]]] =
    parent.trafficControlStatus(members)

  /** Returns the Authority-Of delegations for consortium parties. Non-consortium parties delegate to themselves
    * with threshold one
    */
  override def authorityOf(
      parties: Set[LfPartyId]
  ): Future[PartyTopologySnapshotClient.AuthorityOfResponse] = parent.authorityOf(parties)
}

class CachingTopologySnapshot(
    parent: TopologySnapshotLoader,
    cachingConfigs: CachingConfigs,
    batchingConfig: BatchingConfig,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends TopologySnapshotLoader
    with NamedLogging
    with NoTracing {

  override def timestamp: CantonTimestamp = parent.timestamp

  private val partyCache = cachingConfigs.partyCache
    .buildScaffeine()
    .buildAsyncFuture[PartyId, PartyInfo](
      loader = party => parent.loadActiveParticipantsOf(party, loadParticipantStates),
      allLoader =
        Some(parties => parent.loadBatchActiveParticipantsOf(parties.toSeq, loadParticipantStates)),
    )

  private val participantCache =
    cachingConfigs.participantCache
      .buildScaffeine()
      .buildAsyncFuture[ParticipantId, Option[ParticipantAttributes]](parent.findParticipantState)
  private val keyCache = cachingConfigs.keyCache
    .buildScaffeine()
    .buildAsyncFuture[KeyOwner, KeyCollection](parent.allKeys)

  private val packageVettingCache = cachingConfigs.packageVettingCache
    .buildScaffeine()
    .buildAsyncFuture[(ParticipantId, PackageId), Either[PackageId, Set[PackageId]]](x =>
      loadUnvettedPackagesOrDependencies(x._1, x._2).value
    )

  private val mediatorsCache = new AtomicReference[Option[Future[Seq[MediatorGroup]]]](None)

  private val sequencerGroupCache =
    new AtomicReference[Option[Future[Option[SequencerGroup]]]](None)

  private val allMembersCache = new AtomicReference[Option[Future[Set[Member]]]](None)
  private val memberCache = cachingConfigs.memberCache
    .buildScaffeine()
    .buildAsyncFuture[Member, Boolean](parent.isMemberKnown)

  private val domainParametersCache =
    new AtomicReference[Option[Future[Either[String, DynamicDomainParametersWithValidity]]]](None)

  private val domainParametersChangesCache =
    new AtomicReference[
      Option[Future[Seq[DynamicDomainParametersWithValidity]]]
    ](None)

  private val domainTrafficControlStateCache = cachingConfigs.trafficStatusCache
    .buildScaffeine()
    .buildAsyncFuture[Member, Option[MemberTrafficControlState]](
      loader = member =>
        parent
          .trafficControlStatus(Seq(member))
          .map(_.get(member).flatten),
      allLoader = Some(members => parent.trafficControlStatus(members.toSeq)),
    )

  private val authorityOfCache = cachingConfigs.partyCache
    .buildScaffeine()
    .buildAsyncFuture[Set[LfPartyId], PartyTopologySnapshotClient.AuthorityOfResponse](
      loader = party => parent.authorityOf(party)
    )

  override def participants(): Future[Seq[(ParticipantId, ParticipantPermission)]] =
    parent.participants()

  override def allKeys(owner: KeyOwner): Future[KeyCollection] = keyCache.get(owner)

  override def findParticipantState(
      participantId: ParticipantId
  ): Future[Option[ParticipantAttributes]] =
    participantCache.get(participantId)

  override def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ): Future[PartyInfo] =
    partyCache.get(party)

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => Future[Map[ParticipantId, ParticipantAttributes]],
  ) = {
    // split up the request into separate chunks so that we don't block the cache for too long
    // when loading very large batches
    MonadUtil
      .batchedSequentialTraverse(batchingConfig.parallelism, batchingConfig.maxItemsInSqlClause)(
        parties
      )(
        partyCache.getAll(_).map(_.toSeq)
      )
      .map(_.toMap)
  }

  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  ): Future[Map[ParticipantId, ParticipantAttributes]] =
    participants
      .parTraverse(participant => participantState(participant).map((participant, _)))
      .map(_.toMap)

  override def findParticipantCertificate(
      participantId: ParticipantId
  )(implicit traceContext: TraceContext): Future[Option[LegalIdentityClaimEvidence.X509Cert]] = {
    // This one is not cached as we don't need during processing
    parent.findParticipantCertificate(participantId)
  }

  override def findUnvettedPackagesOrDependencies(
      participantId: ParticipantId,
      packages: Set[PackageId],
  ): EitherT[Future, PackageId, Set[PackageId]] =
    findUnvettedPackagesOrDependenciesUsingLoader(
      participantId,
      packages,
      (x, y) => EitherT(packageVettingCache.get((x, y))),
    )

  private[client] def loadUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packageId: PackageId,
  ): EitherT[Future, PackageId, Set[PackageId]] =
    parent.loadUnvettedPackagesOrDependencies(participant, packageId)

  override def inspectKeys(
      filterOwner: String,
      filterOwnerType: Option[KeyOwnerCode],
      limit: Int,
  ): Future[Map[KeyOwner, KeyCollection]] =
    parent.inspectKeys(filterOwner, filterOwnerType, limit)

  override def inspectKnownParties(
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  ): Future[Set[PartyId]] =
    parent.inspectKnownParties(filterParty, filterParticipant, limit)

  /** returns the list of currently known mediators */
  override def mediatorGroups(): Future[Seq[MediatorGroup]] =
    getAndCache(mediatorsCache, parent.mediatorGroups())

  /** returns the sequencer group if known */
  override def sequencerGroup(): Future[Option[SequencerGroup]] =
    getAndCache(sequencerGroupCache, parent.sequencerGroup())

  /** returns the set of all known members */
  override def allMembers(): Future[Set[Member]] = getAndCache(allMembersCache, parent.allMembers())

  override def isMemberKnown(member: Member): Future[Boolean] =
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
  ): Future[PartyTopologySnapshotClient.AuthorityOfResponse] =
    authorityOfCache.get(parties)

  override def trafficControlStatus(
      members: Seq[Member]
  ): Future[Map[Member, Option[MemberTrafficControlState]]] = {
    domainTrafficControlStateCache.getAll(members)
  }
}
