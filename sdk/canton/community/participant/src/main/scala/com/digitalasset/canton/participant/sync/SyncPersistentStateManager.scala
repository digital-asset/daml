// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.environment.{
  StoreBasedSynchronizerTopologyInitializationCallback,
  SynchronizerTopologyInitializationCallback,
}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.synchronizer.{
  SynchronizerAliasResolution,
  SynchronizerRegistryError,
}
import com.digitalasset.canton.participant.topology.TopologyComponentFactory
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.{
  IndexedPhysicalSynchronizer,
  IndexedStringStore,
  IndexedSynchronizer,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.StampedLockWithHandle

import scala.annotation.unused
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** Read-only interface to the [[SyncPersistentStateManager]] */
trait SyncPersistentStateLookup {
  def getAll: Map[PhysicalSynchronizerId, SyncPersistentState]

  /** Return the latest [[com.digitalasset.canton.participant.store.SyncPersistentState]] (wrt to
    * [[com.digitalasset.canton.topology.PhysicalSynchronizerId]]) for each
    * [[com.digitalasset.canton.topology.SynchronizerId]]
    */
  def getAllLatest: Map[SynchronizerId, SyncPersistentState] =
    getAll.values.toSeq
      .groupBy1(_.physicalSynchronizerId.logical)
      .view
      .mapValues(_.maxBy1(_.physicalSynchronizerId))
      .toMap

  /** Return the latest [[com.digitalasset.canton.participant.store.SyncPersistentState]] (wrt to
    * [[com.digitalasset.canton.topology.PhysicalSynchronizerId]]) for `synchronizerAlias`
    */
  def getLatest(synchronizerAlias: SynchronizerAlias): Option[SyncPersistentState] =
    synchronizerIdForAlias(synchronizerAlias).map(getAllLatest)

  def getAllFor(id: SynchronizerId): Seq[SyncPersistentState]

  def synchronizerIdsForAlias(
      synchronizerAlias: SynchronizerAlias
  ): Option[NonEmpty[Set[PhysicalSynchronizerId]]]

  def allKnownLSIds: Set[SynchronizerId] = getAll.keySet.map(_.logical)

  def synchronizerIdForAlias(synchronizerAlias: SynchronizerAlias): Option[SynchronizerId] =
    synchronizerIdsForAlias(synchronizerAlias).map(_.head1.logical)
  def aliasForSynchronizerId(id: SynchronizerId): Option[SynchronizerAlias]

  def acsInspection(synchronizerId: SynchronizerId): Option[AcsInspection]
  def acsInspection(synchronizerAlias: SynchronizerAlias): Option[AcsInspection] =
    synchronizerIdForAlias(synchronizerAlias).flatMap(acsInspection)

  def reassignmentStore(synchronizerId: SynchronizerId): Option[ReassignmentStore]
  def reassignmentStore(synchronizerAlias: SynchronizerAlias): Option[ReassignmentStore] =
    synchronizerIdForAlias(synchronizerAlias).flatMap(reassignmentStore)

  def acsCommitmentStore(synchronizerId: SynchronizerId): Option[AcsCommitmentStore]
  def acsCommitmentStore(synchronizerAlias: SynchronizerAlias): Option[AcsCommitmentStore] =
    synchronizerIdForAlias(synchronizerAlias).flatMap(acsCommitmentStore)

  def activeContractStore(synchronizerId: SynchronizerId): Option[ActiveContractStore]
  def activeContractStore(synchronizerAlias: SynchronizerAlias): Option[ActiveContractStore] =
    synchronizerIdForAlias(synchronizerAlias).flatMap(activeContractStore)
}

/** Manages participant-relevant state for a synchronizer that needs to survive reconnects
  *
  * Factory for [[com.digitalasset.canton.participant.store.SyncPersistentState]]. Tries to discover
  * existing persistent states or create new ones and checks consistency of synchronizer parameters
  * and unique contract key synchronizers
  */
class SyncPersistentStateManager(
    participantId: ParticipantId,
    aliasResolution: SynchronizerAliasResolution,
    storage: Storage,
    val indexedStringStore: IndexedStringStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    parameters: ParticipantNodeParameters,
    synchronizerCryptoFactory: StaticSynchronizerParameters => SynchronizerCrypto,
    clock: Clock,
    packageDependencyResolver: PackageDependencyResolver,
    ledgerApiStore: Eval[LedgerApiStore],
    val contractStore: Eval[ContractStore],
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SyncPersistentStateLookup
    with StaticSynchronizerParametersGetter
    with AutoCloseable
    with NamedLogging {

  private val lock = new StampedLockWithHandle()

  /** Creates [[com.digitalasset.canton.participant.store.SyncPersistentState]]s for all known
    * synchronizer aliases provided that the synchronizer parameters and a sequencer offset are
    * known. Does not check for unique contract key synchronizer constraints. Must not be called
    * concurrently with itself or other methods of this class.
    */
  def initializePersistentStates()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = lock.withWriteLockHandle { implicit lockHandle =>
    def getStaticSynchronizerParameters(synchronizerId: PhysicalSynchronizerId)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, StaticSynchronizerParameters] =
      EitherT
        .fromOptionF(
          SynchronizerParameterStore(
            storage,
            synchronizerId,
            parameters.processingTimeouts,
            loggerFactory,
          ).lastParameters,
          "No synchronizer parameters in store",
        )

    aliasResolution.physicalSynchronizerIds.toList.parTraverse_ { psid =>
      val resultE = for {
        synchronizerIdIndexed <- EitherT.right(
          IndexedSynchronizer.indexed(indexedStringStore)(psid.logical)
        )
        psidIndexed <- EitherT.right(
          IndexedPhysicalSynchronizer.indexed(indexedStringStore)(psid)
        )
        staticSynchronizerParameters <- getStaticSynchronizerParameters(psid)
        persistentState = createPersistentState(
          psidIndexed,
          synchronizerIdIndexed,
          staticSynchronizerParameters,
        )
        _ = logger.debug(s"Discovered existing state for $psid")
      } yield {
        val synchronizerId = persistentState.physicalSynchronizerIdx.synchronizerId

        val previous = persistentStates.putIfAbsent(synchronizerId, persistentState)
        if (previous.isDefined)
          throw new IllegalArgumentException(
            s"synchronizer state already exists for $synchronizerId"
          )
      }

      resultE.valueOr(error => logger.debug(s"No state for $psid discovered: $error"))
    }
  }

  def getSynchronizerIdx(
      synchronizerId: SynchronizerId
  ): FutureUnlessShutdown[IndexedSynchronizer] =
    IndexedSynchronizer.indexed(this.indexedStringStore)(synchronizerId)

  def getPhysicalSynchronizerIdx(
      synchronizerId: PhysicalSynchronizerId
  ): FutureUnlessShutdown[IndexedPhysicalSynchronizer] =
    IndexedPhysicalSynchronizer.indexed(this.indexedStringStore)(synchronizerId)

  /** Retrieves the [[com.digitalasset.canton.participant.store.SyncPersistentState]] from the
    * [[com.digitalasset.canton.participant.sync.SyncPersistentStateManager]] for the given
    * synchronizer if there is one. Otherwise creates a new
    * [[com.digitalasset.canton.participant.store.SyncPersistentState]] for the synchronizer and
    * registers it with the [[com.digitalasset.canton.participant.sync.SyncPersistentStateManager]].
    * Checks that the [[com.digitalasset.canton.protocol.StaticSynchronizerParameters]] are the same
    * as what has been persisted (if so) and enforces the unique contract key synchronizer
    * constraints.
    *
    * Must not be called concurrently with itself or other methods of this class.
    */
  def lookupOrCreatePersistentState(
      synchronizerAlias: SynchronizerAlias,
      physicalSynchronizerIdx: IndexedPhysicalSynchronizer,
      synchronizerIdx: IndexedSynchronizer,
      synchronizerParameters: StaticSynchronizerParameters,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, SyncPersistentState] =
    lock.withWriteLockHandle { implicit writeLockHandle =>
      val persistentState =
        createPersistentState(physicalSynchronizerIdx, synchronizerIdx, synchronizerParameters)
      for {
        _ <- checkAndUpdateSynchronizerParameters(
          synchronizerAlias,
          persistentState.parameterStore,
          synchronizerParameters,
        )
      } yield {
        persistentStates
          .putIfAbsent(persistentState.physicalSynchronizerIdx.synchronizerId, persistentState)
          .getOrElse(persistentState)
      }
    }

  private def createPersistentState(
      physicalSynchronizerIdx: IndexedPhysicalSynchronizer,
      synchronizerIdx: IndexedSynchronizer,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  )(implicit writeLockHandle: lock.WriteLockHandle): SyncPersistentState =
    persistentStates.getOrElse(
      physicalSynchronizerIdx.synchronizerId,
      mkPersistentState(physicalSynchronizerIdx, synchronizerIdx, staticSynchronizerParameters),
    )

  private def checkAndUpdateSynchronizerParameters(
      alias: SynchronizerAlias,
      parameterStore: SynchronizerParameterStore,
      newParameters: StaticSynchronizerParameters,
  )(implicit
      traceContext: TraceContext,
      @unused writeLockHandle: lock.WriteLockHandle,
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Unit] =
    for {
      oldParametersO <- EitherT.right(parameterStore.lastParameters)
      _ <- oldParametersO match {
        case None =>
          // Store the parameters
          logger.debug(s"Storing synchronizer parameters for synchronizer $alias: $newParameters")
          EitherT.right[SynchronizerRegistryError](parameterStore.setParameters(newParameters))
        case Some(oldParameters) =>
          EitherT.cond[FutureUnlessShutdown](
            oldParameters == newParameters,
            (),
            SynchronizerRegistryError.ConfigurationErrors.SynchronizerParametersChanged
              .Error(oldParametersO, newParameters): SynchronizerRegistryError,
          )
      }
    } yield ()

  override def acsInspection(synchronizerId: SynchronizerId): Option[AcsInspection] =
    for {
      // Taking any of the PSIds is fine since ACS is logical
      psid <- aliasResolution.physicalSynchronizerIds(synchronizerId).headOption
      res <- get(psid)
    } yield res.acsInspection

  override def reassignmentStore(synchronizerId: SynchronizerId): Option[ReassignmentStore] =
    for {
      // Taking any of the PSIds is fine since reassignment store is logical
      psid <- aliasResolution.physicalSynchronizerIds(synchronizerId).headOption
      res <- get(psid)
    } yield res.reassignmentStore

  override def acsCommitmentStore(synchronizerId: SynchronizerId): Option[AcsCommitmentStore] =
    for {
      // Taking any of the PSIds is fine since ACS commitment store is logical
      psid <- aliasResolution.physicalSynchronizerIds(synchronizerId).headOption
      res <- get(psid)
    } yield res.acsCommitmentStore

  override def activeContractStore(synchronizerId: SynchronizerId): Option[ActiveContractStore] =
    for {
      // Taking any of the PSIds is fine since ACS commitment store is logical
      psid <- aliasResolution.physicalSynchronizerIds(synchronizerId).headOption
      res <- get(psid)
    } yield res.activeContractStore

  override def staticSynchronizerParameters(
      synchronizerId: PhysicalSynchronizerId
  ): Option[StaticSynchronizerParameters] =
    get(synchronizerId).map(_.staticSynchronizerParameters)

  override def latestKnownPSId(
      synchronizerId: SynchronizerId
  ): Option[PhysicalSynchronizerId] =
    getAll.keySet.filter(_.logical == synchronizerId).maxOption

  private val persistentStates: concurrent.Map[PhysicalSynchronizerId, SyncPersistentState] =
    TrieMap[PhysicalSynchronizerId, SyncPersistentState]()

  def get(synchronizerId: PhysicalSynchronizerId): Option[SyncPersistentState] =
    lock.withReadLock[Option[SyncPersistentState]](persistentStates.get(synchronizerId))

  override def getAll: Map[PhysicalSynchronizerId, SyncPersistentState] =
    // no lock needed here. just return the current snapshot
    persistentStates.toMap

  override def getAllFor(id: SynchronizerId): Seq[SyncPersistentState] =
    lock.withReadLock[Seq[SyncPersistentState]](
      persistentStates.values.filter(_.physicalSynchronizerId.logical == id).toSeq
    )

  override def synchronizerIdForAlias(
      synchronizerAlias: SynchronizerAlias
  ): Option[SynchronizerId] =
    aliasResolution.synchronizerIdForAlias(synchronizerAlias)

  override def synchronizerIdsForAlias(
      synchronizerAlias: SynchronizerAlias
  ): Option[NonEmpty[Set[PhysicalSynchronizerId]]] =
    aliasResolution.synchronizerIdsForAlias(synchronizerAlias)

  override def aliasForSynchronizerId(synchronizerId: SynchronizerId): Option[SynchronizerAlias] =
    aliasResolution.aliasForSynchronizerId(synchronizerId)

  private def mkPersistentState(
      physicalSynchronizerIdx: IndexedPhysicalSynchronizer,
      synchronizerIdx: IndexedSynchronizer,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  )(implicit @unused writeLockHandle: lock.WriteLockHandle): SyncPersistentState =
    SyncPersistentState
      .create(
        participantId,
        storage,
        physicalSynchronizerIdx,
        synchronizerIdx,
        staticSynchronizerParameters,
        clock,
        synchronizerCryptoFactory(staticSynchronizerParameters),
        parameters,
        indexedStringStore,
        acsCounterParticipantConfigStore,
        packageDependencyResolver,
        ledgerApiStore,
        contractStore,
        loggerFactory,
        futureSupervisor,
      )

  def topologyFactoryFor(
      psid: PhysicalSynchronizerId
  ): Option[TopologyComponentFactory] =
    get(psid).map(state =>
      new TopologyComponentFactory(
        psid,
        synchronizerCryptoFactory(state.staticSynchronizerParameters),
        clock,
        parameters.processingTimeouts,
        futureSupervisor,
        parameters.cachingConfigs,
        parameters.batchingConfig,
        participantId,
        parameters.unsafeOnlinePartyReplication,
        exitOnFatalFailures = parameters.exitOnFatalFailures,
        state.topologyStore,
        loggerFactory.append("synchronizerId", psid.toString),
      )
    )

  def synchronizerTopologyStateInitFor(
      synchronizerId: PhysicalSynchronizerId,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Option[
    SynchronizerTopologyInitializationCallback
  ]] =
    get(synchronizerId) match {
      case None =>
        EitherT.leftT[FutureUnlessShutdown, Option[SynchronizerTopologyInitializationCallback]](
          SynchronizerRegistryError.SynchronizerRegistryInternalError.InvalidState(
            s"topology factory for synchronizer $synchronizerId is unavailable"
          )
        )

      case Some(state) =>
        EitherT.right(
          state.topologyStore
            .findFirstTrustCertificateForParticipant(participantId)
            .map(trustCert =>
              // only if the participant's trustCert is not yet in the topology store do we have to initialize it.
              // The callback will fetch the essential topology state from the sequencer
              Option.when(trustCert.isEmpty)(
                new StoreBasedSynchronizerTopologyInitializationCallback(
                  participantId
                )
              )
            )
        )

    }

  override def close(): Unit =
    LifeCycle.close(persistentStates.values.toSeq :+ aliasResolution: _*)(logger)
}
