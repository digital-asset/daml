// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
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
import com.digitalasset.canton.store.{IndexedStringStore, IndexedSynchronizer, SequencedEventStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** Read-only interface to the [[SyncDomainPersistentStateManager]] */
trait SyncDomainPersistentStateLookup {
  def getAll: Map[SynchronizerId, SyncDomainPersistentState]
}

/** Manages domain state that needs to survive reconnects
  *
  * Factory for [[com.digitalasset.canton.participant.store.SyncDomainPersistentState]]. Tries to discover existing persistent states or create new ones
  * and checks consistency of synchronizer parameters and unique contract key domains
  */
class SyncDomainPersistentStateManager(
    participantId: ParticipantId,
    aliasResolution: SynchronizerAliasResolution,
    storage: Storage,
    val indexedStringStore: IndexedStringStore,
    acsCounterParticipantConfigStore: AcsCounterParticipantConfigStore,
    parameters: ParticipantNodeParameters,
    crypto: Crypto,
    clock: Clock,
    packageDependencyResolver: PackageDependencyResolver,
    ledgerApiStore: Eval[LedgerApiStore],
    val contractStore: Eval[ContractStore],
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SyncDomainPersistentStateLookup
    with AutoCloseable
    with NamedLogging {

  /** Creates [[com.digitalasset.canton.participant.store.SyncDomainPersistentState]]s for all known synchronizer aliases
    * provided that the synchronizer parameters and a sequencer offset are known.
    * Does not check for unique contract key synchronizer constraints.
    * Must not be called concurrently with itself or other methods of this class.
    */
  def initializePersistentStates()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    def getStaticSynchronizerParameters(synchronizerId: SynchronizerId)(implicit
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

    aliasResolution.aliases.toList.parTraverse_ { alias =>
      val resultE = for {
        synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
          aliasResolution.synchronizerIdForAlias(alias).toRight("Unknown synchronizer id")
        )
        synchronizerIdIndexed <- EitherT.right(
          IndexedSynchronizer.indexed(indexedStringStore)(synchronizerId)
        )
        staticSynchronizerParameters <- getStaticSynchronizerParameters(synchronizerId)
        persistentState = createPersistentState(synchronizerIdIndexed, staticSynchronizerParameters)
        _lastProcessedPresent <- persistentState.sequencedEventStore
          .find(SequencedEventStore.LatestUpto(CantonTimestamp.MaxValue))
          .leftMap(_ => "No persistent event")
        _ = logger.debug(s"Discovered existing state for $alias")
      } yield put(persistentState)

      resultE.valueOr(error => logger.debug(s"No state for $alias discovered: $error"))
    }
  }

  def indexedSynchronizerId(
      synchronizerId: SynchronizerId
  ): FutureUnlessShutdown[IndexedSynchronizer] =
    IndexedSynchronizer.indexed(this.indexedStringStore)(synchronizerId)

  /** Retrieves the [[com.digitalasset.canton.participant.store.SyncDomainPersistentState]] from the [[com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager]]
    * for the given synchronizer if there is one. Otherwise creates a new [[com.digitalasset.canton.participant.store.SyncDomainPersistentState]] for the domain
    * and registers it with the [[com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager]].
    * Checks that the [[com.digitalasset.canton.protocol.StaticSynchronizerParameters]] are the same as what has been persisted (if so)
    * and enforces the unique contract key synchronizer constraints.
    *
    * Must not be called concurrently with itself or other methods of this class.
    */
  def lookupOrCreatePersistentState(
      synchronizerAlias: SynchronizerAlias,
      indexedSynchronizer: IndexedSynchronizer,
      synchronizerParameters: StaticSynchronizerParameters,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, SyncDomainPersistentState] = {
    // TODO(#14048) does this method need to be synchronized?
    val persistentState = createPersistentState(indexedSynchronizer, synchronizerParameters)
    for {
      _ <- checkAndUpdateSynchronizerParameters(
        synchronizerAlias,
        persistentState.parameterStore,
        synchronizerParameters,
      )
    } yield {
      // TODO(#14048) potentially delete putIfAbsent
      putIfAbsent(persistentState)
      persistentState
    }
  }

  private def createPersistentState(
      indexedSynchronizer: IndexedSynchronizer,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  ): SyncDomainPersistentState =
    get(indexedSynchronizer.synchronizerId)
      .getOrElse(mkPersistentState(indexedSynchronizer, staticSynchronizerParameters))

  private def checkAndUpdateSynchronizerParameters(
      alias: SynchronizerAlias,
      parameterStore: SynchronizerParameterStore,
      newParameters: StaticSynchronizerParameters,
  )(implicit
      traceContext: TraceContext
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

  def staticSynchronizerParameters(
      synchronizerId: SynchronizerId
  ): Option[StaticSynchronizerParameters] =
    get(synchronizerId).map(_.staticSynchronizerParameters)

  def protocolVersionFor(synchronizerId: SynchronizerId): Option[ProtocolVersion] =
    staticSynchronizerParameters(synchronizerId).map(_.protocolVersion)

  private val sychronizerStates: concurrent.Map[SynchronizerId, SyncDomainPersistentState] =
    TrieMap[SynchronizerId, SyncDomainPersistentState]()

  private def put(state: SyncDomainPersistentState): Unit = {
    val synchronizerId = state.indexedSynchronizer.synchronizerId
    val previous = sychronizerStates.putIfAbsent(synchronizerId, state)
    if (previous.isDefined)
      throw new IllegalArgumentException(s"synchronizer state already exists for $synchronizerId")
  }

  private def putIfAbsent(state: SyncDomainPersistentState): Unit =
    sychronizerStates.putIfAbsent(state.indexedSynchronizer.synchronizerId, state).discard

  def get(synchronizerId: SynchronizerId): Option[SyncDomainPersistentState] =
    sychronizerStates.get(synchronizerId)

  override def getAll: Map[SynchronizerId, SyncDomainPersistentState] = sychronizerStates.toMap

  def getByAlias(synchronizerAlias: SynchronizerAlias): Option[SyncDomainPersistentState] =
    for {
      synchronizerId <- synchronizerIdForAlias(synchronizerAlias)
      res <- get(synchronizerId)
    } yield res

  def synchronizerIdForAlias(synchronizerAlias: SynchronizerAlias): Option[SynchronizerId] =
    aliasResolution.synchronizerIdForAlias(synchronizerAlias)
  def aliasForSynchronizerId(synchronizerId: SynchronizerId): Option[SynchronizerAlias] =
    aliasResolution.aliasForSynchronizerId(synchronizerId)

  private def mkPersistentState(
      indexedSynchronizer: IndexedSynchronizer,
      staticSynchronizerParameters: StaticSynchronizerParameters,
  ): SyncDomainPersistentState = SyncDomainPersistentState
    .create(
      participantId,
      storage,
      indexedSynchronizer,
      staticSynchronizerParameters,
      clock,
      crypto,
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
      synchronizerId: SynchronizerId,
      protocolVersion: ProtocolVersion,
  ): Option[TopologyComponentFactory] =
    get(synchronizerId).map(state =>
      new TopologyComponentFactory(
        synchronizerId,
        protocolVersion,
        crypto,
        clock,
        parameters.processingTimeouts,
        futureSupervisor,
        parameters.cachingConfigs,
        parameters.batchingConfig,
        participantId,
        unsafeEnableOnlinePartyReplication = parameters.unsafeEnableOnlinePartyReplication,
        exitOnFatalFailures = parameters.exitOnFatalFailures,
        state.topologyStore,
        loggerFactory.append("synchronizerId", synchronizerId.toString),
      )
    )

  def synchronizerTopologyStateInitFor(
      synchronizerId: SynchronizerId,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Option[
    SynchronizerTopologyInitializationCallback
  ]] =
    get(synchronizerId) match {
      case None =>
        EitherT.leftT[FutureUnlessShutdown, Option[SynchronizerTopologyInitializationCallback]](
          SynchronizerRegistryError.DomainRegistryInternalError.InvalidState(
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
    LifeCycle.close(sychronizerStates.values.toSeq :+ aliasResolution: _*)(logger)
}
