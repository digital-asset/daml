// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.{
  DomainTopologyInitializationCallback,
  StoreBasedDomainTopologyInitializationCallback,
}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.domain.{DomainAliasResolution, DomainRegistryError}
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.topology.TopologyComponentFactory
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore, SequencedEventStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Read-only interface to the [[SyncDomainPersistentStateManager]] */
trait SyncDomainPersistentStateLookup {
  def getAll: Map[SynchronizerId, SyncDomainPersistentState]
}

/** Manages domain state that needs to survive reconnects
  *
  * Factory for [[com.digitalasset.canton.participant.store.SyncDomainPersistentState]]. Tries to discover existing persistent states or create new ones
  * and checks consistency of domain parameters and unique contract key domains
  */
class SyncDomainPersistentStateManager(
    participantId: ParticipantId,
    aliasResolution: DomainAliasResolution,
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

  /** Creates [[com.digitalasset.canton.participant.store.SyncDomainPersistentState]]s for all known domain aliases
    * provided that the domain parameters and a sequencer offset are known.
    * Does not check for unique contract key domain constraints.
    * Must not be called concurrently with itself or other methods of this class.
    */
  def initializePersistentStates()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    def getStaticDomainParameters(synchronizerId: SynchronizerId)(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, StaticDomainParameters] =
      EitherT
        .fromOptionF(
          DomainParameterStore(
            storage,
            synchronizerId,
            parameters.processingTimeouts,
            loggerFactory,
          ).lastParameters,
          "No domain parameters in store",
        )

    aliasResolution.aliases.toList.parTraverse_ { alias =>
      val resultE = for {
        synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
          aliasResolution.synchronizerIdForAlias(alias).toRight("Unknown synchronizer id")
        )
        synchronizerIdIndexed <- EitherT.right(
          IndexedDomain.indexed(indexedStringStore)(synchronizerId)
        )
        staticDomainParameters <- getStaticDomainParameters(synchronizerId).mapK(
          FutureUnlessShutdown.outcomeK
        )
        persistentState = createPersistentState(synchronizerIdIndexed, staticDomainParameters)
        _lastProcessedPresent <- persistentState.sequencedEventStore
          .find(SequencedEventStore.LatestUpto(CantonTimestamp.MaxValue))
          .leftMap(_ => "No persistent event")
        _ = logger.debug(s"Discovered existing state for $alias")
      } yield put(persistentState)

      resultE.valueOr(error => logger.debug(s"No state for $alias discovered: $error"))
    }
  }

  def indexedSynchronizerId(synchronizerId: SynchronizerId): FutureUnlessShutdown[IndexedDomain] =
    IndexedDomain.indexed(this.indexedStringStore)(synchronizerId)

  /** Retrieves the [[com.digitalasset.canton.participant.store.SyncDomainPersistentState]] from the [[com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager]]
    * for the given domain if there is one. Otherwise creates a new [[com.digitalasset.canton.participant.store.SyncDomainPersistentState]] for the domain
    * and registers it with the [[com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager]].
    * Checks that the [[com.digitalasset.canton.protocol.StaticDomainParameters]] are the same as what has been persisted (if so)
    * and enforces the unique contract key domain constraints.
    *
    * Must not be called concurrently with itself or other methods of this class.
    */
  def lookupOrCreatePersistentState(
      domainAlias: DomainAlias,
      synchronizerId: IndexedDomain,
      domainParameters: StaticDomainParameters,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, SyncDomainPersistentState] = {
    // TODO(#14048) does this method need to be synchronized?
    val persistentState = createPersistentState(synchronizerId, domainParameters)
    for {
      _ <- checkAndUpdateDomainParameters(
        domainAlias,
        persistentState.parameterStore,
        domainParameters,
      )
    } yield {
      // TODO(#14048) potentially delete putIfAbsent
      putIfAbsent(persistentState)
      persistentState
    }
  }

  private def createPersistentState(
      indexedDomain: IndexedDomain,
      staticDomainParameters: StaticDomainParameters,
  ): SyncDomainPersistentState =
    get(indexedDomain.synchronizerId)
      .getOrElse(mkPersistentState(indexedDomain, staticDomainParameters))

  private def checkAndUpdateDomainParameters(
      alias: DomainAlias,
      parameterStore: DomainParameterStore,
      newParameters: StaticDomainParameters,
  )(implicit traceContext: TraceContext): EitherT[Future, DomainRegistryError, Unit] =
    for {
      oldParametersO <- EitherT.right(parameterStore.lastParameters)
      _ <- oldParametersO match {
        case None =>
          // Store the parameters
          logger.debug(s"Storing domain parameters for domain $alias: $newParameters")
          EitherT.right[DomainRegistryError](parameterStore.setParameters(newParameters))
        case Some(oldParameters) =>
          EitherT.cond[Future](
            oldParameters == newParameters,
            (),
            DomainRegistryError.ConfigurationErrors.DomainParametersChanged
              .Error(oldParametersO, newParameters): DomainRegistryError,
          )
      }
    } yield ()

  def staticDomainParameters(synchronizerId: SynchronizerId): Option[StaticDomainParameters] =
    get(synchronizerId).map(_.staticDomainParameters)

  def protocolVersionFor(synchronizerId: SynchronizerId): Option[ProtocolVersion] =
    staticDomainParameters(synchronizerId).map(_.protocolVersion)

  private val domainStates: concurrent.Map[SynchronizerId, SyncDomainPersistentState] =
    TrieMap[SynchronizerId, SyncDomainPersistentState]()

  private def put(state: SyncDomainPersistentState): Unit = {
    val synchronizerId = state.indexedDomain.synchronizerId
    val previous = domainStates.putIfAbsent(synchronizerId, state)
    if (previous.isDefined)
      throw new IllegalArgumentException(s"domain state already exists for $synchronizerId")
  }

  private def putIfAbsent(state: SyncDomainPersistentState): Unit =
    domainStates.putIfAbsent(state.indexedDomain.synchronizerId, state).discard

  def get(synchronizerId: SynchronizerId): Option[SyncDomainPersistentState] =
    domainStates.get(synchronizerId)

  override def getAll: Map[SynchronizerId, SyncDomainPersistentState] = domainStates.toMap

  def getByAlias(domainAlias: DomainAlias): Option[SyncDomainPersistentState] =
    for {
      synchronizerId <- synchronizerIdForAlias(domainAlias)
      res <- get(synchronizerId)
    } yield res

  def synchronizerIdForAlias(domainAlias: DomainAlias): Option[SynchronizerId] =
    aliasResolution.synchronizerIdForAlias(domainAlias)
  def aliasForSynchronizerId(synchronizerId: SynchronizerId): Option[DomainAlias] =
    aliasResolution.aliasForSynchronizerId(synchronizerId)

  private def mkPersistentState(
      synchronizerId: IndexedDomain,
      staticDomainParameters: StaticDomainParameters,
  ): SyncDomainPersistentState = SyncDomainPersistentState
    .create(
      participantId,
      storage,
      synchronizerId,
      staticDomainParameters,
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

  def domainTopologyStateInitFor(
      synchronizerId: SynchronizerId,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Option[
    DomainTopologyInitializationCallback
  ]] =
    get(synchronizerId) match {
      case None =>
        EitherT.leftT[FutureUnlessShutdown, Option[DomainTopologyInitializationCallback]](
          DomainRegistryError.DomainRegistryInternalError.InvalidState(
            "topology factory for domain is unavailable"
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
                new StoreBasedDomainTopologyInitializationCallback(
                  participantId
                )
              )
            )
        )

    }

  override def close(): Unit =
    LifeCycle.close(domainStates.values.toSeq :+ aliasResolution: _*)(logger)
}
