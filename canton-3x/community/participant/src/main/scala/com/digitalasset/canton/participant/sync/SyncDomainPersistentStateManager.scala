// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Eval
import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.TopologyXConfig
import com.digitalasset.canton.crypto.{Crypto, CryptoPureApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.{
  DomainTopologyInitializationCallback,
  StoreBasedDomainTopologyInitializationCallback,
}
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.domain.{DomainAliasResolution, DomainRegistryError}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.topology.{
  TopologyComponentFactory,
  TopologyComponentFactoryOld,
  TopologyComponentFactoryX,
}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore, SequencedEventStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Read-only interface to the [[SyncDomainPersistentStateManager]] */
trait SyncDomainPersistentStateLookup {
  def getAll: Map[DomainId, SyncDomainPersistentState]
}

/** Manages domain state that needs to survive reconnects
  *
  * Factory for [[com.digitalasset.canton.participant.store.SyncDomainPersistentState]]. Tries to discover existing persistent states or create new ones
  * and checks consistency of domain parameters and unique contract key domains
  */
trait SyncDomainPersistentStateManager extends AutoCloseable with SyncDomainPersistentStateLookup {

  /** Creates [[com.digitalasset.canton.participant.store.SyncDomainPersistentState]]s for all known domain aliases
    * provided that the domain parameters and a sequencer offset are known.
    * Does not check for unique contract key domain constraints.
    * Must not be called concurrently with itself or other methods of this class.
    */
  def initializePersistentStates()(implicit traceContext: TraceContext): Future[Unit]

  def indexedDomainId(domainId: DomainId): Future[IndexedDomain]

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
      domainId: IndexedDomain,
      domainParameters: StaticDomainParameters,
      participantSettings: Eval[ParticipantSettingsLookup],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, SyncDomainPersistentState]

  def domainTopologyStateInitFor(
      domainId: DomainId,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, Option[DomainTopologyInitializationCallback]]

  def protocolVersionFor(
      domainId: DomainId
  ): Option[ProtocolVersion]

  def get(domainId: DomainId): Option[SyncDomainPersistentState]

  def getStatusOf(domainId: DomainId): Option[DomainConnectionConfigStore.Status]

  def getByAlias(domainAlias: DomainAlias): Option[SyncDomainPersistentState]

  def domainIdForAlias(domainAlias: DomainAlias): Option[DomainId]

  def aliasForDomainId(domainId: DomainId): Option[DomainAlias]

  def topologyFactoryFor(domainId: DomainId): Option[TopologyComponentFactory]

}

abstract class SyncDomainPersistentStateManagerImpl[S <: SyncDomainPersistentState](
    aliasResolution: DomainAliasResolution,
    storage: Storage,
    val indexedStringStore: IndexedStringStore,
    parameters: ParticipantNodeParameters,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SyncDomainPersistentStateManager
    with NamedLogging {

  override def initializePersistentStates()(implicit traceContext: TraceContext): Future[Unit] = {
    def getProtocolVersion(domainId: DomainId)(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, ProtocolVersion] =
      EitherT
        .fromOptionF(
          DomainParameterStore(
            storage,
            domainId,
            parameters.processingTimeouts,
            loggerFactory,
          ).lastParameters,
          "No domain parameters in store",
        )
        .map(_.protocolVersion)

    aliasResolution.aliases.toList.parTraverse_ { alias =>
      val resultE = for {
        domainId <- EitherT.fromEither[Future](
          aliasResolution.domainIdForAlias(alias).toRight("Unknown domain-id")
        )
        domainIdIndexed <- EitherT.right(IndexedDomain.indexed(indexedStringStore)(domainId))
        protocolVersion <- getProtocolVersion(domainId)
        persistentState = createPersistentState(alias, domainIdIndexed, protocolVersion)
        _lastProcessedPresent <- persistentState.sequencedEventStore
          .find(SequencedEventStore.LatestUpto(CantonTimestamp.MaxValue))
          .leftMap(_ => "No persistent event")
        _ = logger.debug(s"Discovered existing state for $alias")
      } yield put(persistentState)

      resultE.valueOr(error => logger.debug(s"No state for $alias discovered: $error"))
    }
  }

  override def indexedDomainId(domainId: DomainId): Future[IndexedDomain] = {
    IndexedDomain.indexed(this.indexedStringStore)(domainId)
  }

  override def lookupOrCreatePersistentState(
      domainAlias: DomainAlias,
      domainId: IndexedDomain,
      domainParameters: StaticDomainParameters,
      participantSettings: Eval[ParticipantSettingsLookup],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, SyncDomainPersistentState] = {
    // TODO(#14048) does this method need to be synchronized?
    val persistentState =
      createPersistentState(domainAlias, domainId, domainParameters.protocolVersion)
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
      alias: DomainAlias,
      domainId: IndexedDomain,
      protocolVersion: ProtocolVersion,
  ): S =
    get(domainId.item)
      .getOrElse(
        mkPersistentState(alias, domainId, protocolVersion)
      )

  protected def mkPersistentState(
      alias: DomainAlias,
      domainId: IndexedDomain,
      protocolVersion: ProtocolVersion,
  ): S

  private def checkAndUpdateDomainParameters(
      alias: DomainAlias,
      parameterStore: DomainParameterStore,
      newParameters: StaticDomainParameters,
  )(implicit traceContext: TraceContext): EitherT[Future, DomainRegistryError, Unit] = {
    for {
      oldParametersO <- EitherT.liftF(parameterStore.lastParameters)
      _ <- oldParametersO match {
        case None =>
          // Store the parameters
          logger.debug(s"Storing domain parameters for domain $alias: $newParameters")
          EitherT.liftF[Future, DomainRegistryError, Unit](
            parameterStore.setParameters(newParameters)
          )
        case Some(oldParameters) =>
          EitherT.cond[Future](
            oldParameters == newParameters,
            (),
            DomainRegistryError.ConfigurationErrors.DomainParametersChanged
              .Error(oldParametersO, newParameters): DomainRegistryError,
          )
      }
    } yield ()
  }

  def protocolVersionFor(domainId: DomainId): Option[ProtocolVersion] =
    get(domainId).map(_.protocolVersion)

  private val domainStates: concurrent.Map[DomainId, S] =
    TrieMap[DomainId, S]()

  private def put(state: S): Unit = {
    val domainId = state.domainId
    val previous = domainStates.putIfAbsent(domainId.item, state)
    if (previous.isDefined)
      throw new IllegalArgumentException(s"domain state already exists for $domainId")
  }

  private def putIfAbsent(state: S): Unit =
    domainStates.putIfAbsent(state.domainId.item, state).discard

  override def get(domainId: DomainId): Option[S] =
    domainStates.get(domainId)

  override def getAll: Map[DomainId, S] = domainStates.toMap

  override def getStatusOf(domainId: DomainId): Option[DomainConnectionConfigStore.Status] =
    this.aliasResolution.connectionStateForDomain(domainId)

  override def getByAlias(domainAlias: DomainAlias): Option[S] =
    for {
      domainId <- domainIdForAlias(domainAlias)
      res <- get(domainId)
    } yield res

  override def domainIdForAlias(domainAlias: DomainAlias): Option[DomainId] =
    aliasResolution.domainIdForAlias(domainAlias)
  override def aliasForDomainId(domainId: DomainId): Option[DomainAlias] =
    aliasResolution.aliasForDomainId(domainId)

  override def close(): Unit =
    Lifecycle.close(domainStates.values.toSeq :+ aliasResolution: _*)(logger)
}

class SyncDomainPersistentStateManagerOld(
    participantId: ParticipantId,
    aliasResolution: DomainAliasResolution,
    storage: Storage,
    indexedStringStore: IndexedStringStore,
    parameters: ParticipantNodeParameters,
    pureCrypto: CryptoPureApi,
    clock: Clock,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SyncDomainPersistentStateManagerImpl[SyncDomainPersistentStateOld](
      aliasResolution,
      storage,
      indexedStringStore,
      parameters,
      loggerFactory,
    ) {
  override protected def mkPersistentState(
      alias: DomainAlias,
      domainId: IndexedDomain,
      protocolVersion: ProtocolVersion,
  ): SyncDomainPersistentStateOld = SyncDomainPersistentState
    .createOld(
      storage,
      alias,
      domainId,
      protocolVersion,
      pureCrypto,
      parameters.stores,
      parameters.cachingConfigs,
      parameters.batchingConfig,
      parameters.processingTimeouts,
      parameters.enableAdditionalConsistencyChecks,
      indexedStringStore,
      loggerFactory,
      futureSupervisor,
    )

  override def topologyFactoryFor(domainId: DomainId): Option[TopologyComponentFactory] = {
    get(domainId).map(state =>
      new TopologyComponentFactoryOld(
        participantId,
        domainId,
        clock,
        parameters.skipTopologyManagerSignatureValidation,
        parameters.processingTimeouts,
        futureSupervisor,
        parameters.cachingConfigs,
        parameters.batchingConfig,
        state.topologyStore,
        loggerFactory,
      )
    )
  }

  def domainTopologyStateInitFor(
      domainId: DomainId,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, Option[DomainTopologyInitializationCallback]] =
    EitherT.pure(None)
}

class SyncDomainPersistentStateManagerX(
    aliasResolution: DomainAliasResolution,
    storage: Storage,
    indexedStringStore: IndexedStringStore,
    parameters: ParticipantNodeParameters,
    topologyXConfig: TopologyXConfig,
    crypto: Crypto,
    clock: Clock,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SyncDomainPersistentStateManagerImpl[SyncDomainPersistentStateX](
      aliasResolution,
      storage,
      indexedStringStore,
      parameters,
      loggerFactory,
    ) {

  override protected def mkPersistentState(
      alias: DomainAlias,
      domainId: IndexedDomain,
      protocolVersion: ProtocolVersion,
  ): SyncDomainPersistentStateX = SyncDomainPersistentState
    .createX(
      storage,
      domainId,
      protocolVersion,
      clock,
      crypto,
      parameters.stores,
      topologyXConfig,
      parameters.cachingConfigs,
      parameters.batchingConfig,
      parameters.processingTimeouts,
      parameters.enableAdditionalConsistencyChecks,
      indexedStringStore,
      loggerFactory,
      futureSupervisor,
    )

  override def topologyFactoryFor(domainId: DomainId): Option[TopologyComponentFactory] = {
    get(domainId).map(state =>
      new TopologyComponentFactoryX(
        domainId,
        crypto,
        clock,
        parameters.processingTimeouts,
        futureSupervisor,
        parameters.cachingConfigs,
        parameters.batchingConfig,
        topologyXConfig,
        state.topologyStore,
        loggerFactory.append("domainId", domainId.toString),
      )
    )
  }

  override def domainTopologyStateInitFor(
      domainId: DomainId,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, Option[DomainTopologyInitializationCallback]] = {
    get(domainId) match {
      case None =>
        EitherT.leftT[Future, Option[DomainTopologyInitializationCallback]](
          DomainRegistryError.DomainRegistryInternalError.InvalidState(
            "topology factory for domain is unavailable"
          )
        )

      case Some(state) =>
        EitherT.liftF(
          state.topologyStore
            .findFirstTrustCertificateForParticipant(participantId)
            .map(trustCert =>
              // only if the participant's trustCert is not yet in the topology store do we have to initialize it.
              // The callback will fetch the essential topology state from the sequencer
              Option.when(trustCert.isEmpty)(
                new StoreBasedDomainTopologyInitializationCallback(
                  participantId,
                  state.topologyStore,
                )
              )
            )
        )

    }
  }
}
