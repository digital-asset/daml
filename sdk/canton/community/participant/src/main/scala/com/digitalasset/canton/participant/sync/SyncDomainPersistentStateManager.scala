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
import com.digitalasset.canton.lifecycle.Lifecycle
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
class SyncDomainPersistentStateManager(
    participantId: ParticipantId,
    aliasResolution: DomainAliasResolution,
    storage: Storage,
    val indexedStringStore: IndexedStringStore,
    parameters: ParticipantNodeParameters,
    crypto: Crypto,
    clock: Clock,
    packageDependencyResolver: PackageDependencyResolver,
    ledgerApiStore: Eval[LedgerApiStore],
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
  def initializePersistentStates()(implicit traceContext: TraceContext): Future[Unit] = {
    def getStaticDomainParameters(domainId: DomainId)(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, StaticDomainParameters] =
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

    aliasResolution.aliases.toList.parTraverse_ { alias =>
      val resultE = for {
        domainId <- EitherT.fromEither[Future](
          aliasResolution.domainIdForAlias(alias).toRight("Unknown domain-id")
        )
        domainIdIndexed <- EitherT.right(IndexedDomain.indexed(indexedStringStore)(domainId))
        staticDomainParameters <- getStaticDomainParameters(domainId)
        persistentState = createPersistentState(domainIdIndexed, staticDomainParameters)
        _lastProcessedPresent <- persistentState.sequencedEventStore
          .find(SequencedEventStore.LatestUpto(CantonTimestamp.MaxValue))
          .leftMap(_ => "No persistent event")
        _ = logger.debug(s"Discovered existing state for $alias")
      } yield put(persistentState)

      resultE.valueOr(error => logger.debug(s"No state for $alias discovered: $error"))
    }
  }

  def indexedDomainId(domainId: DomainId): Future[IndexedDomain] =
    IndexedDomain.indexed(this.indexedStringStore)(domainId)

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
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, SyncDomainPersistentState] = {
    // TODO(#14048) does this method need to be synchronized?
    val persistentState = createPersistentState(domainId, domainParameters)
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
    get(indexedDomain.domainId)
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

  def staticDomainParameters(domainId: DomainId): Option[StaticDomainParameters] =
    get(domainId).map(_.staticDomainParameters)

  def protocolVersionFor(domainId: DomainId): Option[ProtocolVersion] =
    staticDomainParameters(domainId).map(_.protocolVersion)

  private val domainStates: concurrent.Map[DomainId, SyncDomainPersistentState] =
    TrieMap[DomainId, SyncDomainPersistentState]()

  private def put(state: SyncDomainPersistentState): Unit = {
    val domainId = state.indexedDomain.domainId
    val previous = domainStates.putIfAbsent(domainId, state)
    if (previous.isDefined)
      throw new IllegalArgumentException(s"domain state already exists for $domainId")
  }

  private def putIfAbsent(state: SyncDomainPersistentState): Unit =
    domainStates.putIfAbsent(state.indexedDomain.domainId, state).discard

  def get(domainId: DomainId): Option[SyncDomainPersistentState] =
    domainStates.get(domainId)

  override def getAll: Map[DomainId, SyncDomainPersistentState] = domainStates.toMap

  def getByAlias(domainAlias: DomainAlias): Option[SyncDomainPersistentState] =
    for {
      domainId <- domainIdForAlias(domainAlias)
      res <- get(domainId)
    } yield res

  def domainIdForAlias(domainAlias: DomainAlias): Option[DomainId] =
    aliasResolution.domainIdForAlias(domainAlias)
  def aliasForDomainId(domainId: DomainId): Option[DomainAlias] =
    aliasResolution.aliasForDomainId(domainId)

  private def mkPersistentState(
      domainId: IndexedDomain,
      staticDomainParameters: StaticDomainParameters,
  ): SyncDomainPersistentState = SyncDomainPersistentState
    .create(
      participantId,
      storage,
      domainId,
      staticDomainParameters,
      clock,
      crypto,
      parameters,
      indexedStringStore,
      packageDependencyResolver,
      ledgerApiStore,
      loggerFactory,
      futureSupervisor,
    )

  def topologyFactoryFor(domainId: DomainId): Option[TopologyComponentFactory] =
    get(domainId).map(state =>
      new TopologyComponentFactory(
        domainId,
        crypto,
        clock,
        parameters.processingTimeouts,
        futureSupervisor,
        parameters.cachingConfigs,
        parameters.batchingConfig,
        exitOnFatalFailures = parameters.exitOnFatalFailures,
        state.topologyStore,
        loggerFactory.append("domainId", domainId.toString),
      )
    )

  def domainTopologyStateInitFor(
      domainId: DomainId,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DomainRegistryError, Option[DomainTopologyInitializationCallback]] =
    get(domainId) match {
      case None =>
        EitherT.leftT[Future, Option[DomainTopologyInitializationCallback]](
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
                  participantId,
                  state.topologyStore,
                )
              )
            )
        )

    }

  override def close(): Unit =
    Lifecycle.close(domainStates.values.toSeq :+ aliasResolution: _*)(logger)
}
