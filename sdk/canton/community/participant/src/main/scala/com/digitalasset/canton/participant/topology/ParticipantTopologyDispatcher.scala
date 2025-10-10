// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.common.sequencer.{
  SequencerBasedRegisterTopologyTransactionHandle,
  SequencerConnectClient,
}
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.{LocalNodeConfig, ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.crypto.{Crypto, SynchronizerCrypto}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SyncPersistentState
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.version.ParticipantProtocolFeatureFlags

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

trait ParticipantTopologyDispatcherHandle {

  /** Signal synchronizer connection such that we resume topology transaction dispatching
    *
    * When connecting / reconnecting to a synchronizer, we will first attempt to push out all
    * pending topology transactions until we have caught up with the authorized store.
    *
    * This will guarantee that all parties known on this participant are active once the
    * synchronizer is marked as ready to process transactions.
    */
  def synchronizerConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Unit]

}

// Not final because of testing / mocking
class ParticipantTopologyDispatcher(
    val manager: AuthorizedTopologyManager,
    participantId: ParticipantId,
    state: SyncPersistentStateManager,
    topologyConfig: TopologyConfig,
    crypto: Crypto,
    clock: Clock,
    config: LocalNodeConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit override protected val executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasFutureSupervision {

  /** map of active synchronizer outboxes, i.e. where we are connected and actively try to push
    * topology state onto the synchronizers
    */
  private[topology] val synchronizers =
    new TrieMap[PhysicalSynchronizerId, NonEmpty[Seq[SynchronizerOutbox]]]()

  def queueStatus: TopologyQueueStatus = {
    val (dispatcher, clients) = synchronizers.values.foldLeft((0, 0)) {
      case ((disp, clts), outbox) =>
        (
          disp + outbox.map(_.queueSize).sum,
          clts + outbox.map(_.targetClient.numPendingChanges).sum,
        )
    }
    TopologyQueueStatus(
      manager = managerQueueSize,
      dispatcher = dispatcher,
      clients = clients,
    )
  }

  def synchronizerDisconnected(
      synchronizerId: PhysicalSynchronizerId
  )(implicit traceContext: TraceContext): Unit =
    synchronizers.remove(synchronizerId) match {
      case Some(outboxes) =>
        disconnectOutboxes(synchronizerId)
        outboxes.foreach(_.close())
      case None =>
        logger.debug(s"Topology pusher already disconnected from $synchronizerId")
    }

  def awaitIdle(synchronizerId: PhysicalSynchronizerId, timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Boolean] =
    synchronizers
      .get(synchronizerId)
      .fold(
        EitherT.leftT[FutureUnlessShutdown, Boolean](
          SynchronizerRegistryError.SynchronizerRegistryInternalError
            .InvalidState(
              "Can not await idle without the synchronizer being connected"
            ): SynchronizerRegistryError
        )
      )(x =>
        EitherT.right[SynchronizerRegistryError](
          x.forgetNE.parTraverse(_.awaitIdle(timeout)).map(_.forall(identity))
        )
      )

  private def getState(synchronizerId: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, SyncPersistentState] =
    EitherT
      .fromEither[FutureUnlessShutdown](
        state
          .get(synchronizerId)
          .toRight(
            SynchronizerRegistryError.SynchronizerRegistryInternalError
              .InvalidState("No persistent state for synchronizer")
          )
      )

  private def managerQueueSize: Int =
    manager.queueSize + state.getAll.values.map(_.topologyManager.queueSize).sum

  // connect to manager
  manager.addObserver(new TopologyManagerObserver {
    override def addedNewTransactions(
        timestamp: CantonTimestamp,
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
      val num = transactions.size
      synchronizers.values.toList
        .flatMap(_.forgetNE)
        .collect { case outbox: StoreBasedSynchronizerOutbox => outbox }
        .parTraverse(_.newTransactionsAdded(timestamp, num))
        .map(_ => ())
    }
  })

  def trustSynchronizer(synchronizerId: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Unit] = {
    val logicalSynchronizerId = synchronizerId.logical
    val featureFlagsForPV = ParticipantProtocolFeatureFlags.supportedFeatureFlagsByPV.getOrElse(
      synchronizerId.protocolVersion,
      Set.empty,
    )

    def alreadyTrustedInStoreWithSupportedFeatures(
        store: TopologyStore[?]
    ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Boolean] =
      EitherT.right(
        synchronizeWithClosing(functionFullName)(
          store
            .findPositiveTransactions(
              asOf = CantonTimestamp.MaxValue,
              asOfInclusive = true,
              isProposal = false,
              types = Seq(SynchronizerTrustCertificate.code),
              filterUid = Some(NonEmpty(Seq, participantId.uid)),
              filterNamespace = None,
            )
            .map(_.toTopologyState.exists {
              // If certificate is missing feature flags, re-issue the trust certificate with it
              case SynchronizerTrustCertificate(
                    `participantId`,
                    `logicalSynchronizerId`,
                    featureFlags,
                  ) =>
                featureFlagsForPV.diff(featureFlags.toSet).isEmpty
              case _ => false
            })
        )
      )

    def trustSynchronizer(
        state: SyncPersistentState
    ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Unit] =
      MonadUtil.unlessM(
        alreadyTrustedInStoreWithSupportedFeatures(manager.store)
      ) {
        synchronizeWithClosing(functionFullName) {
          manager
            .proposeAndAuthorize(
              TopologyChangeOp.Replace,
              SynchronizerTrustCertificate(
                participantId,
                logicalSynchronizerId,
                featureFlagsForPV.toSeq,
              ),
              serial = None,
              signingKeys = Seq.empty,
              protocolVersion = state.staticSynchronizerParameters.protocolVersion,
              expectFullAuthorization = true,
              waitToBecomeEffective = None,
            )
            .bimap(
              SynchronizerRegistryError.ConfigurationErrors.CanNotIssueSynchronizerTrustCertificate
                .Error(_),
              _ => (),
            )
        }
      }

    // check if cert already exists in the synchronizer store
    getState(synchronizerId).flatMap(state =>
      MonadUtil.unlessM(
        alreadyTrustedInStoreWithSupportedFeatures(state.topologyStore)
      )(
        trustSynchronizer(state)
      )
    )
  }

  def onboardToSynchronizer(
      psid: PhysicalSynchronizerId,
      alias: SynchronizerAlias,
      sequencerConnectClient: SequencerConnectClient,
  )(implicit
      executionContext: ExecutionContextExecutor,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Boolean] =
    getState(psid).flatMap { state =>
      SynchronizerOnboardingOutbox
        .initiateOnboarding(
          alias,
          psid,
          participantId,
          sequencerConnectClient,
          manager.store,
          topologyConfig,
          timeouts,
          loggerFactory
            .append("synchronizerId", psid.toString)
            .appendUnnamedKey("onboarding", "onboarding"),
          SynchronizerCrypto(crypto, state.staticSynchronizerParameters),
        )
    }

  def createHandler(
      synchronizerAlias: SynchronizerAlias,
      client: SynchronizerTopologyClientWithInit,
      sequencerClient: SequencerClient,
      timeTracker: SynchronizerTimeTracker,
  ): ParticipantTopologyDispatcherHandle = {
    val synchronizerLoggerFactory =
      loggerFactory.append("synchronizerId", sequencerClient.psid.toString)
    new ParticipantTopologyDispatcherHandle {
      val handle = new SequencerBasedRegisterTopologyTransactionHandle(
        sequencerClient,
        participantId,
        timeTracker,
        clock,
        config.topology,
        timeouts,
        synchronizerLoggerFactory,
      )

      override def synchronizerConnected()(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Unit] =
        getState(sequencerClient.psid)
          .flatMap { state =>
            val queueBasedSynchronizerOutbox = new QueueBasedSynchronizerOutbox(
              synchronizerAlias = synchronizerAlias,
              memberId = participantId,
              handle = handle,
              targetClient = client,
              synchronizerOutboxQueue = state.synchronizerOutboxQueue,
              targetStore = state.topologyStore,
              timeouts = timeouts,
              loggerFactory = synchronizerLoggerFactory,
              crypto = SynchronizerCrypto(crypto, state.staticSynchronizerParameters),
              topologyConfig = topologyConfig,
            )

            val storeBasedSynchronizerOutbox = new StoreBasedSynchronizerOutbox(
              synchronizerAlias = synchronizerAlias,
              memberId = participantId,
              handle = handle,
              targetClient = client,
              authorizedStore = manager.store,
              targetStore = state.topologyStore,
              timeouts = timeouts,
              loggerFactory = loggerFactory,
              crypto = SynchronizerCrypto(crypto, state.staticSynchronizerParameters),
              topologyConfig = topologyConfig,
              futureSupervisor = futureSupervisor,
            )
            val psid = client.psid
            ErrorUtil.requireState(
              !synchronizers.contains(psid),
              s"topology pusher for $psid already exists",
            )
            val outboxes = NonEmpty(Seq, queueBasedSynchronizerOutbox, storeBasedSynchronizerOutbox)
            synchronizers += psid -> outboxes

            state.topologyManager.addObserver(new TopologyManagerObserver {
              override def addedNewTransactions(
                  timestamp: CantonTimestamp,
                  transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
              )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
                queueBasedSynchronizerOutbox.newTransactionsAdded(
                  timestamp,
                  transactions.size,
                )
            })

            outboxes.forgetNE.parTraverse_(
              _.startup().leftMap(
                SynchronizerRegistryError.InitialOnboardingError.Error(_): SynchronizerRegistryError
              )
            )
          }
    }
  }

  private def disconnectOutboxes(synchronizerId: PhysicalSynchronizerId)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug("Clearing synchronizer topology manager observers")
    state.get(synchronizerId).foreach(_.topologyManager.clearObservers())
  }
}

/** Utility class to dispatch the initial set of onboarding transactions to a synchronizer
  *
  * Generally, when we onboard to a new synchronizer, we only want to onboard with the minimal set
  * of topology transactions that are required to join a synchronizer. Otherwise, if we e.g. have
  * registered one million parties and then subsequently roll a key, we'd send an enormous amount of
  * unnecessary topology transactions.
  */
private class SynchronizerOnboardingOutbox(
    synchronizerAlias: SynchronizerAlias,
    val psid: PhysicalSynchronizerId,
    participantId: ParticipantId,
    sequencerConnectClient: SequencerConnectClient,
    val authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
    override protected val topologyConfig: TopologyConfig,
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    override protected val crypto: SynchronizerCrypto,
) extends StoreBasedSynchronizerOutboxDispatchHelper
    with FlagCloseable {

  override protected val memberId: Member = participantId

  private def run()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Boolean] = (for {
    initialTransactions <- loadInitialTransactionsFromStore()
    _ = logger.debug(
      s"Sending ${initialTransactions.size} onboarding transactions to $synchronizerAlias"
    )
    _result <- dispatch(initialTransactions)
      .leftMap(err =>
        SynchronizerRegistryError.InitialOnboardingError.Error(
          err.toString
        ): SynchronizerRegistryError
      )
  } yield true).thereafter { _ =>
    close()
  }

  private def loadInitialTransactionsFromStore()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Seq[
    GenericSignedTopologyTransaction
  ]] =
    for {
      candidates <- EitherT.right(
        synchronizeWithClosing(functionFullName)(
          authorizedStore
            .findParticipantOnboardingTransactions(participantId, psid.logical)
        )
      )
      applicable <- EitherT.right(
        synchronizeWithClosing(functionFullName)(onlyApplicable(candidates))
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](initializedWith(applicable))
      // Try to convert if necessary the topology transactions for the required protocol version of the synchronizer
      convertedTxs <- synchronizeWithClosing(functionFullName) {
        convertTransactions(applicable).leftMap[SynchronizerRegistryError](
          SynchronizerRegistryError.TopologyConversionError.Error(_)
        )
      }
    } yield convertedTxs

  private def dispatch(transactions: Seq[GenericSignedTopologyTransaction])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectClient.Error, Unit] =
    sequencerConnectClient.registerOnboardingTopologyTransactions(
      synchronizerAlias,
      participantId,
      transactions,
    )

  private def initializedWith(
      initial: Seq[GenericSignedTopologyTransaction]
  )(implicit traceContext: TraceContext): Either[SynchronizerRegistryError, Unit] = {
    val (haveEncryptionKey, haveSigningKey) =
      initial.map(_.mapping).foldLeft((false, false)) {
        case ((haveEncryptionKey, haveSigningKey), OwnerToKeyMapping(`participantId`, keys)) =>
          (
            haveEncryptionKey || keys.exists(!_.isSigning),
            haveSigningKey || keys.exists(_.isSigning),
          )
        case (acc, _) => acc
      }
    if (!haveEncryptionKey) {
      Left(
        SynchronizerRegistryError.InitialOnboardingError.Error(
          "Can not onboard as local participant doesn't have a valid encryption key"
        )
      )
    } else if (!haveSigningKey) {
      Left(
        SynchronizerRegistryError.InitialOnboardingError.Error(
          "Can not onboard as local participant doesn't have a valid signing key"
        )
      )
    } else Either.unit
  }

}

object SynchronizerOnboardingOutbox {
  def initiateOnboarding(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: PhysicalSynchronizerId,
      participantId: ParticipantId,
      sequencerConnectClient: SequencerConnectClient,
      authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
      topologyConfig: TopologyConfig,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      crypto: SynchronizerCrypto,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Boolean] = {
    val outbox = new SynchronizerOnboardingOutbox(
      synchronizerAlias,
      synchronizerId,
      participantId,
      sequencerConnectClient,
      authorizedStore,
      topologyConfig,
      timeouts,
      loggerFactory,
      crypto,
    )
    outbox.run().transform { res =>
      outbox.close()
      res
    }
  }
}
