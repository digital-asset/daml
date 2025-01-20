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
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SyncPersistentState
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

trait ParticipantTopologyDispatcherHandle {

  /** Signal synchronizer connection such that we resume topology transaction dispatching
    *
    * When connecting / reconnecting to a synchronizer, we will first attempt to push out all
    * pending topology transactions until we have caught up with the authorized store.
    *
    * This will guarantee that all parties known on this participant are active once the synchronizer
    * is marked as ready to process transactions.
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

  /** map of active synchronizer outboxes, i.e. where we are connected and actively try to push topology state onto the synchronizers */
  private[topology] val synchronizers =
    new TrieMap[SynchronizerAlias, NonEmpty[Seq[SynchronizerOutbox]]]()

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
      synchronizerAlias: SynchronizerAlias
  )(implicit traceContext: TraceContext): Unit =
    synchronizers.remove(synchronizerAlias) match {
      case Some(outboxes) =>
        state.synchronizerIdForAlias(synchronizerAlias).foreach(disconnectOutboxes)
        outboxes.foreach(_.close())
      case None =>
        logger.debug(s"Topology pusher already disconnected from $synchronizerAlias")
    }

  def awaitIdle(synchronizerAlias: SynchronizerAlias, timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Boolean] =
    synchronizers
      .get(synchronizerAlias)
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

  private def getState(synchronizerId: SynchronizerId)(implicit
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

  def trustSynchronizer(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    def alreadyTrustedInStore(
        store: TopologyStore[?]
    ): EitherT[FutureUnlessShutdown, String, Boolean] =
      for {
        alreadyTrusted <- EitherT
          .right[String](
            performUnlessClosingUSF(functionFullName)(
              store
                .findPositiveTransactions(
                  asOf = CantonTimestamp.MaxValue,
                  asOfInclusive = true,
                  isProposal = false,
                  types = Seq(SynchronizerTrustCertificate.code),
                  filterUid = Some(Seq(participantId.uid)),
                  filterNamespace = None,
                )
                .map(_.toTopologyState.exists {
                  case SynchronizerTrustCertificate(`participantId`, `synchronizerId`) => true
                  case _ => false
                })
            )
          )
      } yield alreadyTrusted
    def trustDomain(
        state: SyncPersistentState
    ): EitherT[FutureUnlessShutdown, String, Unit] =
      performUnlessClosingEitherUSF(functionFullName) {
        MonadUtil.unlessM(alreadyTrustedInStore(manager.store)) {
          manager
            .proposeAndAuthorize(
              TopologyChangeOp.Replace,
              SynchronizerTrustCertificate(
                participantId,
                synchronizerId,
              ),
              serial = None,
              signingKeys = Seq.empty,
              protocolVersion = state.staticSynchronizerParameters.protocolVersion,
              expectFullAuthorization = true,
            )
            // TODO(#14048) improve error handling
            .leftMap(_.cause)
        }
      }
    // check if cert already exists in the synchronizer store
    val ret = for {
      state <- getState(synchronizerId).leftMap(_.cause)
      alreadyTrustedInDomainStore <- alreadyTrustedInStore(state.topologyStore)
      _ <-
        if (alreadyTrustedInDomainStore) EitherT.rightT[FutureUnlessShutdown, String](())
        else trustDomain(state)
    } yield ()
    ret
  }

  def onboardToSynchronizer(
      synchronizerId: SynchronizerId,
      alias: SynchronizerAlias,
      sequencerConnectClient: SequencerConnectClient,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContextExecutor,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Boolean] =
    getState(synchronizerId).flatMap { _ =>
      SynchronizerOnboardingOutbox
        .initiateOnboarding(
          alias,
          synchronizerId,
          protocolVersion,
          participantId,
          sequencerConnectClient,
          manager.store,
          timeouts,
          loggerFactory
            .append("synchronizerId", synchronizerId.toString)
            .appendUnnamedKey("onboarding", "onboarding"),
          crypto,
        )
    }

  def createHandler(
      synchronizerAlias: SynchronizerAlias,
      synchronizerId: SynchronizerId,
      protocolVersion: ProtocolVersion,
      client: SynchronizerTopologyClientWithInit,
      sequencerClient: SequencerClient,
  ): ParticipantTopologyDispatcherHandle = {
    val synchronizerLoggerFactory = loggerFactory.append("synchronizerId", synchronizerId.toString)
    new ParticipantTopologyDispatcherHandle {
      val handle = new SequencerBasedRegisterTopologyTransactionHandle(
        sequencerClient,
        synchronizerId,
        participantId,
        clock,
        config.topology,
        protocolVersion,
        timeouts,
        synchronizerLoggerFactory,
      )

      override def synchronizerConnected()(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Unit] =
        getState(synchronizerId)
          .flatMap { state =>
            val queueBasedSynchronizerOutbox = new QueueBasedSynchronizerOutbox(
              synchronizerAlias = synchronizerAlias,
              synchronizerId = synchronizerId,
              memberId = participantId,
              protocolVersion = protocolVersion,
              handle = handle,
              targetClient = client,
              synchronizerOutboxQueue = state.synchronizerOutboxQueue,
              targetStore = state.topologyStore,
              timeouts = timeouts,
              loggerFactory = synchronizerLoggerFactory,
              crypto = crypto,
              broadcastBatchSize = topologyConfig.broadcastBatchSize,
            )

            val storeBasedSynchronizerOutbox = new StoreBasedSynchronizerOutbox(
              synchronizerAlias = synchronizerAlias,
              synchronizerId = synchronizerId,
              memberId = participantId,
              protocolVersion = protocolVersion,
              handle = handle,
              targetClient = client,
              authorizedStore = manager.store,
              targetStore = state.topologyStore,
              timeouts = timeouts,
              loggerFactory = loggerFactory,
              crypto = crypto,
              broadcastBatchSize = topologyConfig.broadcastBatchSize,
              futureSupervisor = futureSupervisor,
            )
            ErrorUtil.requireState(
              !synchronizers.contains(synchronizerAlias),
              s"topology pusher for $synchronizerAlias already exists",
            )
            val outboxes = NonEmpty(Seq, queueBasedSynchronizerOutbox, storeBasedSynchronizerOutbox)
            synchronizers += synchronizerAlias -> outboxes

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
                SynchronizerRegistryError.InitialOnboardingError.Error(_)
              )
            )
          }
    }
  }

  private def disconnectOutboxes(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug("Clearing synchronizer topology manager observers")
    state.get(synchronizerId).foreach(_.topologyManager.clearObservers())
  }

}

/** Utility class to dispatch the initial set of onboarding transactions to a synchronizer
  *
  * Generally, when we onboard to a new synchronizer, we only want to onboard with the minimal set of
  * topology transactions that are required to join a synchronizer. Otherwise, if we e.g. have
  * registered one million parties and then subsequently roll a key, we'd send an enormous
  * amount of unnecessary topology transactions.
  */
private class SynchronizerOnboardingOutbox(
    synchronizerAlias: SynchronizerAlias,
    val synchronizerId: SynchronizerId,
    val protocolVersion: ProtocolVersion,
    participantId: ParticipantId,
    sequencerConnectClient: SequencerConnectClient,
    val authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    override protected val crypto: Crypto,
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
        performUnlessClosingUSF(functionFullName)(
          authorizedStore
            .findParticipantOnboardingTransactions(participantId, synchronizerId)
        )
      )
      applicable <- EitherT.right(
        performUnlessClosingUSF(functionFullName)(onlyApplicable(candidates))
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](initializedWith(applicable))
      // Try to convert if necessary the topology transactions for the required protocol version of the synchronizer
      convertedTxs <- performUnlessClosingEitherUSF(functionFullName) {
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
      synchronizerId: SynchronizerId,
      protocolVersion: ProtocolVersion,
      participantId: ParticipantId,
      sequencerConnectClient: SequencerConnectClient,
      authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      crypto: Crypto,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SynchronizerRegistryError, Boolean] = {
    val outbox = new SynchronizerOnboardingOutbox(
      synchronizerAlias,
      synchronizerId,
      protocolVersion,
      participantId,
      sequencerConnectClient,
      authorizedStore,
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
