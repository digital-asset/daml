// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.domain.{
  RegisterTopologyTransactionHandleCommon,
  SequencerBasedRegisterTopologyTransactionHandle,
}
import com.digitalasset.canton.config.{DomainTimeTrackerConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.{
  DomainRegistryError,
  ParticipantInitializeTopology,
}
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManagerImpl
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  RegisterTopologyTransactionResponseResult,
}
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SequencerClient,
  SequencerClientFactory,
}
import com.digitalasset.canton.sequencing.protocol.Batch
import com.digitalasset.canton.sequencing.{EnvelopeHandler, SequencerConnections}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, SequencerAlias}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

trait TopologyDispatcherCommon extends NamedLogging with FlagCloseable

trait ParticipantTopologyDispatcherHandle {

  /** Signal domain connection such that we resume topology transaction dispatching
    *
    * When connecting / reconnecting to a domain, we will first attempt to push out all
    * pending topology transactions until we have caught up with the authorized store.
    *
    * This will guarantee that all parties known on this participant are active once the domain
    * is marked as ready to process transactions.
    */
  def domainConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit]

  def processor: EnvelopeHandler

}

trait ParticipantTopologyDispatcherCommon extends TopologyDispatcherCommon {

  def trustDomain(domainId: DomainId, parameters: StaticDomainParameters)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]
  def onboardToDomain(
      domainId: DomainId,
      alias: DomainAlias,
      timeTrackerConfig: DomainTimeTrackerConfig,
      sequencerConnection: SequencerConnections,
      sequencerClientFactory: SequencerClientFactory,
      protocolVersion: ProtocolVersion,
      expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionServiceFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean]

  def awaitIdle(domain: DomainAlias, timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean]

  def domainDisconnected(domain: DomainAlias)(implicit traceContext: TraceContext): Unit

  def createHandler(
      domain: DomainAlias,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      client: DomainTopologyClientWithInit,
      sequencerClient: SequencerClient,
  ): ParticipantTopologyDispatcherHandle

  def queueStatus: TopologyQueueStatus

}

abstract class ParticipantTopologyDispatcherImplCommon[S <: SyncDomainPersistentState](
    state: SyncDomainPersistentStateManagerImpl[S]
)(implicit
    val ec: ExecutionContext
) extends ParticipantTopologyDispatcherCommon {

  /** map of active domain outboxes, i.e. where we are connected and actively try to push topology state onto the domains */
  private[topology] val domains = new TrieMap[DomainAlias, NonEmpty[Seq[DomainOutboxCommon]]]()

  def queueStatus: TopologyQueueStatus = {
    val (dispatcher, clients) = domains.values.foldLeft((0, 0)) { case ((disp, clts), outbox) =>
      (disp + outbox.map(_.queueSize).sum, clts + outbox.map(_.targetClient.numPendingChanges).sum)
    }
    TopologyQueueStatus(
      manager = managerQueueSize,
      dispatcher = dispatcher,
      clients = clients,
    )
  }
  protected def managerQueueSize: Int

  override def domainDisconnected(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): Unit =
    domains.remove(domain) match {
      case Some(outbox) =>
        outbox.foreach(_.close())
      case None =>
        logger.debug(s"Topology pusher already disconnected from $domain")
    }

  override def awaitIdle(domain: DomainAlias, timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] =
    domains
      .get(domain)
      .fold(
        EitherT.leftT[FutureUnlessShutdown, Boolean](
          DomainRegistryError.DomainRegistryInternalError
            .InvalidState(
              "Can not await idle without the domain being connected"
            ): DomainRegistryError
        )
      )(x =>
        EitherT.right[DomainRegistryError](
          x.forgetNE.parTraverse(_.awaitIdle(timeout)).map(_.forall(identity))
        )
      )

  protected def getState(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, S] =
    EitherT
      .fromEither[FutureUnlessShutdown](
        state
          .get(domainId)
          .toRight(
            DomainRegistryError.DomainRegistryInternalError
              .InvalidState("No persistent state for domain")
          )
      )

}

/** Identity dispatcher, registering identities with a domain
  *
  * The dispatcher observes the participant topology manager and tries to shovel
  * new topology transactions added to the manager to all connected domains.
  */
class ParticipantTopologyDispatcher(
    val manager: ParticipantTopologyManager,
    participantId: ParticipantId,
    state: SyncDomainPersistentStateManagerImpl[SyncDomainPersistentState],
    crypto: Crypto,
    clock: Clock,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ParticipantTopologyDispatcherImplCommon[SyncDomainPersistentState](state) {

  override protected def managerQueueSize: Int = manager.queueSize

  // connect to manager
  manager.addObserver(new ParticipantTopologyManagerObserver {
    override def addedNewTransactions(
        timestamp: CantonTimestamp,
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
      val num = transactions.size
      domains.values.toList
        .flatMap(_.forgetNE)
        .parTraverse(_.newTransactionsAddedToAuthorizedStore(timestamp, num))
        .map(_ => ())
    }
  })

  override def createHandler(
      domain: DomainAlias,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      client: DomainTopologyClientWithInit,
      sequencerClient: SequencerClient,
  ): ParticipantTopologyDispatcherHandle = new ParticipantTopologyDispatcherHandle {

    val handle = new SequencerBasedRegisterTopologyTransactionHandle(
      new SequencerBasedRegisterTopologyTransactionHandle.SenderImpl(clock, protocolVersion) {
        override protected def sendInternal(
            batch: Batch[DefaultOpenEnvelope],
            maxSequencingTime: CantonTimestamp,
            callback: SendCallback,
        )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
          sequencerClient.sendAsync(
            batch = batch,
            maxSequencingTime = maxSequencingTime,
            callback = callback,
          )
      },
      domainId,
      participantId,
      participantId,
      protocolVersion,
      timeouts,
      loggerFactory.append("domainId", domainId.toString),
    )

    override def domainConnected()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit] =
      getState(domainId)
        .flatMap { state =>
          val outbox = new StoreBasedDomainOutbox(
            domain,
            domainId,
            participantId,
            protocolVersion,
            handle,
            client,
            manager.store,
            state.topologyStore,
            timeouts,
            loggerFactory.append("domainId", domainId.toString),
            crypto,
          )
          ErrorUtil.requireState(
            !domains.contains(domain),
            s"topology pusher for $domain already exists",
          )
          domains += domain -> NonEmpty(Seq, outbox)
          outbox
            .startup()
            .leftMap(DomainRegistryError.DomainRegistryInternalError.InitialOnboardingError(_))
        }

    override def processor: EnvelopeHandler = handle.processor

  }

  override def trustDomain(domainId: DomainId, parameters: StaticDomainParameters)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    manager.issueParticipantDomainStateCert(participantId, domainId, parameters.protocolVersion)

  override def onboardToDomain(
      domainId: DomainId,
      alias: DomainAlias,
      timeTrackerConfig: DomainTimeTrackerConfig,
      sequencerConnection: SequencerConnections,
      sequencerClientFactory: SequencerClientFactory,
      protocolVersion: ProtocolVersion,
      expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionServiceFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] =
    getState(domainId).flatMap { state =>
      (new ParticipantInitializeTopology(
        domainId,
        alias,
        participantId,
        manager.store,
        state.topologyStore,
        clock,
        timeTrackerConfig,
        timeouts,
        loggerFactory.append("domainId", domainId.toString),
        sequencerClientFactory,
        sequencerConnection,
        crypto,
        protocolVersion,
        expectedSequencers,
      )).run()
    }
}

/** Utility class to dispatch the initial set of onboarding transactions to a domain
  *
  * Generally, when we onboard to a new domain, we only want to onboard with the minimal set of
  * topology transactions that are required to join a domain. Otherwise, if we e.g. have
  * registered one million parties and then subsequently roll a key, we'd send an enormous
  * amount of unnecessary topology transactions.
  */
private class DomainOnboardingOutbox(
    domain: DomainAlias,
    val domainId: DomainId,
    val protocolVersion: ProtocolVersion,
    participantId: ParticipantId,
    val handle: RegisterTopologyTransactionHandleCommon[
      GenericSignedTopologyTransaction,
      RegisterTopologyTransactionResponseResult.State,
    ],
    val authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
    val targetStore: TopologyStore[TopologyStoreId.DomainStore],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    override protected val crypto: Crypto,
) extends DomainOutboxDispatch[
      GenericSignedTopologyTransaction,
      RegisterTopologyTransactionResponseResult.State,
      RegisterTopologyTransactionHandleCommon[
        GenericSignedTopologyTransaction,
        RegisterTopologyTransactionResponseResult.State,
      ],
      TopologyStore[
        TopologyStoreId.DomainStore
      ],
    ]
    with DomainOutboxDispatchHelperOld {

  override val memberId: Member = participantId
  private def run()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = (for {
    initialTransactions <- loadInitialTransactionsFromStore()
    _ = logger.debug(
      s"Sending ${initialTransactions.size} onboarding transactions to $domain"
    )
    result <- dispatch(domain, initialTransactions).leftMap[DomainRegistryError](
      DomainRegistryError.DomainRegistryInternalError.InitialOnboardingError(_)
    )
  } yield {
    result.forall(res => isExpectedState(res))
  }).thereafter { _ =>
    close()
  }

  private def loadInitialTransactionsFromStore()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Seq[GenericSignedTopologyTransaction]] =
    for {
      candidates <- EitherT.right(
        performUnlessClosingUSF(functionFullName)(
          authorizedStore
            .findParticipantOnboardingTransactions(participantId, domainId)
        )
      )
      applicablePossiblyPresent <- EitherT.right(
        performUnlessClosingF(functionFullName)(onlyApplicable(candidates))
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](initializedWith(applicablePossiblyPresent))
      applicable <- EitherT.right(
        performUnlessClosingF(functionFullName)(notAlreadyPresent(applicablePossiblyPresent))
      )
      // Try to convert if necessary the topology transactions for the required protocol version of the domain
      convertedTxs <- performUnlessClosingEitherU(functionFullName) {
        convertTransactions(applicable).leftMap[DomainRegistryError](
          DomainRegistryError.TopologyConversionError.Error(_)
        )
      }
    } yield convertedTxs

  private def initializedWith(
      initial: Seq[GenericSignedTopologyTransaction]
  )(implicit traceContext: TraceContext): Either[DomainRegistryError, Unit] = {
    val (haveEncryptionKey, haveSigningKey) =
      initial.map(_.transaction.element.mapping).foldLeft((false, false)) {
        case ((haveEncryptionKey, haveSigningKey), OwnerToKeyMapping(`memberId`, key)) =>
          (haveEncryptionKey || !key.isSigning, haveSigningKey || key.isSigning)
        case (acc, _) => acc
      }
    if (!haveEncryptionKey) {
      Left(
        DomainRegistryError.DomainRegistryInternalError.InitialOnboardingError(
          "Can not onboard as local participant doesn't have a valid encryption key"
        )
      )
    } else if (!haveSigningKey) {
      Left(
        DomainRegistryError.DomainRegistryInternalError.InitialOnboardingError(
          "Can not onboard as local participant doesn't have a valid signing key"
        )
      )
    } else Right(())
  }

}

object DomainOnboardingOutbox {
  def initiateOnboarding(
      domain: DomainAlias,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      participantId: ParticipantId,
      handle: RegisterTopologyTransactionHandleCommon[
        GenericSignedTopologyTransaction,
        RegisterTopologyTransactionResponseResult.State,
      ],
      authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
      targetStore: TopologyStore[TopologyStoreId.DomainStore],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      crypto: Crypto,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
    val outbox = new DomainOnboardingOutbox(
      domain,
      domainId,
      protocolVersion,
      participantId,
      handle,
      authorizedStore,
      targetStore,
      timeouts,
      loggerFactory.append("domainId", domainId.toString),
      crypto,
    )
    outbox.run().transform { res =>
      outbox.close()
      res
    }
  }
}
