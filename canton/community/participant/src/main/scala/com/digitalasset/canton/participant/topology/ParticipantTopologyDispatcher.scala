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
  RegisterTopologyTransactionHandle,
  SequencerBasedRegisterTopologyTransactionHandleX,
}
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.{DomainTimeTrackerConfig, LocalNodeConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.{
  DomainRegistryError,
  ParticipantInitializeTopologyX,
}
import com.digitalasset.canton.participant.store.SyncDomainPersistentState
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManagerImpl
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.{SequencerClient, SequencerClientFactory}
import com.digitalasset.canton.sequencing.{EnvelopeHandler, SequencerConnections}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
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
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

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
    state: SyncDomainPersistentStateManagerImpl[S],
    override protected val futureSupervisor: FutureSupervisor,
)(implicit override protected val executionContext: ExecutionContext)
    extends ParticipantTopologyDispatcherCommon
    with HasFutureSupervision {

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
  )(implicit traceContext: TraceContext): Unit = {
    domains.remove(domain) match {
      case Some(outboxes) =>
        state.domainIdForAlias(domain).foreach(disconnectOutboxXes)
        outboxes.foreach(_.close())
      case None =>
        logger.debug(s"Topology pusher already disconnected from $domain")
    }
  }

  protected def disconnectOutboxXes(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): Unit

  override def awaitIdle(domain: DomainAlias, timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
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
  }

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

// TODO(#15161) collapse with abstract class and two base traits
class ParticipantTopologyDispatcherX(
    val manager: AuthorizedTopologyManagerX,
    participantId: ParticipantId,
    state: SyncDomainPersistentStateManagerImpl[SyncDomainPersistentState],
    crypto: Crypto,
    clock: Clock,
    config: LocalNodeConfig,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ParticipantTopologyDispatcherImplCommon[SyncDomainPersistentState](
      state,
      futureSupervisor,
    ) {

  override protected def managerQueueSize: Int =
    manager.queueSize + state.getAll.values.map(_.topologyManager.queueSize).sum

  // connect to manager
  manager.addObserver(new TopologyManagerObserver {
    override def addedNewTransactions(
        timestamp: CantonTimestamp,
        transactions: Seq[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
      val num = transactions.size
      domains.values.toList
        .flatMap(_.forgetNE)
        .collect { case outbox: StoreBasedDomainOutboxX => outbox }
        .parTraverse(_.newTransactionsAddedToAuthorizedStore(timestamp, num))
        .map(_ => ())
    }
  })

  override def trustDomain(domainId: DomainId, parameters: StaticDomainParameters)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    def alreadyTrustedInStore(
        store: TopologyStoreX[?]
    ): EitherT[FutureUnlessShutdown, String, Boolean] =
      for {
        alreadyTrusted <- EitherT
          .right[String](
            performUnlessClosingF(functionFullName)(
              store
                .findPositiveTransactions(
                  asOf = CantonTimestamp.MaxValue,
                  asOfInclusive = true,
                  isProposal = false,
                  types = Seq(DomainTrustCertificateX.code),
                  filterUid = Some(Seq(participantId.uid)),
                  filterNamespace = None,
                )
                .map(_.toTopologyState.exists {
                  case DomainTrustCertificateX(`participantId`, `domainId`, _, _) => true
                  case _ => false
                })
            )
          )
      } yield alreadyTrusted
    def trustDomain(
        state: SyncDomainPersistentState
    ): EitherT[FutureUnlessShutdown, String, Unit] =
      performUnlessClosingEitherUSF(functionFullName) {
        MonadUtil.unlessM(alreadyTrustedInStore(manager.store)) {
          manager
            .proposeAndAuthorize(
              TopologyChangeOpX.Replace,
              DomainTrustCertificateX(
                participantId,
                domainId,
                transferOnlyToGivenTargetDomains = false,
                targetDomains = Seq.empty,
              ),
              serial = None,
              // TODO(#12390) auto-determine signing keys
              signingKeys = Seq(participantId.uid.namespace.fingerprint),
              protocolVersion = state.protocolVersion,
              expectFullAuthorization = true,
              force = false,
            )
            // TODO(#14048) improve error handling
            .leftMap(_.cause)
        }
      }
    // check if cert already exists in the domain store
    val ret = for {
      state <- getState(domainId).leftMap(_.cause)
      alreadyTrustedInDomainStore <- alreadyTrustedInStore(state.topologyStore)
      _ <-
        if (alreadyTrustedInDomainStore) EitherT.rightT[FutureUnlessShutdown, String](())
        else trustDomain(state)
    } yield ()
    ret
  }

  override def onboardToDomain(
      domainId: DomainId,
      alias: DomainAlias,
      timeTrackerConfig: DomainTimeTrackerConfig,
      sequencerConnections: SequencerConnections,
      sequencerClientFactory: SequencerClientFactory,
      protocolVersion: ProtocolVersion,
      expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
    getState(domainId).flatMap { state =>
      (new ParticipantInitializeTopologyX(
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
        sequencerConnections,
        crypto,
        config.topologyX,
        protocolVersion,
        expectedSequencers,
      )).run()
    }
  }

  override def createHandler(
      domain: DomainAlias,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      client: DomainTopologyClientWithInit,
      sequencerClient: SequencerClient,
  ): ParticipantTopologyDispatcherHandle = {
    val domainLoggerFactory = loggerFactory.append("domainId", domainId.toString)
    new ParticipantTopologyDispatcherHandle {
      val handle = new SequencerBasedRegisterTopologyTransactionHandleX(
        sequencerClient,
        domainId,
        participantId,
        clock,
        config.topologyX,
        protocolVersion,
        timeouts,
        domainLoggerFactory,
      )

      override def domainConnected()(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit] =
        getState(domainId)
          .flatMap { state =>
            val queueBasedDomainOutbox = new QueueBasedDomainOutboxX(
              domain,
              domainId,
              participantId,
              protocolVersion,
              handle,
              client,
              state.domainOutboxQueue,
              state.topologyStore,
              timeouts,
              domainLoggerFactory,
              crypto,
            )

            val storeBasedDomainOutbox = new StoreBasedDomainOutboxX(
              domain,
              domainId,
              memberId = participantId,
              protocolVersion,
              handle,
              client,
              manager.store,
              targetStore = state.topologyStore,
              timeouts,
              loggerFactory,
              crypto,
              futureSupervisor = futureSupervisor,
            )
            ErrorUtil.requireState(
              !domains.contains(domain),
              s"topology pusher for $domain already exists",
            )
            val outboxes = NonEmpty(Seq, queueBasedDomainOutbox, storeBasedDomainOutbox)
            domains += domain -> outboxes

            state.topologyManager.addObserver(new TopologyManagerObserver {
              override def addedNewTransactions(
                  timestamp: CantonTimestamp,
                  transactions: Seq[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
              )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
                queueBasedDomainOutbox.newTransactionsAddedToAuthorizedStore(
                  timestamp,
                  transactions.size,
                )
            })

            outboxes.forgetNE.parTraverse_(
              _.startup().leftMap(
                DomainRegistryError.DomainRegistryInternalError.InitialOnboardingError(_)
              )
            )
          }

      override def processor: EnvelopeHandler = handle.processor

    }
  }

  override protected def disconnectOutboxXes(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug("Clearing domain topology manager observers")
    state.get(domainId).foreach(_.topologyManager.clearObservers())
  }

}

/** Utility class to dispatch the initial set of onboarding transactions to a domain - X version
  *
  * Generally, when we onboard to a new domain, we only want to onboard with the minimal set of
  * topology transactions that are required to join a domain. Otherwise, if we e.g. have
  * registered one million parties and then subsequently roll a key, we'd send an enormous
  * amount of unnecessary topology transactions.
  */
private class DomainOnboardingOutboxX(
    domain: DomainAlias,
    val domainId: DomainId,
    val protocolVersion: ProtocolVersion,
    participantId: ParticipantId,
    val handle: RegisterTopologyTransactionHandle,
    val authorizedStore: TopologyStoreX[TopologyStoreId.AuthorizedStore],
    val targetStore: TopologyStoreX[TopologyStoreId.DomainStore],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    override protected val crypto: Crypto,
) extends DomainOutboxDispatch
    with StoreBasedDomainOutboxDispatchHelperX {

  override protected val memberId: Member = participantId

  private def run()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = (for {
    initialTransactions <- loadInitialTransactionsFromStore()
    _ = logger.debug(
      s"Sending ${initialTransactions.size} onboarding transactions to ${domain}"
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
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Seq[GenericSignedTopologyTransactionX]] =
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
      initial: Seq[GenericSignedTopologyTransactionX]
  )(implicit traceContext: TraceContext): Either[DomainRegistryError, Unit] = {
    val (haveEncryptionKey, haveSigningKey) =
      initial.map(_.mapping).foldLeft((false, false)) {
        case ((haveEncryptionKey, haveSigningKey), OwnerToKeyMappingX(`participantId`, _, keys)) =>
          (
            haveEncryptionKey || keys.exists(!_.isSigning),
            haveSigningKey || keys.exists(_.isSigning),
          )
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

object DomainOnboardingOutboxX {
  def initiateOnboarding(
      domain: DomainAlias,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      participantId: ParticipantId,
      handle: RegisterTopologyTransactionHandle,
      authorizedStore: TopologyStoreX[TopologyStoreId.AuthorizedStore],
      targetStore: TopologyStoreX[TopologyStoreId.DomainStore],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      crypto: Crypto,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
    val outbox = new DomainOnboardingOutboxX(
      domain,
      domainId,
      protocolVersion,
      participantId,
      handle,
      authorizedStore,
      targetStore,
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
