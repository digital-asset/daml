// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, SigningKeyUsage}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.domain.config.PublicServerConfig
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.admin.data.{
  SequencerAdminStatus,
  SequencerHealthStatus,
}
import com.digitalasset.canton.domain.sequencing.authentication.grpc.{
  SequencerAuthenticationServerInterceptor,
  SequencerConnectServerInterceptor,
}
import com.digitalasset.canton.domain.sequencing.authentication.{
  MemberAuthenticationService,
  MemberAuthenticationServiceFactory,
  MemberAuthenticationStore,
}
import com.digitalasset.canton.domain.sequencing.config.SequencerNodeParameters
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.domain.sequencing.service.*
import com.digitalasset.canton.domain.sequencing.service.channel.GrpcSequencerChannelService
import com.digitalasset.canton.health.HealthListener
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  Lifecycle,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.DomainParametersLookup.SequencerDomainParameters
import com.digitalasset.canton.protocol.{
  DomainParametersLookup,
  DynamicDomainParametersLookup,
  StaticDomainParameters,
}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencer.admin.v30.{
  SequencerAdministrationServiceGrpc,
  SequencerPruningAdministrationServiceGrpc,
}
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.handlers.{
  DiscardIgnoredEvents,
  EnvelopeOpener,
  StripSignature,
}
import com.digitalasset.canton.sequencing.traffic.TrafficControlProcessor
import com.digitalasset.canton.store.{IndexedDomain, SequencerCounterTrackerStore}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessingSubscriber,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{TopologyStateForInitializationService, TopologyStore}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  DomainTrustCertificate,
  MediatorDomainState,
  SequencerDomainState,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}
import com.digitalasset.canton.{SequencerCounter, config, lifecycle}
import com.google.common.annotations.VisibleForTesting
import io.grpc.{ServerInterceptors, ServerServiceDefinition}
import org.apache.pekko.actor.ActorSystem

import scala.concurrent.{ExecutionContext, Future}

final case class SequencerAuthenticationConfig(
    nonceExpirationInterval: config.NonNegativeFiniteDuration,
    maxTokenExpirationInterval: config.NonNegativeFiniteDuration,
) {
  // only authentication tokens are supported
  val check: AuthenticationCheck = AuthenticationCheck.AuthenticationToken
}

object SequencerAuthenticationConfig {
  val Disabled: Option[SequencerAuthenticationConfig] = None
}

/** Run a sequencer and its supporting services.
  *
  * @param authenticationConfig   Authentication setup if supported, otherwise none.
  * @param staticDomainParameters The set of members to register on startup statically.
  *
  * Creates a sequencer client and connect it to a topology client
  * to power sequencer authentication.
  */
class SequencerRuntime(
    sequencerId: SequencerId,
    val sequencer: Sequencer,
    client: SequencerClient,
    staticDomainParameters: StaticDomainParameters,
    localNodeParameters: SequencerNodeParameters,
    publicServerConfig: PublicServerConfig,
    timeTracker: DomainTimeTracker,
    val metrics: SequencerMetrics,
    indexedDomain: IndexedDomain,
    val syncCrypto: DomainSyncCryptoClient,
    domainTopologyManager: DomainTopologyManager,
    topologyStore: TopologyStore[DomainStore],
    topologyClient: DomainTopologyClientWithInit,
    topologyProcessor: TopologyTransactionProcessor,
    topologyManagerStatusO: Option[TopologyManagerStatus],
    storage: Storage,
    clock: Clock,
    authenticationConfig: SequencerAuthenticationConfig,
    staticMembersToRegister: Seq[Member],
    futureSupervisor: FutureSupervisor,
    memberAuthenticationServiceFactory: MemberAuthenticationServiceFactory,
    topologyStateForInitializationService: TopologyStateForInitializationService,
    maybeDomainOutboxFactory: Option[DomainOutboxFactorySingleCreate],
    protected val loggerFactory: NamedLoggerFactory,
    runtimeReadyPromise: PromiseUnlessShutdown[Unit],
)(implicit
    executionContext: ExecutionContext,
    actorSystem: ActorSystem,
    traceContext: TraceContext,
) extends FlagCloseable
    with HasCloseContext
    with NamedLogging {

  override protected def timeouts: ProcessingTimeout = localNodeParameters.processingTimeouts

  def domainId: DomainId = indexedDomain.domainId

  def initialize()(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    def keyCheckET =
      EitherT {
        val snapshot = syncCrypto
          .currentSnapshotApproximation(TraceContext.empty)
          .ipsSnapshot
        snapshot
          .signingKeys(sequencerId, SigningKeyUsage.All)
          .map { keys =>
            Either.cond(keys.nonEmpty, (), s"Missing sequencer keys at ${snapshot.referenceTime}.")
          }
      }

    def registerInitialMembers: EitherT[Future, String, Unit] = {
      logger.debug(s"Registering initial sequencer members: $staticMembersToRegister")
      staticMembersToRegister
        .parTraverse_ { member =>
          for {
            firstKnownAtO <- EitherT.right(topologyClient.headSnapshot.memberFirstKnownAt(member))
            _ <- {
              firstKnownAtO match {
                case Some((_, firstKnownAtEffectiveTime)) =>
                  sequencer
                    .registerMemberInternal(member, firstKnownAtEffectiveTime.value)
                    .leftMap(_.toString)
                case None => EitherT.leftT[Future, Unit](s"Member $member not known in topology")
              }
            }
          } yield ()
        }
    }

    for {
      _ <- keyCheckET
      _ <- registerInitialMembers
    } yield ()
  }

  private val sequencerDomainParamsLookup
      : DynamicDomainParametersLookup[SequencerDomainParameters] =
    DomainParametersLookup.forSequencerDomainParameters(
      staticDomainParameters,
      publicServerConfig.overrideMaxRequestSize,
      topologyClient,
      futureSupervisor,
      loggerFactory,
    )

  private val sequencerService = GrpcSequencerService(
    sequencer,
    metrics,
    authenticationConfig.check,
    clock,
    sequencerDomainParamsLookup,
    localNodeParameters,
    staticDomainParameters.protocolVersion,
    topologyStateForInitializationService,
    loggerFactory,
  )

  private val sequencerChannelServiceO = Option.when(
    localNodeParameters.unsafeEnableOnlinePartyReplication
  )(
    new GrpcSequencerChannelService(
      authenticationConfig.check,
      localNodeParameters.processingTimeouts,
      loggerFactory,
    )
  )

  sequencer
    .registerOnHealthChange(new HealthListener {
      override def name: String = "SequencerRuntime"

      override def poke()(implicit traceContext: TraceContext): Unit = {
        val status = sequencer.getState
        if (!status.isActive && !isClosing) {
          logger.warn(
            s"Sequencer is unhealthy, so disconnecting all members. ${status.details.getOrElse("")}"
          )
          // Run into a Future because closing subscriptions can take time, especially when having DB connection issues
          FutureUtil.doNotAwait(
            Future(sequencerService.disconnectAllMembers()),
            "Failed to disconnect members",
          )
          sequencerChannelServiceO.foreach(channelService =>
            FutureUtil.doNotAwait(
              Future(channelService.disconnectAllMembers()),
              "Failed to disconnect members from sequencer channels",
            )
          )
        } else {
          logger.info(s"Sequencer is healthy")
        }
      }
    })
    .discard[Boolean]

  private case class AuthenticationServices(
      memberAuthenticationService: MemberAuthenticationService,
      sequencerAuthenticationService: GrpcSequencerAuthenticationService,
      authenticationInterceptor: SequencerAuthenticationServerInterceptor,
  )

  private val authenticationServices = {
    val authenticationService = memberAuthenticationServiceFactory.createAndSubscribe(
      syncCrypto,
      new MemberAuthenticationStore(),
      // closing the subscription when the token expires will force the client to try to reconnect
      // immediately and notice it is unauthenticated, which will cause it to also start re-authenticating
      // it's important to disconnect the member AFTER we expired the token, as otherwise, the member
      // can still re-subscribe with the token just before we removed it
      Traced.lift { case (member, tc) =>
        sequencerService.disconnectMember(member)(tc)
        sequencerChannelServiceO.foreach(_.disconnectMember(member)(tc))
      },
      runtimeReadyPromise.future.map(_ =>
        ()
      ), // on shutdown, MemberAuthenticationStore will be closed via closeContext
    )

    val sequencerAuthenticationService =
      new GrpcSequencerAuthenticationService(
        authenticationService,
        staticDomainParameters.protocolVersion,
        loggerFactory,
      )

    val sequencerAuthInterceptor =
      new SequencerAuthenticationServerInterceptor(authenticationService, loggerFactory)

    AuthenticationServices(
      authenticationService,
      sequencerAuthenticationService,
      sequencerAuthInterceptor,
    )
  }

  def health: SequencerHealthStatus = sequencer.getState

  def topologyQueue: TopologyQueueStatus = TopologyQueueStatus(
    manager = topologyManagerStatusO.map(_.queueSize).getOrElse(0),
    dispatcher = domainOutboxO.map(_.queueSize).getOrElse(0),
    clients = topologyClient.numPendingChanges,
  )

  def adminStatus: SequencerAdminStatus = sequencer.adminStatus

  def fetchActiveMembers(): Seq[Member] =
    sequencerService.membersWithActiveSubscriptions

  def registerAdminGrpcServices(
      register: ServerServiceDefinition => Unit
  ): Unit = {
    sequencer.adminServices.foreach(register)
    register(
      SequencerAdministrationServiceGrpc.bindService(
        sequencerAdministrationService,
        executionContext,
      )
    )
    register(
      SequencerPruningAdministrationServiceGrpc.bindService(
        new GrpcSequencerPruningAdministrationService(sequencer, loggerFactory),
        executionContext,
      )
    )
    // register the api info services
    register(
      ApiInfoServiceGrpc.bindService(
        new GrpcApiInfoService(
          CantonGrpcUtil.ApiName.AdminApi
        ),
        executionContext,
      )
    )
  }

  def sequencerServices(implicit ec: ExecutionContext): Seq[ServerServiceDefinition] = {
    def interceptAuthentication(svcDef: ServerServiceDefinition) = {
      import scala.jdk.CollectionConverters.*

      // use the auth service interceptor if available
      val interceptors = List(authenticationServices.authenticationInterceptor).asJava

      ServerInterceptors.intercept(svcDef, interceptors)
    }

    Seq(
      ServerInterceptors.intercept(
        v30.SequencerConnectServiceGrpc.bindService(
          new GrpcSequencerConnectService(
            domainId,
            sequencerId,
            staticDomainParameters,
            domainTopologyManager,
            syncCrypto,
            loggerFactory,
          )(
            ec
          ),
          executionContext,
        ),
        new SequencerConnectServerInterceptor(loggerFactory),
      ),
      v30.SequencerAuthenticationServiceGrpc
        .bindService(authenticationServices.sequencerAuthenticationService, ec),
      interceptAuthentication(v30.SequencerServiceGrpc.bindService(sequencerService, ec)),
      ApiInfoServiceGrpc.bindService(
        new GrpcApiInfoService(
          CantonGrpcUtil.ApiName.SequencerPublicApi
        ),
        executionContext,
      ),
    ) :++ sequencerChannelServiceO
      .map(svc => interceptAuthentication(v30.SequencerChannelServiceGrpc.bindService(svc, ec)))
      .toList
  }

  @VisibleForTesting
  protected[canton] val topologyManagerSequencerCounterTrackerStore: SequencerCounterTrackerStore =
    SequencerCounterTrackerStore(
      storage,
      indexedDomain,
      timeouts,
      loggerFactory,
    )

  logger.info("Subscribing to topology transactions for auto-registering members")
  topologyProcessor.subscribe(new TopologyTransactionProcessingSubscriber {
    override val executionOrder: Int = 5

    override def observed(
        sequencedTimestamp: SequencedTime,
        effectiveTimestamp: EffectiveTime,
        sequencerCounter: SequencerCounter,
        transactions: Seq[GenericSignedTopologyTransaction],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

      val possibleNewMembers = transactions.map(_.mapping).flatMap {
        case dtc: DomainTrustCertificate => Seq(dtc.participantId)
        case mds: MediatorDomainState => mds.active ++ mds.observers
        case sds: SequencerDomainState => sds.active ++ sds.observers
        case _ => Seq.empty
      }

      // TODO(#18401): Change F to FUS in registerMemberInternal
      val f = possibleNewMembers
        .parTraverse_ { member =>
          logger.info(s"Topology change has triggered sequencer registration of member $member")
          sequencer.registerMemberInternal(member, effectiveTimestamp.value)
        }
        .valueOr(e =>
          ErrorUtil.internalError(new RuntimeException(s"Failed to register member: $e"))
        )
      lifecycle.FutureUnlessShutdown.outcomeF(f)
    }
  })

  private lazy val domainOutboxO: Option[DomainOutboxHandle] =
    maybeDomainOutboxFactory
      .map(
        _.createOnlyOnce(
          staticDomainParameters.protocolVersion,
          topologyClient,
          client,
          clock,
          loggerFactory,
        )
      )

  private val topologyHandler = topologyProcessor.createHandler(domainId)
  private val trafficProcessor =
    new TrafficControlProcessor(
      syncCrypto,
      domainId,
      sequencer.rateLimitManager.flatMap(_.balanceKnownUntil),
      loggerFactory,
    )

  sequencer.rateLimitManager.foreach(rlm => trafficProcessor.subscribe(rlm.balanceUpdateSubscriber))

  private val eventHandler = StripSignature(topologyHandler.combineWith(trafficProcessor))

  private val sequencerAdministrationService =
    new GrpcSequencerAdministrationService(
      sequencer,
      client,
      topologyStore,
      topologyClient,
      timeTracker,
      staticDomainParameters,
      loggerFactory,
    )

  def initializeAll()(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    for {
      _ <- initialize()
      _ = logger.debug("Subscribing topology client within sequencer runtime")
      _ <- EitherT.right(
        client.subscribeTracking(
          topologyManagerSequencerCounterTrackerStore,
          DiscardIgnoredEvents(loggerFactory) {
            EnvelopeOpener(staticDomainParameters.protocolVersion, syncCrypto.pureCrypto)(
              eventHandler
            )
          },
          timeTracker,
        )
      )
      _ <- domainOutboxO
        .map(_.startup().onShutdown(Right(())))
        .getOrElse(EitherT.rightT[Future, String](()))
    } yield {
      logger.info("Sequencer runtime initialized")
      runtimeReadyPromise.outcome(())
    }

  override def onClosed(): Unit =
    Lifecycle.close(
      Lifecycle.toCloseableOption(sequencer.rateLimitManager),
      timeTracker,
      syncCrypto,
      topologyClient,
      client,
      topologyProcessor,
      topologyManagerSequencerCounterTrackerStore,
      sequencerService,
      Lifecycle.toCloseableOption(sequencerChannelServiceO),
      authenticationServices.memberAuthenticationService,
      sequencer,
    )(logger)
}
