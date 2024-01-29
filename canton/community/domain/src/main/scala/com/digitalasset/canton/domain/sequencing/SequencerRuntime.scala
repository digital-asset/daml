// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.admin.v30.{
  SequencerAdministrationServiceGrpc,
  SequencerVersionServiceGrpc,
}
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.domain.config.PublicServerConfig
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.authentication.grpc.{
  SequencerAuthenticationServerInterceptor,
  SequencerConnectServerInterceptor,
}
import com.digitalasset.canton.domain.sequencing.authentication.{
  MemberAuthenticationService,
  MemberAuthenticationServiceFactory,
  MemberAuthenticationStore,
}
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.domain.sequencing.sequencer.errors.RegisterMemberError.AlreadyRegisteredError
import com.digitalasset.canton.domain.sequencing.sequencer.errors.{
  OperationError,
  RegisterMemberError,
  SequencerWriteError,
}
import com.digitalasset.canton.domain.sequencing.service.*
import com.digitalasset.canton.health.HealthListener
import com.digitalasset.canton.health.admin.data.{SequencerHealthStatus, TopologyQueueStatus}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DomainParametersLookup.SequencerDomainParameters
import com.digitalasset.canton.protocol.{
  DomainParametersLookup,
  DynamicDomainParametersLookup,
  StaticDomainParameters,
}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.store.TopologyStateForInitializationService
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.FutureUtil
import com.digitalasset.canton.{DiscardOps, config}
import io.grpc.{ServerInterceptors, ServerServiceDefinition}
import org.apache.pekko.actor.ActorSystem

import scala.concurrent.{ExecutionContext, Future, Promise}

final case class SequencerAuthenticationConfig(
    nonceExpirationTime: config.NonNegativeFiniteDuration,
    tokenExpirationTime: config.NonNegativeFiniteDuration,
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
  */
class SequencerRuntime(
    sequencerId: SequencerId,
    val sequencer: Sequencer,
    staticDomainParameters: StaticDomainParameters,
    localNodeParameters: CantonNodeWithSequencerParameters,
    publicServerConfig: PublicServerConfig,
    val metrics: SequencerMetrics,
    val domainId: DomainId,
    protected val syncCrypto: DomainSyncCryptoClient,
    topologyClient: DomainTopologyClientWithInit,
    topologyManagerStatusO: Option[TopologyManagerStatus],
    storage: Storage,
    clock: Clock,
    authenticationConfig: SequencerAuthenticationConfig,
    additionalAdminServiceFactory: Sequencer => Option[ServerServiceDefinition],
    staticMembersToRegister: Seq[Member],
    futureSupervisor: FutureSupervisor,
    memberAuthenticationServiceFactory: MemberAuthenticationServiceFactory,
    topologyStateForInitializationService: Option[TopologyStateForInitializationService],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext,
    actorSystem: ActorSystem,
) extends FlagCloseable
    with HasCloseContext
    with NamedLogging {

  override protected def timeouts: ProcessingTimeout = localNodeParameters.processingTimeouts

  protected val isTopologyInitializedPromise = Promise[Unit]()

  protected def domainOutboxO: Option[DomainOutboxHandle] = None

  def initialize(
      topologyInitIsCompleted: Boolean = true
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    def keyCheckET =
      EitherT {
        val snapshot = syncCrypto
          .currentSnapshotApproximation(TraceContext.empty)
          .ipsSnapshot
        snapshot
          .signingKey(sequencerId)
          .map { keyO =>
            Either.cond(keyO.nonEmpty, (), s"Missing sequencer keys at ${snapshot.referenceTime}.")
          }
      }

    def registerInitialMembers = {
      logger.debug("Registering initial sequencer members")
      // only register the sequencer itself if we have remote sequencers that will necessitate topology transactions
      // being sent to them
      staticMembersToRegister
        .parTraverse(
          sequencer
            .ensureRegistered(_)
            .leftFlatMap[Unit, SequencerWriteError[RegisterMemberError]] {
              // if other sibling sequencers are initializing at the same time and try to run this step,
              // or if a sequencer automatically registers the idm (eg the Ethereum sequencer)
              // we might get AlreadyRegisteredError which we can safely ignore
              case OperationError(_: AlreadyRegisteredError) => EitherT.pure(())
              case otherError => EitherT.leftT(otherError)
            }
        )
    }

    for {
      _ <- keyCheckET
      _ <- registerInitialMembers.leftMap(_.toString)
    } yield {
      // if we run embedded, we complete the future here
      if (topologyInitIsCompleted) {
        isTopologyInitializedPromise.success(())
      }
    }
  }

  protected val sequencerDomainParamsLookup
      : DynamicDomainParametersLookup[SequencerDomainParameters] =
    DomainParametersLookup.forSequencerDomainParameters(
      staticDomainParameters,
      publicServerConfig.overrideMaxRequestSize,
      topologyClient,
      futureSupervisor,
      loggerFactory,
    )

  /** Only SequencerXs expect MediatorXs to expose "topology_request_address" and
    * allow corresponding unauthenticated messages upon initial bootstrap.
    */
  protected def mediatorsProcessParticipantTopologyRequests: Boolean = false

  private val sequencerService = GrpcSequencerService(
    sequencer,
    metrics,
    authenticationConfig.check,
    clock,
    sequencerDomainParamsLookup,
    localNodeParameters,
    staticDomainParameters.protocolVersion,
    topologyStateForInitializationService,
    mediatorsProcessParticipantTopologyRequests,
    loggerFactory,
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
        } else {
          logger.info(s"Sequencer is healthy")
        }
      }
    })
    .discard[Boolean]

  private val sequencerAdministrationService =
    new GrpcSequencerAdministrationService(sequencer, loggerFactory)

  private case class AuthenticationServices(
      memberAuthenticationService: MemberAuthenticationService,
      sequencerAuthenticationService: GrpcSequencerAuthenticationService,
      authenticationInterceptor: SequencerAuthenticationServerInterceptor,
  )

  private val authenticationServices = {
    val authenticationService = memberAuthenticationServiceFactory.createAndSubscribe(
      syncCrypto,
      MemberAuthenticationStore(storage, timeouts, loggerFactory, closeContext),
      // closing the subscription when the token expires will force the client to try to reconnect
      // immediately and notice it is unauthenticated, which will cause it to also start reauthenticating
      // it's important to disconnect the member AFTER we expired the token, as otherwise, the member
      // can still re-subscribe with the token just before we removed it
      Traced.lift(sequencerService.disconnectMember(_)(_)),
      isTopologyInitializedPromise.future,
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

  def health: Future[SequencerHealthStatus] =
    Future.successful(sequencer.getState)

  def topologyQueue: TopologyQueueStatus = TopologyQueueStatus(
    manager = topologyManagerStatusO.map(_.queueSize).getOrElse(0),
    dispatcher = domainOutboxO.map(_.queueSize).getOrElse(0),
    clients = topologyClient.numPendingChanges,
  )

  def fetchActiveMembers(): Future[Seq[Member]] =
    Future.successful(sequencerService.membersWithActiveSubscriptions)

  def registerAdminGrpcServices(
      register: ServerServiceDefinition => Unit
  )(implicit ec: ExecutionContext): Unit = {
    register(
      SequencerAdministrationServiceGrpc.bindService(sequencerAdministrationService, ec)
    )
    register(
      SequencerVersionServiceGrpc
        .bindService(
          new GrpcSequencerVersionService(staticDomainParameters.protocolVersion, loggerFactory),
          executionContext,
        )
    )
    // hook for registering enterprise administration service if in an appropriate environment
    additionalAdminServiceFactory(sequencer).foreach(register)
  }

  def domainServices(implicit ec: ExecutionContext): Seq[ServerServiceDefinition] = Seq(
    {
      ServerInterceptors.intercept(
        v30.SequencerConnectServiceGrpc.bindService(
          new GrpcSequencerConnectService(
            domainId,
            sequencerId,
            staticDomainParameters,
            syncCrypto,
            loggerFactory,
          )(
            ec
          ),
          executionContext,
        ),
        new SequencerConnectServerInterceptor(loggerFactory),
      )
    }, {
      SequencerVersionServiceGrpc.bindService(
        new GrpcSequencerVersionService(staticDomainParameters.protocolVersion, loggerFactory),
        ec,
      )
    }, {
      v30.SequencerAuthenticationServiceGrpc
        .bindService(authenticationServices.sequencerAuthenticationService, ec)
    }, {
      import scala.jdk.CollectionConverters.*

      // use the auth service interceptor if available
      val interceptors = List(authenticationServices.authenticationInterceptor).asJava

      ServerInterceptors.intercept(
        v30.SequencerServiceGrpc.bindService(sequencerService, ec),
        interceptors,
      )
    },
  )

  override def onClosed(): Unit =
    Lifecycle.close(
      syncCrypto,
      topologyClient,
      sequencerService,
      authenticationServices.memberAuthenticationService,
      sequencer,
    )(logger)

}
