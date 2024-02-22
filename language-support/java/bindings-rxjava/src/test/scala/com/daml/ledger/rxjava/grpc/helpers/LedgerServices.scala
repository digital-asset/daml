// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import java.net.{InetSocketAddress, SocketAddress}
import java.time.{Clock, Duration}
import java.util.concurrent.TimeUnit

import org.apache.pekko.actor.ActorSystem
import com.daml.ledger.rxjava.grpc._
import com.daml.ledger.rxjava.grpc.helpers.UpdateServiceImpl.LedgerItem
import com.daml.ledger.rxjava.{CommandCompletionClient, EventQueryClient, PackageClient}
import com.daml.grpc.adapter.{ExecutionSequencerFactory, SingleThreadExecutionSequencerPool}
import com.digitalasset.canton.ledger.api.auth.interceptor.{
  AuthorizationInterceptor,
  IdentityProviderAwareAuthService,
}
import com.digitalasset.canton.ledger.api.auth.{
  AuthService,
  AuthServiceWildcard,
  Authorizer,
  ClaimSet,
}
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitForUpdateIdResponse,
}
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.ledger.api.v1.package_service.{
  GetPackageResponse,
  GetPackageStatusResponse,
  ListPackagesResponse,
}
import com.daml.ledger.api.v2.testing.time_service.GetTimeResponse
import com.daml.ledger.api.v2.command_submission_service.SubmitResponse
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.ledger.localstore.InMemoryUserManagementStore
import com.google.protobuf.empty.Empty
import io.grpc._
import io.grpc.netty.NettyServerBuilder
import io.reactivex.Observable

import scala.concurrent.ExecutionContext.global
import scala.concurrent.{ExecutionContext, Future}

final class LedgerServices(val name: String) {

  import LedgerServices._

  val executionContext: ExecutionContext = global
  private val esf: ExecutionSequencerFactory = new SingleThreadExecutionSequencerPool(name)
  private val pekkoSystem = ActorSystem("LedgerServicesParticipant")
  private val participantId = "LedgerServicesParticipant"
  private val loggerFactory = NamedLoggerFactory.root
  val authorizer: Authorizer =
    new Authorizer(
      now = () => Clock.systemUTC().instant(),
      ledgerId = name,
      participantId = participantId,
      userManagementStore = new InMemoryUserManagementStore(createAdmin = false, loggerFactory),
      ec = executionContext,
      userRightsCheckIntervalInSeconds = 1,
      pekkoScheduler = pekkoSystem.scheduler,
      jwtTimestampLeeway = None,
      telemetry = NoOpTelemetry,
      loggerFactory = loggerFactory,
    )

  def newServerBuilder(): NettyServerBuilder = NettyServerBuilder.forAddress(nextAddress())

  def withServer(authService: AuthService, services: Seq[ServerServiceDefinition])(
      f: Server => Any
  ): Any = {
    var server: Option[Server] = None
    try {
      val realServer = createServer(authService, services)
      server = Some(realServer)
      f(realServer)
    } finally {
      server.foreach(_.shutdown())
      server.foreach(_.awaitTermination(1, TimeUnit.MINUTES))
      ()
    }
  }

  def withServerAndChannel(authService: AuthService, services: Seq[ServerServiceDefinition])(
      f: ManagedChannel => Any
  ): Any = {
    withServer(authService, services) { server =>
      var channel: Option[ManagedChannel] = None
      try {
        val realChannel = createChannel(server.getPort)
        channel = Some(realChannel)
        f(realChannel)
      } finally {
        channel.foreach(_.shutdown())
        channel.foreach(_.awaitTermination(1, TimeUnit.MINUTES))
      }
    }
  }

  private class IDPAuthService extends IdentityProviderAwareAuthService {
    override def decodeMetadata(headers: Metadata)(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[ClaimSet] =
      Future.successful(ClaimSet.Unauthenticated)
  }

  private def createServer(
      authService: AuthService,
      services: Seq[ServerServiceDefinition],
  ): Server = {
    val authorizationInterceptor = AuthorizationInterceptor(
      authService,
      Some(new InMemoryUserManagementStore(false, loggerFactory)),
      new IDPAuthService,
      NoOpTelemetry,
      loggerFactory,
      executionContext,
    )
    services
      .foldLeft(newServerBuilder())(_ addService _)
      .intercept(authorizationInterceptor)
      .build()
      .start()
  }

  private def createChannel(port: Int): ManagedChannel =
    ManagedChannelBuilder
      .forAddress("localhost", port)
      .usePlaintext()
      .build()

  def withACSClient(
      getActiveContractsResponses: Observable[GetActiveContractsResponse],
      ledgerContent: Observable[LedgerItem],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: (StateClientImpl, StateServiceImpl) => Any): Any = {
    val (service, serviceImpl) =
      StateServiceImpl.createWithRef(getActiveContractsResponses, ledgerContent, authorizer)(
        executionContext
      )
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new StateClientImpl(channel, esf, accessToken), serviceImpl)
    }
  }

  def withTimeClient(
      services: Seq[ServerServiceDefinition],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: TimeClientImpl => Any): Any =
    withServerAndChannel(authService, services) { channel =>
      f(new TimeClientImpl(channel, esf, accessToken))
    }

  def withCommandSubmissionClient(
      getResponse: () => Future[SubmitResponse],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
      timeout: java.util.Optional[Duration] = java.util.Optional.empty[Duration],
  )(f: (CommandSubmissionClientImpl, CommandSubmissionServiceImpl) => Any): Any = {
    val (service, serviceImpl) =
      CommandSubmissionServiceImpl.createWithRef(getResponse, authorizer)(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new CommandSubmissionClientImpl(channel, accessToken, timeout), serviceImpl)
    }
  }

  def withCommandCompletionClient(
      completions: List[CompletionStreamResponse],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: (CommandCompletionClient, CommandCompletionServiceImpl) => Any): Any = {
    val (service, impl) =
      CommandCompletionServiceImpl.createWithRef(completions, authorizer)(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new CommandCompletionClientImpl(channel, esf, accessToken), impl)
    }
  }

  def withEventQueryClient(
      getEventsByContractIdResponse: Future[GetEventsByContractIdResponse],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: (EventQueryClient, EventQueryServiceImpl) => Any): Any = {
    val (service, impl) =
      EventQueryServiceImpl.createWithRef(
        getEventsByContractIdResponse,
        authorizer,
      )(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new EventQueryClientImpl(channel, accessToken), impl)
    }
  }

  def withPackageClient(
      listPackagesResponse: Future[ListPackagesResponse],
      getPackageResponse: Future[GetPackageResponse],
      getPackageStatusResponse: Future[GetPackageStatusResponse],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: (PackageClient, PackageServiceImpl) => Any): Any = {
    val (service, impl) =
      PackageServiceImpl.createWithRef(
        listPackagesResponse,
        getPackageResponse,
        getPackageStatusResponse,
        authorizer,
      )(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new PackageClientImpl(channel, accessToken), impl)
    }
  }

  def withCommandClient(
      submitAndWaitResponse: Future[Empty],
      submitAndWaitForTransactionIdResponse: Future[SubmitAndWaitForUpdateIdResponse],
      submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
      submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: (CommandClientImpl, CommandServiceImpl) => Any): Any = {
    val (service, serviceImpl) = CommandServiceImpl.createWithRef(
      submitAndWaitResponse,
      submitAndWaitForTransactionIdResponse,
      submitAndWaitForTransactionResponse,
      submitAndWaitForTransactionTreeResponse,
      authorizer,
    )(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new CommandClientImpl(channel, accessToken), serviceImpl)
    }
  }

  def withUpdateClient(
      ledgerContent: Observable[LedgerItem],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: (UpdateClientImpl, UpdateServiceImpl) => Any): Any = {
    val (service, serviceImpl) =
      UpdateServiceImpl.createWithRef(ledgerContent, authorizer)(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new UpdateClientImpl(channel, esf, accessToken), serviceImpl)
    }
  }

  def withUserManagementClient(
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: (UserManagementClientImpl, UserManagementServiceImpl) => Any): Any = {
    val (service, serviceImpl) =
      UserManagementServiceImpl.createWithRef(authorizer, loggerFactory)(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new UserManagementClientImpl(channel, accessToken), serviceImpl)
    }
  }

  def withFakeLedgerServer(
      getActiveContractsResponse: Observable[GetActiveContractsResponse],
      transactions: Observable[LedgerItem],
      commandSubmissionResponse: Future[SubmitResponse],
      completions: List[CompletionStreamResponse],
      submitAndWaitResponse: Future[Empty],
      submitAndWaitForUpdateIdResponse: Future[SubmitAndWaitForUpdateIdResponse],
      submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
      submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse],
      getTimeResponse: Future[GetTimeResponse],
      getEventsByContractIdResponse: Future[GetEventsByContractIdResponse],
      listPackagesResponse: Future[ListPackagesResponse],
      getPackageResponse: Future[GetPackageResponse],
      getPackageStatusResponse: Future[GetPackageStatusResponse],
      authService: AuthService,
  )(f: (Server, LedgerServicesImpls) => Any): Any = {
    val (services, impls) = LedgerServicesImpls.createWithRef(
      getActiveContractsResponse,
      transactions,
      commandSubmissionResponse,
      completions,
      submitAndWaitResponse,
      submitAndWaitForUpdateIdResponse,
      submitAndWaitForTransactionResponse,
      submitAndWaitForTransactionTreeResponse,
      getTimeResponse,
      getEventsByContractIdResponse,
      listPackagesResponse,
      getPackageResponse,
      getPackageStatusResponse,
      authorizer,
    )(executionContext)
    withServer(authService, services) { server =>
      f(server, impls)
    }
  }
}

object LedgerServices {
  def nextAddress(): SocketAddress = new InetSocketAddress(0)
}
