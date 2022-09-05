// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import java.net.{InetSocketAddress, SocketAddress}
import java.time.{Clock, Duration}
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.daml.ledger.rxjava.grpc._
import com.daml.ledger.rxjava.grpc.helpers.TransactionsServiceImpl.LedgerItem
import com.daml.ledger.rxjava.{CommandCompletionClient, LedgerConfigurationClient, PackageClient}
import com.daml.grpc.adapter.{ExecutionSequencerFactory, SingleThreadExecutionSequencerPool}
import com.daml.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard, Authorizer}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.{
  CompletionEndResponse,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
}
import com.daml.ledger.api.v1.ledger_configuration_service.GetLedgerConfigurationResponse
import com.daml.ledger.api.v1.package_service.{
  GetPackageResponse,
  GetPackageStatusResponse,
  ListPackagesResponse,
}
import com.daml.ledger.api.v1.testing.time_service.GetTimeResponse
import com.daml.ledger.participant.state.index.impl.inmemory.InMemoryUserManagementStore
import com.daml.logging.LoggingContext
import com.google.protobuf.empty.Empty
import io.grpc._
import io.grpc.netty.NettyServerBuilder
import io.reactivex.Observable

import scala.concurrent.ExecutionContext.global
import scala.concurrent.{ExecutionContext, Future}

final class LedgerServices(val ledgerId: String) {

  import LedgerServices._

  val executionContext: ExecutionContext = global
  private val esf: ExecutionSequencerFactory = new SingleThreadExecutionSequencerPool(ledgerId)
  private val akkaSystem = ActorSystem("LedgerServicesParticipant")
  private val participantId = "LedgerServicesParticipant"
  private val authorizer =
    new Authorizer(
      () => Clock.systemUTC().instant(),
      ledgerId,
      participantId,
      new InMemoryUserManagementStore(),
      executionContext,
      userRightsCheckIntervalInSeconds = 1,
      akkaScheduler = akkaSystem.scheduler,
    )(LoggingContext.ForTesting)

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

  private def createServer(
      authService: AuthService,
      services: Seq[ServerServiceDefinition],
  ): Server = {
    val authorizationInterceptor = AuthorizationInterceptor(
      authService,
      Some(new InMemoryUserManagementStore()),
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
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: (ActiveContractClientImpl, ActiveContractsServiceImpl) => Any): Any = {
    val (service, serviceImpl) =
      ActiveContractsServiceImpl.createWithRef(getActiveContractsResponses, authorizer)(
        executionContext
      )
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new ActiveContractClientImpl(ledgerId, channel, esf, accessToken), serviceImpl)
    }
  }

  def withTimeClient(
      services: Seq[ServerServiceDefinition],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: TimeClientImpl => Any): Any =
    withServerAndChannel(authService, services) { channel =>
      f(new TimeClientImpl(ledgerId, channel, esf, accessToken))
    }

  def withCommandSubmissionClient(
      getResponse: () => Future[Empty],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
      timeout: java.util.Optional[Duration] = java.util.Optional.empty[Duration],
  )(f: (CommandSubmissionClientImpl, CommandSubmissionServiceImpl) => Any): Any = {
    val (service, serviceImpl) =
      CommandSubmissionServiceImpl.createWithRef(getResponse, authorizer)(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new CommandSubmissionClientImpl(ledgerId, channel, accessToken, timeout), serviceImpl)
    }
  }

  def withCommandCompletionClient(
      completions: List[CompletionStreamResponse],
      end: CompletionEndResponse,
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: (CommandCompletionClient, CommandCompletionServiceImpl) => Any): Any = {
    val (service, impl) =
      CommandCompletionServiceImpl.createWithRef(completions, end, authorizer)(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new CommandCompletionClientImpl(ledgerId, channel, esf, accessToken), impl)
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
      f(new PackageClientImpl(ledgerId, channel, accessToken), impl)
    }
  }

  def withCommandClient(
      submitAndWaitResponse: Future[Empty],
      submitAndWaitForTransactionIdResponse: Future[SubmitAndWaitForTransactionIdResponse],
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
      f(new CommandClientImpl(ledgerId, channel, accessToken), serviceImpl)
    }
  }

  def withConfigurationClient(
      responses: Seq[GetLedgerConfigurationResponse],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: (LedgerConfigurationClient, LedgerConfigurationServiceImpl) => Any): Any = {
    val (service, impl) =
      LedgerConfigurationServiceImpl.createWithRef(responses, authorizer)(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new LedgerConfigurationClientImpl(ledgerId, channel, esf, accessToken), impl)
    }
  }

  def withLedgerIdentityClient(
      getResponse: () => Future[String],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
      timeout: java.util.Optional[Duration] = java.util.Optional.empty[Duration],
  )(f: (LedgerIdentityClientImpl, LedgerIdentityServiceImpl) => Any): Any = {
    val (service, serviceImpl) =
      LedgerIdentityServiceImpl.createWithRef(getResponse, authorizer)(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new LedgerIdentityClientImpl(channel, accessToken, timeout), serviceImpl)
    }
  }

  def withTransactionsClient(
      ledgerContent: Observable[LedgerItem],
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: (TransactionClientImpl, TransactionsServiceImpl) => Any): Any = {
    val (service, serviceImpl) =
      TransactionsServiceImpl.createWithRef(ledgerContent, authorizer)(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new TransactionClientImpl(ledgerId, channel, esf, accessToken), serviceImpl)
    }
  }

  def withUserManagementClient(
      authService: AuthService = AuthServiceWildcard,
      accessToken: java.util.Optional[String] = java.util.Optional.empty[String],
  )(f: (UserManagementClientImpl, UserManagementServiceImpl) => Any): Any = {
    val (service, serviceImpl) =
      UserManagementServiceImpl.createWithRef(authorizer)(executionContext)
    withServerAndChannel(authService, Seq(service)) { channel =>
      f(new UserManagementClientImpl(channel, accessToken), serviceImpl)
    }
  }

  def withFakeLedgerServer(
      getActiveContractsResponse: Observable[GetActiveContractsResponse],
      transactions: Observable[LedgerItem],
      commandSubmissionResponse: Future[Empty],
      completions: List[CompletionStreamResponse],
      completionsEnd: CompletionEndResponse,
      submitAndWaitResponse: Future[Empty],
      submitAndWaitForTransactionIdResponse: Future[SubmitAndWaitForTransactionIdResponse],
      submitAndWaitForTransactionResponse: Future[SubmitAndWaitForTransactionResponse],
      submitAndWaitForTransactionTreeResponse: Future[SubmitAndWaitForTransactionTreeResponse],
      getTimeResponses: List[GetTimeResponse],
      getLedgerConfigurationResponses: Seq[GetLedgerConfigurationResponse],
      listPackagesResponse: Future[ListPackagesResponse],
      getPackageResponse: Future[GetPackageResponse],
      getPackageStatusResponse: Future[GetPackageStatusResponse],
      authService: AuthService,
  )(f: (Server, LedgerServicesImpls) => Any): Any = {
    val (services, impls) = LedgerServicesImpls.createWithRef(
      Future.successful(ledgerId),
      getActiveContractsResponse,
      transactions,
      commandSubmissionResponse,
      completions,
      completionsEnd,
      submitAndWaitResponse,
      submitAndWaitForTransactionIdResponse,
      submitAndWaitForTransactionResponse,
      submitAndWaitForTransactionTreeResponse,
      getTimeResponses,
      getLedgerConfigurationResponses,
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
