// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.server.LedgerApiServer

import java.io.IOException
import java.net.{BindException, InetSocketAddress}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.stream.ActorMaterializer
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.{Engine, EngineInfo}
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionEndRequest
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfiguration
import com.digitalasset.ledger.backend.api.v1.LedgerBackend
import com.digitalasset.ledger.client.configuration.TlsConfiguration
import com.digitalasset.ledger.client.services.commands.CommandSubmissionFlow
import com.digitalasset.platform.api.grpc.GrpcApiUtil
import com.digitalasset.platform.sandbox.config.{SandboxConfig, SandboxContext}
import com.digitalasset.platform.sandbox.services._
import com.digitalasset.platform.sandbox.services.transaction.SandboxTransactionService
import com.digitalasset.platform.sandbox.stores.ledger._
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.digitalasset.platform.server.services.command.ReferenceCommandService
import com.digitalasset.platform.server.services.identity.LedgerIdentityServiceImpl
import com.digitalasset.platform.server.services.testing.{ReferenceTimeService, TimeServiceBackend}
import com.digitalasset.platform.services.time.TimeProviderType
import io.grpc.netty.{GrpcSslContexts, NettyServerBuilder}
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.{BindableService, Server}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.ssl.{ClientAuth, SslContext}
import io.netty.util.concurrent.DefaultThreadFactory
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace

object LedgerApiServer {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def apply(
      ledgerBackend: LedgerBackend,
      timeProvider: TimeProvider,
      engine: Engine,
      config: => SandboxConfig,
      serverPort: Int, //TODO: why do we need this if we already have a SandboxConfig?
      optTimeServiceBackend: Option[TimeServiceBackend],
      optResetService: Option[SandboxResetService])(
      implicit materializer: ActorMaterializer): LedgerApiServer = {

    new LedgerApiServer(
      ledgerBackend, { (mat, esf) =>
        services(config, ledgerBackend, engine, timeProvider, optTimeServiceBackend)(mat, esf)
      },
      optResetService,
      config.address,
      serverPort,
      serverSslContext(config.tlsConfig, ClientAuth.REQUIRE)
    ).start()
  }

  /** If enabled and all required fields are present, it returns an SslContext suitable for server usage */
  private def serverSslContext(
      tlsConfig: Option[TlsConfiguration],
      clientAuth: ClientAuth): Option[SslContext] =
    tlsConfig.flatMap { c =>
      if (c.enabled)
        Some(
          GrpcSslContexts
            .forServer(
              c.keyCertChainFile.getOrElse(throw new IllegalStateException(
                s"Unable to convert ${this.toString} to SSL Context: cannot create server context without keyCertChainFile.")),
              c.keyFileOrFail
            )
            .trustManager(c.trustCertCollectionFile.orNull)
            .clientAuth(clientAuth)
            .build
        )
      else None
    }

  private def services(
      config: SandboxConfig,
      ledgerBackend: LedgerBackend,
      engine: Engine,
      timeProvider: TimeProvider,
      optTimeServiceBackend: Option[TimeServiceBackend])(
      implicit mat: ActorMaterializer,
      esf: ExecutionSequencerFactory): List[BindableService] = {
    implicit val ec: ExecutionContext = mat.system.dispatcher

    val context = SandboxContext.fromConfig(config)

    val packageResolver = (pkgId: Ref.PackageId) =>
      Future.successful(context.packageContainer.getPackage(pkgId))

    val identifierResolver: IdentifierResolver = new IdentifierResolver(packageResolver)

    val submissionService =
      SandboxSubmissionService.createApiService(
        context.packageContainer,
        identifierResolver,
        ledgerBackend,
        config.timeModel,
        timeProvider,
        new CommandExecutorImpl(engine, context.packageContainer)
      )

    logger.info(EngineInfo.show)

    val transactionService =
      SandboxTransactionService.createApiService(ledgerBackend, identifierResolver)

    val identityService = LedgerIdentityServiceImpl(ledgerBackend.ledgerId)

    val packageService = SandboxPackageService(context.sandboxTemplateStore, ledgerBackend.ledgerId)

    val configurationService =
      LedgerConfigurationServiceImpl(
        LedgerConfiguration(
          Some(GrpcApiUtil.durationToProto(config.timeModel.minTtl)),
          Some(GrpcApiUtil.durationToProto(config.timeModel.maxTtl))),
        ledgerBackend.ledgerId
      )

    val completionService =
      SandboxCommandCompletionService(ledgerBackend)

    val commandService = ReferenceCommandService(
      ReferenceCommandService.Configuration(
        ledgerBackend.ledgerId,
        config.commandConfig.inputBufferSize,
        config.commandConfig.maxParallelSubmissions,
        config.commandConfig.maxCommandsInFlight,
        config.commandConfig.limitMaxCommandsInFlight,
        config.commandConfig.historySize,
        config.commandConfig.retentionPeriod,
        config.commandConfig.commandTtl
      ),
      // Using local services skips the gRPC layer, improving performance.
      ReferenceCommandService.LowLevelCommandServiceAccess.LocalServices(
        CommandSubmissionFlow(
          submissionService.submit,
          config.commandConfig.maxParallelSubmissions),
        r =>
          completionService.service
            .asInstanceOf[SandboxCommandCompletionService]
            .completionStreamSource(r),
        () => completionService.completionEnd(CompletionEndRequest(ledgerBackend.ledgerId))
      )
    )

    val activeContractsService =
      SandboxActiveContractsService(ledgerBackend, identifierResolver)

    val reflectionService = ProtoReflectionService.newInstance()

    val timeServiceOpt =
      optTimeServiceBackend.map { tsb =>
        ReferenceTimeService(
          ledgerBackend.ledgerId,
          tsb,
          config.timeProviderType == TimeProviderType.StaticAllowBackwards
        )
      }

    timeServiceOpt.toList :::
      List(
      identityService,
      packageService,
      configurationService,
      submissionService,
      transactionService,
      completionService,
      commandService,
      activeContractsService,
      reflectionService
    )
  }
}

class LedgerApiServer(
    ledgerBackend: LedgerBackend,
    services: (ActorMaterializer, ExecutionSequencerFactory) => Iterable[BindableService],
    optResetService: Option[SandboxResetService],
    addressOption: Option[String],
    serverPort: Int,
    maybeBundle: Option[SslContext] = None)(implicit materializer: ActorMaterializer)
    extends AutoCloseable {

  class UnableToBind(port: Int, cause: Throwable)
      extends RuntimeException(
        s"LedgerApiServer was unable to bind to port $port. " +
          "User should terminate the process occupying the port, or choose a different one.",
        cause)
      with NoStackTrace

  private val logger = LoggerFactory.getLogger(this.getClass)
  private var actualPort
    : Int = -1 // we need this to remember ephemeral ports when using ResetService
  def port: Int = if (actualPort == -1) serverPort else actualPort

  private val serverEsf = new AkkaExecutionSequencerPool(
    // NOTE(JM): Pick a unique pool name as we want to allow multiple ledger api server
    // instances, and it's pretty difficult to wait for the name to become available
    // again (the name deregistration is asynchronous and the close method is not waiting for
    // it, and it isn't trivial to implement).
    // https://doc.akka.io/docs/akka/2.5/actors.html#graceful-stop
    s"ledger-api-server-rs-grpc-bridge-${UUID.randomUUID}",
    Runtime.getRuntime.availableProcessors() * 8
  )(materializer.system)
  private val serverEventLoopGroup = createEventLoopGroup(materializer.system.name)

  private var runningServices: Iterable[BindableService] = services(materializer, serverEsf)

  def getServer = server

  private var server: Server = _

  private def start(): LedgerApiServer = {
    val builder = addressOption.fold(NettyServerBuilder.forPort(port))(address =>
      NettyServerBuilder.forAddress(new InetSocketAddress(address, port)))

    maybeBundle
      .fold {
        logger.info("Starting plainText server")
      } { sslContext =>
        logger.info("Starting TLS server")
        val _ = builder.sslContext(sslContext)
      }

    builder.directExecutor()
    builder.workerEventLoopGroup(serverEventLoopGroup)
    builder.permitKeepAliveTime(10, TimeUnit.SECONDS)
    builder.permitKeepAliveWithoutCalls(true)
    server = optResetService.toList
      .foldLeft(runningServices.foldLeft(builder)(_ addService _))(_ addService _)
      .build
    try {
      server.start()
      actualPort = server.getPort
    } catch {
      case io: IOException if io.getCause != null && io.getCause.isInstanceOf[BindException] =>
        throw new UnableToBind(port, io.getCause)
    }
    logger.info(s"listening on ${addressOption.getOrElse("localhost")}:${server.getPort}")
    this
  }

  def closeAllServices(): Unit = {
    runningServices.foreach {
      case closeable: AutoCloseable => closeable.close()
      case _ => ()
    }
    runningServices = Nil
  }

  private def createEventLoopGroup(threadPoolName: String): NioEventLoopGroup = {
    val threadFactory =
      new DefaultThreadFactory(s"$threadPoolName-grpc-eventloop-${UUID.randomUUID}", true)
    val parallelism = Runtime.getRuntime.availableProcessors
    new NioEventLoopGroup(parallelism, threadFactory)
  }

  override def close(): Unit = {
    closeAllServices()
    ledgerBackend.close()
    Option(server).foreach { s =>
      s.shutdownNow()
      s.awaitTermination(10, TimeUnit.SECONDS)
      server = null
    }
    // `shutdownGracefully` has a "quiet period" which specifies a time window in which
    // _no requests_ must be witnessed before shutdown is _initiated_. Here we want to
    // start immediately, so no quiet period -- by default it's 2 seconds.
    // Moreover, there's also a "timeout" parameter
    // which caps the time to wait for the quiet period to be fullfilled. Since we have
    // no quiet period, this can also be 0.
    // See <https://netty.io/4.1/api/io/netty/util/concurrent/EventExecutorGroup.html#shutdownGracefully-long-long-java.util.concurrent.TimeUnit->.
    // The 10 seconds to wait is sort of arbitrary, it's long enough to be noticeable though.
    Option(serverEventLoopGroup).foreach(
      _.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS).await(10L, TimeUnit.SECONDS))
    Option(serverEsf).foreach(_.close())
  }
}
