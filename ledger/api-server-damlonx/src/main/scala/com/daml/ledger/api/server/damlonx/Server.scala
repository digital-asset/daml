// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx

import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.stream.ActorMaterializer
import com.daml.ledger.api.server.damlonx.services._
import com.daml.ledger.participant.state.index.v1.IndexService
import com.daml.ledger.participant.state.v1.WriteService
import com.digitalasset.daml.lf.engine.{Engine, EngineInfo}
import com.digitalasset.daml.lf.lfpackage.Decode
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionEndRequest
import com.digitalasset.ledger.client.services.commands.CommandSubmissionFlow
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.digitalasset.platform.server.services.command.ReferenceCommandService
import com.digitalasset.platform.server.services.identity.LedgerIdentityService
import io.grpc.BindableService
import io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.ssl.SslContext
import io.netty.util.concurrent.DefaultThreadFactory
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object Server {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def apply(
      serverPort: Int,
      sslContext: Option[SslContext],
      indexService: IndexService,
      writeService: WriteService)(implicit materializer: ActorMaterializer): Server = {
    implicit val serverEsf: AkkaExecutionSequencerPool =
      new AkkaExecutionSequencerPool(
        // NOTE(JM): Pick a unique pool name as we want to allow multiple ledger api server
        // instances, and it's pretty difficult to wait for the name to become available
        // again (the name deregistration is asynchronous and the close method is not waiting for
        // it, and it isn't trivial to implement).
        // https://doc.akka.io/docs/akka/2.5/actors.html#graceful-stop
        s"api-server-damlonx-rs-grpc-bridge-${UUID.randomUUID}",
        Runtime.getRuntime.availableProcessors() * 8
      )(materializer.system)

    new Server(
      serverEsf,
      serverPort,
      sslContext,
      createServices(indexService, writeService),
    )
  }

  private def createIdentifierResolver(indexService: IndexService)(
      implicit ec: ExecutionContext): IdentifierResolver = {
    IdentifierResolver(
      pkgId =>
        indexService
          .getPackage(pkgId)
          .map(optArchive => optArchive.map(Decode.decodeArchive(_)._2))
    )
  }

  private def createServices(indexService: IndexService, writeService: WriteService)(
      implicit mat: ActorMaterializer,
      serverEsf: ExecutionSequencerFactory): List[BindableService] = {
    implicit val ec: ExecutionContext = mat.system.dispatcher

    // FIXME(JM): Any point in keeping getLedgerId async?
    val ledgerId = Await.result(
      indexService.getLedgerId(),
      10.seconds
    )
    val identifierResolver = createIdentifierResolver(indexService)
    val engine = Engine()
    logger.info(EngineInfo.show)

    val submissionService =
      DamlOnXSubmissionService.create(
        identifierResolver,
        ledgerId,
        indexService,
        writeService,
        engine)

    val commandCompletionService =
      DamlOnXCommandCompletionService.create(indexService)

    val activeContractsService =
      DamlOnXActiveContractsService.create(indexService, identifierResolver)

    val transactionService =
      DamlOnXTransactionService.create(ledgerId, indexService, identifierResolver)

    val identityService =
      LedgerIdentityService.createApiService(() => Future.successful(domain.LedgerId(ledgerId)))

    // FIXME(JM): hard-coded values copied from SandboxConfig.
    val commandService = ReferenceCommandService.createApiService(
      ReferenceCommandService.Configuration(
        domain.LedgerId(ledgerId),
        512, // config.commandConfig.inputBufferSize,
        128, // config.commandConfig.maxParallelSubmissions,
        256, // config.commandConfig.maxCommandsInFlight,
        true, // config.commandConfig.limitMaxCommandsInFlight,
        5000, // config.commandConfig.historySize,
        24.hours, // config.commandConfig.retentionPeriod,
        20.seconds // config.commandConfig.commandTtl
      ),
      // Using local services skips the gRPC layer, improving performance.
      ReferenceCommandService.LowLevelCommandServiceAccess.LocalServices(
        CommandSubmissionFlow(
          submissionService.submit,
          128 /* config.commandConfig.maxParallelSubmissions */ ),
        r =>
          commandCompletionService.service
            .asInstanceOf[DamlOnXCommandCompletionService]
            .completionStreamSource(r),
        () => commandCompletionService.completionEnd(CompletionEndRequest(ledgerId)),
        transactionService.getTransactionById,
        transactionService.getFlatTransactionById
      ),
      identifierResolver
    )

    val packageService = DamlOnXPackageService(indexService, ledgerId)

    val timeService = new DamlOnXTimeService(indexService)

    val configurationService = DamlOnXLedgerConfigurationService(
      indexService
    )

    val reflectionService = ProtoReflectionService.newInstance()

    List(
      submissionService,
      commandCompletionService,
      activeContractsService,
      transactionService,
      commandService,
      identityService,
      packageService,
      timeService,
      configurationService,
      reflectionService
    )
  }
}

final class Server private (
    serverEsf: AkkaExecutionSequencerPool,
    serverPort: Int,
    sslContext: Option[SslContext],
    services: Iterable[BindableService])(implicit materializer: ActorMaterializer)
    extends AutoCloseable {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val serverEventLoopGroup: NioEventLoopGroup = {
    val threadFactory =
      new DefaultThreadFactory(s"api-server-damlonx-grpc-eventloop-${UUID.randomUUID}", true)
    val parallelism = Runtime.getRuntime.availableProcessors
    new NioEventLoopGroup(parallelism, threadFactory)
  }

  private val server: io.grpc.Server = {
    val builder = NettyServerBuilder
      .forPort(serverPort)
      .directExecutor()
      .workerEventLoopGroup(serverEventLoopGroup)
      .permitKeepAliveTime(10, TimeUnit.SECONDS)
      .permitKeepAliveWithoutCalls(true)

    sslContext
      .fold {
        logger.info("Starting plainText server")
      } { sslContext =>
        logger.info("Starting TLS server")
        val _ = builder.sslContext(sslContext)
      }

    val server = services.foldLeft(builder)(_ addService _).build

    // FIXME(JM): catch bind exceptions?
    server.start()

    logger.info(s"listening on localhost:${server.getPort}")

    server
  }

  lazy val port = server.getPort

  override def close(): Unit = {
    logger.info("Shutting down server...")
    server.shutdownNow()
    assume(server.awaitTermination(10, TimeUnit.SECONDS))
    serverEventLoopGroup
      .shutdownGracefully(0, 0, TimeUnit.SECONDS)
      .await(10, TimeUnit.SECONDS)
    serverEsf.close()
  }
}
