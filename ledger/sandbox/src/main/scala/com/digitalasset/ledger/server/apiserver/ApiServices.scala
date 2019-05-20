// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.server.apiserver

import akka.NotUsed
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v1.ConfigurationService
import com.daml.ledger.participant.state.v1.{Configuration, WriteService}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.{Engine, EngineInfo}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionEndRequest
import com.digitalasset.ledger.backend.api.v1.LedgerBackend
import com.digitalasset.ledger.client.services.commands.CommandSubmissionFlow
import com.digitalasset.platform.sandbox.config.{SandboxConfig, SandboxContext}
import com.digitalasset.platform.sandbox.services._
import com.digitalasset.platform.sandbox.services.transaction.SandboxTransactionService
import com.digitalasset.platform.sandbox.stores.ledger.CommandExecutorImpl
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.digitalasset.platform.server.services.command.ReferenceCommandService
import com.digitalasset.platform.server.services.identity.LedgerIdentityServiceImpl
import com.digitalasset.platform.server.services.testing.{ReferenceTimeService, TimeServiceBackend}
import com.digitalasset.platform.services.time.TimeProviderType
import io.grpc.BindableService
import io.grpc.protobuf.services.ProtoReflectionService
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future, Promise}

trait ApiServices extends AutoCloseable {
  val services: Iterable[BindableService]

  def withServices(otherServices: immutable.Seq[BindableService]): ApiServices
}

private case class ApiServicesBundle(services: immutable.Seq[BindableService]) extends ApiServices {

  override def close(): Unit =
    services.foreach {
      case closeable: AutoCloseable => closeable.close()
      case _ => ()
    }

  override def withServices(otherServices: immutable.Seq[BindableService]): ApiServices =
    copy(services = services.++:(otherServices))

}

object ApiServices {

  private val logger = LoggerFactory.getLogger(this.getClass)

  //TODO: this is here only temporarily
  def configurationService(config: SandboxConfig) = new ConfigurationService {
    override def getLedgerConfiguration(): Source[Configuration, NotUsed] =
      Source
        .single(Configuration(config.timeModel))
        .concat(Source.fromFuture(Promise[Configuration]().future)) // we should keep the stream open!

  }

  def create(
      config: SandboxConfig,
      ledgerBackend: LedgerBackend,
      writeService: WriteService,
      configService: ConfigurationService,
      engine: Engine,
      timeProvider: TimeProvider,
      optTimeServiceBackend: Option[TimeServiceBackend])(
      implicit mat: ActorMaterializer,
      esf: ExecutionSequencerFactory): ApiServices = {

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
        writeService,
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
      LedgerConfigurationService.createApiService(configService, ledgerBackend.ledgerId)

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
        () => completionService.completionEnd(CompletionEndRequest(ledgerBackend.ledgerId)),
        transactionService.getTransactionById,
        transactionService.getFlatTransactionById
      ),
      identifierResolver
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

    new ApiServicesBundle(
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
      ))
  }
}
