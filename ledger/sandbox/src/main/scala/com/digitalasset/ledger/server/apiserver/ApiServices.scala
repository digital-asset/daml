// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.server.apiserver

import akka.NotUsed
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{
  ActiveContractsService,
  ConfigurationService,
  IdentityService,
  PackagesService
}
import com.daml.ledger.participant.state.index.v2._
import com.daml.ledger.participant.state.v1.WriteService
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.{Engine, EngineInfo}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
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
import scalaz.syntax.tag._

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
    override def getLedgerConfiguration(): Source[LedgerConfiguration, NotUsed] =
      Source
        .single(LedgerConfiguration(config.timeModel.minTtl, config.timeModel.maxTtl))
        .concat(Source.fromFuture(Promise[LedgerConfiguration]().future)) // we should keep the stream open!
  }

  def identityService(ledgerId: LedgerId): IdentityService =
    () => Future.successful(ledgerId)

  def create(
      config: SandboxConfig,
      ledgerBackend: LedgerBackend, //eventually this should not be needed!
      writeService: WriteService,
      configurationService: ConfigurationService,
      identityService: IdentityService,
      packagesService: PackagesService,
      activeContractsService: ActiveContractsService,
      engine: Engine,
      timeProvider: TimeProvider,
      optTimeServiceBackend: Option[TimeServiceBackend])(
      implicit mat: ActorMaterializer,
      esf: ExecutionSequencerFactory): Future[ApiServices] = {

    implicit val ec: ExecutionContext = mat.system.dispatcher

    identityService.getLedgerId().map { ledgerId =>
      val context = SandboxContext.fromConfig(config)

      val packageResolver =
        (pkgId: Ref.PackageId) => Future.successful(context.packageContainer.getPackage(pkgId))

      val identifierResolver: IdentifierResolver = new IdentifierResolver(packageResolver)

      val apiSubmissionService =
        SandboxSubmissionService.createApiService(
          ledgerId,
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
        SandboxTransactionService.createApiService(ledgerId, ledgerBackend, identifierResolver)

      val apiLedgerIdentityService = LedgerIdentityServiceImpl(() => identityService.getLedgerId())

      val apiPackageService = SandboxPackageService(ledgerId, packagesService)

      val apiConfigurationService =
        LedgerConfigurationService.createApiService(ledgerId, configurationService)

      val completionService =
        SandboxCommandCompletionService(ledgerId, ledgerBackend)

      val apiCommandService = ReferenceCommandService(
        ReferenceCommandService.Configuration(
          ledgerId,
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
            apiSubmissionService.submit,
            config.commandConfig.maxParallelSubmissions),
          r =>
            completionService.service
              .asInstanceOf[SandboxCommandCompletionService]
              .completionStreamSource(r),
          () => completionService.completionEnd(CompletionEndRequest(ledgerId.unwrap)),
          transactionService.getTransactionById,
          transactionService.getFlatTransactionById
        ),
        identifierResolver
      )

      val apiActiveContractsService =
        SandboxActiveContractsService(ledgerId, activeContractsService, identifierResolver)

      val reflectionService = ProtoReflectionService.newInstance()

      val timeServiceOpt =
        optTimeServiceBackend.map { tsb =>
          ReferenceTimeService(
            ledgerId,
            tsb,
            config.timeProviderType == TimeProviderType.StaticAllowBackwards
          )
        }

      new ApiServicesBundle(
        timeServiceOpt.toList :::
          List(
          apiLedgerIdentityService,
          apiPackageService,
          apiConfigurationService,
          apiSubmissionService,
          transactionService,
          completionService,
          apiCommandService,
          apiActiveContractsService,
          reflectionService
        ))
    }
  }

}
