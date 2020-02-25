// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2._
import com.daml.ledger.participant.state.v1.{Configuration, WriteService}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine._
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.auth.Authorizer
import com.digitalasset.ledger.api.auth.services._
import com.digitalasset.ledger.api.health.HealthChecks
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionEndRequest
import com.digitalasset.ledger.client.services.commands.CommandSubmissionFlow
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.apiserver.services.admin.{
  ApiConfigManagementService,
  ApiPackageManagementService,
  ApiPartyManagementService
}
import com.digitalasset.platform.apiserver.services.transaction.ApiTransactionService
import com.digitalasset.platform.apiserver.services.{
  ApiActiveContractsService,
  ApiCommandCompletionService,
  ApiCommandService,
  ApiLedgerConfigurationService,
  ApiLedgerIdentityService,
  ApiPackageService,
  ApiSubmissionService,
  ApiTimeService
}
import com.digitalasset.platform.configuration.CommandConfiguration
import com.digitalasset.platform.server.api.services.grpc.GrpcHealthService
import io.grpc.BindableService
import io.grpc.protobuf.services.ProtoReflectionService
import scalaz.syntax.tag._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

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
    copy(services = services ++ otherServices)

}

object ApiServices {

  private val logger = ContextualizedLogger.get(this.getClass)

  def create(
      participantId: Ref.ParticipantId,
      writeService: WriteService,
      indexService: IndexService,
      authorizer: Authorizer,
      engine: Engine,
      timeProvider: TimeProvider,
      defaultLedgerConfiguration: Configuration,
      commandConfig: CommandConfiguration,
      optTimeServiceBackend: Option[TimeServiceBackend],
      metrics: MetricRegistry,
      healthChecks: HealthChecks,
      seedService: Option[() => crypto.Hash]
  )(
      implicit mat: Materializer,
      esf: ExecutionSequencerFactory,
      logCtx: LoggingContext): Future[ApiServices] = {
    implicit val ec: ExecutionContext = mat.system.dispatcher

    // still trying to keep it tidy in case we want to split it later
    val configurationService: IndexConfigurationService = indexService
    val identityService: IdentityProvider = indexService
    val packagesService: IndexPackagesService = indexService
    val activeContractsService: IndexActiveContractsService = indexService
    val transactionsService: IndexTransactionsService = indexService
    val contractStore: ContractStore = indexService
    val completionsService: IndexCompletionsService = indexService
    val partyManagementService: IndexPartyManagementService = indexService
    val configManagementService: IndexConfigManagementService = indexService

    identityService.getLedgerId().map { ledgerId =>
      val apiSubmissionService =
        ApiSubmissionService.create(
          ledgerId,
          contractStore,
          writeService,
          defaultLedgerConfiguration.timeModel,
          timeProvider,
          new CommandExecutorImpl(engine, packagesService.getLfPackage, participantId, seedService),
          metrics,
        )

      logger.info(EngineInfo.show)

      val apiTransactionService =
        ApiTransactionService.create(ledgerId, transactionsService)

      val apiLedgerIdentityService =
        ApiLedgerIdentityService.create(() => identityService.getLedgerId())

      val apiPackageService = ApiPackageService.create(ledgerId, packagesService)

      val apiConfigurationService =
        ApiLedgerConfigurationService.create(ledgerId, configurationService)

      val apiCompletionService =
        ApiCommandCompletionService.create(ledgerId, completionsService)

      val apiCommandService = ApiCommandService.create(
        ApiCommandService.Configuration(
          ledgerId,
          commandConfig.inputBufferSize,
          commandConfig.maxParallelSubmissions,
          commandConfig.maxCommandsInFlight,
          commandConfig.limitMaxCommandsInFlight,
          commandConfig.historySize,
          commandConfig.retentionPeriod,
          commandConfig.commandTtl
        ),
        // Using local services skips the gRPC layer, improving performance.
        ApiCommandService.LowLevelCommandServiceAccess.LocalServices(
          CommandSubmissionFlow(apiSubmissionService.submit, commandConfig.maxParallelSubmissions),
          r => apiCompletionService.completionStreamSource(r),
          () => apiCompletionService.completionEnd(CompletionEndRequest(ledgerId.unwrap)),
          apiTransactionService.getTransactionById,
          apiTransactionService.getFlatTransactionById
        )
      )

      val apiActiveContractsService =
        ApiActiveContractsService.create(ledgerId, activeContractsService)

      val apiTimeServiceOpt =
        optTimeServiceBackend.map { tsb =>
          new TimeServiceAuthorization(
            ApiTimeService.create(
              ledgerId,
              tsb,
            ),
            authorizer
          )
        }

      val apiPartyManagementService =
        ApiPartyManagementService
          .createApiService(partyManagementService, transactionsService, writeService)

      val apiPackageManagementService =
        ApiPackageManagementService
          .createApiService(indexService, transactionsService, writeService, timeProvider)

      val apiConfigManagementService =
        ApiConfigManagementService
          .createApiService(
            configManagementService,
            writeService,
            timeProvider,
            defaultLedgerConfiguration)

      val apiReflectionService = ProtoReflectionService.newInstance()

      val apiHealthService = new GrpcHealthService(healthChecks)

      // Note: the command service uses the command submission, command completion, and transaction services internally.
      // These connections do not use authorization, authorization wrappers are only added here to all exposed services.
      ApiServicesBundle(
        apiTimeServiceOpt.toList :::
          List(
          new LedgerIdentityServiceAuthorization(apiLedgerIdentityService, authorizer),
          new PackageServiceAuthorization(apiPackageService, authorizer),
          new LedgerConfigurationServiceAuthorization(apiConfigurationService, authorizer),
          new CommandSubmissionServiceAuthorization(apiSubmissionService, authorizer),
          new TransactionServiceAuthorization(apiTransactionService, authorizer),
          new CommandCompletionServiceAuthorization(apiCompletionService, authorizer),
          new CommandServiceAuthorization(apiCommandService, authorizer),
          new ActiveContractsServiceAuthorization(apiActiveContractsService, authorizer),
          new PartyManagementServiceAuthorization(apiPartyManagementService, authorizer),
          new PackageManagementServiceAuthorization(apiPackageManagementService, authorizer),
          new ConfigManagementServiceAuthorization(apiConfigManagementService, authorizer),
          apiReflectionService,
          apiHealthService,
        ))
    }
  }
}
