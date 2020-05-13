// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.auth.services._
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.api.v1.command_completion_service.CompletionEndRequest
import com.daml.ledger.client.services.commands.CommandSubmissionFlow
import com.daml.ledger.participant.state.index.v2._
import com.daml.ledger.participant.state.v1.{SeedService, WriteService}
import com.daml.lf.data.Ref
import com.daml.lf.engine._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.execution.{
  LedgerTimeAwareCommandExecutor,
  StoreBackedCommandExecutor,
  TimedCommandExecutor
}
import com.daml.platform.apiserver.services.admin.{
  ApiConfigManagementService,
  ApiPackageManagementService,
  ApiPartyManagementService
}
import com.daml.platform.apiserver.services.transaction.ApiTransactionService
import com.daml.platform.apiserver.services._
import com.daml.platform.configuration.{
  CommandConfiguration,
  LedgerConfiguration,
  PartyConfiguration
}
import com.daml.platform.server.api.services.grpc.GrpcHealthService
import com.daml.platform.services.time.TimeProviderType
import com.daml.resources.{Resource, ResourceOwner}
import io.grpc.BindableService
import io.grpc.protobuf.services.ProtoReflectionService
import scalaz.syntax.tag._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

trait ApiServices {
  val services: Iterable[BindableService]

  def withServices(otherServices: immutable.Seq[BindableService]): ApiServices
}

private case class ApiServicesBundle(services: immutable.Seq[BindableService]) extends ApiServices {

  override def withServices(otherServices: immutable.Seq[BindableService]): ApiServices =
    copy(services = services ++ otherServices)

}

object ApiServices {

  private val logger = ContextualizedLogger.get(this.getClass)

  class Owner(
      participantId: Ref.ParticipantId,
      writeService: WriteService,
      indexService: IndexService,
      authorizer: Authorizer,
      engine: Engine,
      timeProvider: TimeProvider,
      timeProviderType: TimeProviderType,
      ledgerConfiguration: LedgerConfiguration,
      commandConfig: CommandConfiguration,
      partyConfig: PartyConfiguration,
      optTimeServiceBackend: Option[TimeServiceBackend],
      metrics: Metrics,
      healthChecks: HealthChecks,
      seedService: SeedService
  )(
      implicit mat: Materializer,
      esf: ExecutionSequencerFactory,
      logCtx: LoggingContext,
  ) extends ResourceOwner[ApiServices] {
    private val configurationService: IndexConfigurationService = indexService
    private val identityService: IdentityProvider = indexService
    private val packagesService: IndexPackagesService = indexService
    private val activeContractsService: IndexActiveContractsService = indexService
    private val transactionsService: IndexTransactionsService = indexService
    private val contractStore: ContractStore = indexService
    private val completionsService: IndexCompletionsService = indexService
    private val partyManagementService: IndexPartyManagementService = indexService
    private val configManagementService: IndexConfigManagementService = indexService
    private val submissionService: IndexSubmissionService = indexService

    override def acquire()(implicit executionContext: ExecutionContext): Resource[ApiServices] =
      Resource(
        for {
          ledgerId <- identityService.getLedgerId()
          ledgerConfigProvider = LedgerConfigProvider.create(
            configManagementService,
            writeService,
            timeProvider,
            ledgerConfiguration)
          services = createServices(ledgerId, ledgerConfigProvider)(mat.system.dispatcher)
          _ <- ledgerConfigProvider.ready
        } yield (ledgerConfigProvider, services)
      ) {
        case (ledgerConfigProvider, services) =>
          Future {
            services.foreach {
              case closeable: AutoCloseable => closeable.close()
              case _ => ()
            }
            ledgerConfigProvider.close()
          }
      }.map(x => ApiServicesBundle(x._2))

    private def createServices(ledgerId: LedgerId, ledgerConfigProvider: LedgerConfigProvider)(
        implicit executionContext: ExecutionContext): List[BindableService] = {
      val commandExecutor = new TimedCommandExecutor(
        new LedgerTimeAwareCommandExecutor(
          new StoreBackedCommandExecutor(
            engine,
            participantId,
            packagesService,
            contractStore,
            metrics,
          ),
          contractStore,
          maxRetries = 3,
          metrics,
        ),
        metrics,
      )
      val apiSubmissionService = ApiSubmissionService.create(
        ledgerId,
        contractStore,
        writeService,
        submissionService,
        partyManagementService,
        timeProvider,
        timeProviderType,
        ledgerConfigProvider,
        seedService,
        commandExecutor,
        ApiSubmissionService.Configuration(
          partyConfig.implicitPartyAllocation,
        ),
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

      // Note: the command service uses the command submission, command completion, and transaction
      // services internally. These connections do not use authorization, authorization wrappers are
      // only added here to all exposed services.
      val apiCommandService = ApiCommandService.create(
        ApiCommandService.Configuration(
          ledgerId,
          commandConfig.inputBufferSize,
          commandConfig.maxCommandsInFlight,
          commandConfig.limitMaxCommandsInFlight,
          commandConfig.retentionPeriod,
        ),
        // Using local services skips the gRPC layer, improving performance.
        ApiCommandService.LocalServices(
          CommandSubmissionFlow(apiSubmissionService.submit, commandConfig.maxCommandsInFlight),
          r => apiCompletionService.completionStreamSource(r),
          () => apiCompletionService.completionEnd(CompletionEndRequest(ledgerId.unwrap)),
          apiTransactionService.getTransactionById,
          apiTransactionService.getFlatTransactionById
        ),
        timeProvider,
        ledgerConfigProvider,
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
            ledgerConfiguration)

      val apiReflectionService = ProtoReflectionService.newInstance()

      val apiHealthService = new GrpcHealthService(healthChecks)

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
      )
    }
  }
}
