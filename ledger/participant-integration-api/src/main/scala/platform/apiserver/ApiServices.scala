// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.time.Duration

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
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.lf.engine._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{ParticipantMetrics => Metrics}
import com.daml.platform.apiserver.execution.{
  LedgerTimeAwareCommandExecutor,
  StoreBackedCommandExecutor,
  TimedCommandExecutor,
}
import com.daml.platform.apiserver.services._
import com.daml.platform.apiserver.services.admin.{
  ApiConfigManagementService,
  ApiPackageManagementService,
  ApiParticipantPruningService,
  ApiPartyManagementService,
}
import com.daml.platform.apiserver.services.transaction.ApiTransactionService
import com.daml.platform.configuration.{
  CommandConfiguration,
  LedgerConfiguration,
  PartyConfiguration,
}
import com.daml.platform.server.api.services.grpc.{
  GrpcCommandCompletionService,
  GrpcHealthService,
  GrpcTransactionService,
}
import com.daml.platform.services.time.TimeProviderType
import io.grpc.BindableService
import io.grpc.protobuf.services.ProtoReflectionService
import scalaz.syntax.tag._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

private[daml] trait ApiServices {
  val services: Iterable[BindableService]

  def withServices(otherServices: immutable.Seq[BindableService]): ApiServices
}

private case class ApiServicesBundle(services: immutable.Seq[BindableService]) extends ApiServices {

  override def withServices(otherServices: immutable.Seq[BindableService]): ApiServices =
    copy(services = services ++ otherServices)

}

private[daml] object ApiServices {

  private val logger = ContextualizedLogger.get(this.getClass)

  final class Owner(
      participantId: Ref.ParticipantId,
      optWriteService: Option[WriteService],
      indexService: IndexService,
      authorizer: Authorizer,
      engine: Engine,
      timeProvider: TimeProvider,
      timeProviderType: TimeProviderType,
      ledgerConfiguration: LedgerConfiguration,
      commandConfig: CommandConfiguration,
      partyConfig: PartyConfiguration,
      optTimeServiceBackend: Option[TimeServiceBackend],
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      healthChecks: HealthChecks,
      seedService: SeedService,
      managementServiceTimeout: Duration,
  )(implicit
      materializer: Materializer,
      esf: ExecutionSequencerFactory,
      loggingContext: LoggingContext,
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

    override def acquire()(implicit context: ResourceContext): Resource[ApiServices] = {
      logger.info(engine.info.toString)

      for {
        ledgerId <- Resource.fromFuture(indexService.getLedgerId())
        ledgerConfigProvider <- LedgerConfigProvider
          .owner(
            indexService,
            optWriteService,
            timeProvider,
            ledgerConfiguration,
          )(materializer, loggingContext)
          .acquire()
        services <- Resource(
          Future(createServices(ledgerId, ledgerConfigProvider)(servicesExecutionContext))
        )(services =>
          Future {
            services.foreach {
              case closeable: AutoCloseable => closeable.close()
              case _ => ()
            }
          }
        )
      } yield ApiServicesBundle(services)
    }

    private def createServices(
        ledgerId: LedgerId,
        ledgerConfigProvider: LedgerConfigProvider,
    )(implicit executionContext: ExecutionContext): List[BindableService] = {
      val apiTransactionService =
        ApiTransactionService.create(ledgerId, transactionsService, metrics)

      val apiLedgerIdentityService =
        ApiLedgerIdentityService.create(() => identityService.getLedgerId())

      val apiVersionService =
        ApiVersionService.create()

      val apiPackageService = ApiPackageService.create(ledgerId, packagesService)

      val apiConfigurationService =
        ApiLedgerConfigurationService.create(ledgerId, configurationService)

      val apiCompletionService =
        ApiCommandCompletionService.create(ledgerId, completionsService, metrics)

      val apiActiveContractsService =
        ApiActiveContractsService.create(ledgerId, activeContractsService, metrics)

      val apiTimeServiceOpt =
        optTimeServiceBackend.map(tsb =>
          new TimeServiceAuthorization(ApiTimeService.create(ledgerId, tsb), authorizer)
        )

      val writeServiceBackedApiServices =
        intitializeWriteServiceBackedApiServices(
          ledgerId,
          ledgerConfigProvider,
          apiCompletionService,
          apiTransactionService,
        )

      val apiReflectionService = ProtoReflectionService.newInstance()

      val apiHealthService = new GrpcHealthService(healthChecks)

      apiTimeServiceOpt.toList :::
        writeServiceBackedApiServices :::
        List(
          new LedgerIdentityServiceAuthorization(apiLedgerIdentityService, authorizer),
          new PackageServiceAuthorization(apiPackageService, authorizer),
          new LedgerConfigurationServiceAuthorization(apiConfigurationService, authorizer),
          new TransactionServiceAuthorization(apiTransactionService, authorizer),
          new CommandCompletionServiceAuthorization(apiCompletionService, authorizer),
          new ActiveContractsServiceAuthorization(apiActiveContractsService, authorizer),
          apiReflectionService,
          apiHealthService,
          apiVersionService,
        )
    }

    private def intitializeWriteServiceBackedApiServices(
        ledgerId: LedgerId,
        ledgerConfigProvider: LedgerConfigProvider,
        apiCompletionService: GrpcCommandCompletionService,
        apiTransactionService: GrpcTransactionService,
    )(implicit executionContext: ExecutionContext): List[BindableService] = {
      optWriteService.toList.flatMap { writeService =>
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
          writeService,
          submissionService,
          partyManagementService,
          timeProvider,
          timeProviderType,
          ledgerConfigProvider,
          seedService,
          commandExecutor,
          ApiSubmissionService.Configuration(
            partyConfig.implicitPartyAllocation
          ),
          metrics,
        )

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
            apiTransactionService.getFlatTransactionById,
          ),
          timeProvider,
          ledgerConfigProvider,
          metrics,
        )

        val apiPartyManagementService = ApiPartyManagementService.createApiService(
          partyManagementService,
          transactionsService,
          writeService,
          managementServiceTimeout,
        )

        val apiPackageManagementService = ApiPackageManagementService.createApiService(
          indexService,
          transactionsService,
          writeService,
          managementServiceTimeout,
          engine,
        )

        val apiConfigManagementService = ApiConfigManagementService.createApiService(
          configManagementService,
          writeService,
          timeProvider,
          ledgerConfiguration,
        )

        val apiParticipantPruningService =
          ApiParticipantPruningService.createApiService(indexService, writeService)

        List(
          new CommandSubmissionServiceAuthorization(apiSubmissionService, authorizer),
          new CommandServiceAuthorization(apiCommandService, authorizer),
          new PartyManagementServiceAuthorization(apiPartyManagementService, authorizer),
          new PackageManagementServiceAuthorization(apiPackageManagementService, authorizer),
          new ConfigManagementServiceAuthorization(apiConfigManagementService, authorizer),
          new ParticipantPruningServiceAuthorization(apiParticipantPruningService, authorizer),
        )
      }
    }
  }

}
