// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.auth.services._
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.health.HealthChecks
import com.daml.ledger.client.services.commands.CommandSubmissionFlow
import com.daml.ledger.participant.state.index.v2._
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.lf.engine._
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.configuration.{
  LedgerConfigurationInitializer,
  LedgerConfigurationSubscription,
}
import com.daml.platform.apiserver.execution.{
  LedgerTimeAwareCommandExecutor,
  StoreBackedCommandExecutor,
  TimedCommandExecutor,
}
import com.daml.platform.apiserver.services._
import com.daml.platform.apiserver.services.admin._
import com.daml.platform.apiserver.services.transaction.ApiTransactionService
import com.daml.platform.configuration.{
  CommandConfiguration,
  InitialLedgerConfiguration,
  PartyConfiguration,
}
import com.daml.platform.server.api.services.domain.CommandCompletionService
import com.daml.platform.server.api.services.grpc.{GrpcHealthService, GrpcTransactionService}
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.usermanagement.UserManagementConfig
import com.daml.telemetry.TelemetryContext
import io.grpc.BindableService
import io.grpc.protobuf.services.ProtoReflectionService

import scala.collection.immutable
import scala.concurrent.duration.{Duration, FiniteDuration}
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
      optWriteService: Option[state.WriteService],
      indexService: IndexService,
      userManagementStore: UserManagementStore,
      authorizer: Authorizer,
      engine: Engine,
      timeProvider: TimeProvider,
      timeProviderType: TimeProviderType,
      configurationLoadTimeout: Duration,
      initialLedgerConfiguration: Option[InitialLedgerConfiguration],
      commandConfig: CommandConfiguration,
      partyConfig: PartyConfiguration,
      optTimeServiceBackend: Option[TimeServiceBackend],
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      healthChecks: HealthChecks,
      seedService: SeedService,
      managementServiceTimeout: FiniteDuration,
      checkOverloaded: TelemetryContext => Option[state.SubmissionResult],
      ledgerFeatures: LedgerFeatures,
      userManagementConfig: UserManagementConfig,
      apiStreamShutdownTimeout: scala.concurrent.duration.Duration,
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
    private val meteringStore: MeteringStore = indexService

    private val configurationInitializer = new LedgerConfigurationInitializer(
      indexService = indexService,
      optWriteService = optWriteService,
      timeProvider = timeProvider,
      materializer = materializer,
      servicesExecutionContext = servicesExecutionContext,
    )

    override def acquire()(implicit context: ResourceContext): Resource[ApiServices] = {
      logger.info(engine.info.toString)
      for {
        currentLedgerConfiguration <- configurationInitializer.initialize(
          initialLedgerConfiguration = initialLedgerConfiguration,
          configurationLoadTimeout = configurationLoadTimeout,
        )
        services <- Resource(
          Future(
            createServices(identityService.ledgerId, currentLedgerConfiguration, checkOverloaded)(
              servicesExecutionContext
            )
          )
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
        ledgerConfigurationSubscription: LedgerConfigurationSubscription,
        checkOverloaded: TelemetryContext => Option[state.SubmissionResult],
    )(implicit executionContext: ExecutionContext): List[BindableService] = {
      val apiTransactionService =
        ApiTransactionService.create(ledgerId, transactionsService, metrics)

      val apiLedgerIdentityService =
        ApiLedgerIdentityService.create(ledgerId)

      val apiVersionService =
        ApiVersionService.create(
          ledgerFeatures,
          userManagementConfig = userManagementConfig,
        )

      val apiPackageService =
        ApiPackageService.create(ledgerId, packagesService)

      val apiConfigurationService =
        ApiLedgerConfigurationService.create(ledgerId, configurationService)

      val (completionService, grpcCompletionService) =
        ApiCommandCompletionService.create(
          ledgerId,
          completionsService,
          metrics,
        )

      val apiActiveContractsService =
        ApiActiveContractsService.create(
          ledgerId,
          activeContractsService,
          metrics,
        )

      val apiTimeServiceOpt =
        optTimeServiceBackend.map(tsb =>
          new TimeServiceAuthorization(
            ApiTimeService.create(ledgerId, tsb, apiStreamShutdownTimeout),
            authorizer,
          )
        )
      val writeServiceBackedApiServices =
        intitializeWriteServiceBackedApiServices(
          ledgerId,
          ledgerConfigurationSubscription,
          completionService,
          apiTransactionService,
          checkOverloaded,
        )

      val apiReflectionService = ProtoReflectionService.newInstance()

      val apiHealthService = new GrpcHealthService(healthChecks)

      val maybeApiUserManagementService: Option[UserManagementServiceAuthorization] =
        if (userManagementConfig.enabled) {
          val apiUserManagementService =
            new ApiUserManagementService(
              userManagementStore,
              maxUsersPageSize = userManagementConfig.maxUsersPageSize,
              submissionIdGenerator = SubmissionIdGenerator.Random,
            )
          val authorized =
            new UserManagementServiceAuthorization(apiUserManagementService, authorizer)
          Some(authorized)
        } else {
          None
        }

      val apiMeteringReportService =
        new ApiMeteringReportService(participantId, meteringStore)

      apiTimeServiceOpt.toList :::
        writeServiceBackedApiServices :::
        List(
          new LedgerIdentityServiceAuthorization(apiLedgerIdentityService, authorizer),
          new PackageServiceAuthorization(apiPackageService, authorizer),
          new LedgerConfigurationServiceAuthorization(apiConfigurationService, authorizer),
          new TransactionServiceAuthorization(apiTransactionService, authorizer),
          new CommandCompletionServiceAuthorization(grpcCompletionService, authorizer),
          new ActiveContractsServiceAuthorization(apiActiveContractsService, authorizer),
          apiReflectionService,
          apiHealthService,
          apiVersionService,
          new MeteringReportServiceAuthorization(apiMeteringReportService, authorizer),
        ) ::: maybeApiUserManagementService.toList
    }

    private def intitializeWriteServiceBackedApiServices(
        ledgerId: LedgerId,
        ledgerConfigurationSubscription: LedgerConfigurationSubscription,
        apiCompletionService: CommandCompletionService,
        apiTransactionService: GrpcTransactionService,
        checkOverloaded: TelemetryContext => Option[state.SubmissionResult],
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
          partyManagementService,
          timeProvider,
          timeProviderType,
          ledgerConfigurationSubscription,
          seedService,
          commandExecutor,
          checkOverloaded,
          ApiSubmissionService.Configuration(
            partyConfig.implicitPartyAllocation
          ),
          metrics,
        )

        // Note: the command service uses the command submission, command completion, and transaction
        // services internally. These connections do not use authorization, authorization wrappers are
        // only added here to all exposed services.
        val apiCommandService = ApiCommandService.create(
          configuration = ApiCommandService.Configuration(
            ledgerId,
            commandConfig.inputBufferSize,
            commandConfig.maxCommandsInFlight,
            commandConfig.trackerRetentionPeriod,
          ),
          // Using local services skips the gRPC layer, improving performance.
          submissionFlow =
            CommandSubmissionFlow(apiSubmissionService.submit, commandConfig.maxCommandsInFlight),
          completionServices = apiCompletionService,
          transactionServices = new ApiCommandService.TransactionServices(
            getTransactionById = apiTransactionService.getTransactionById,
            getFlatTransactionById = apiTransactionService.getFlatTransactionById,
          ),
          timeProvider = timeProvider,
          ledgerConfigurationSubscription = ledgerConfigurationSubscription,
          metrics = metrics,
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
        )

        val apiParticipantPruningService =
          ApiParticipantPruningService.createApiService(
            indexService,
            writeService,
          )

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
