// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.Authorizer
import com.digitalasset.canton.config
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.api.auth.services.*
import com.digitalasset.canton.ledger.api.grpc.GrpcHealthService
import com.digitalasset.canton.ledger.api.health.HealthChecks
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.api.validation.*
import com.digitalasset.canton.ledger.localstore.api.{
  IdentityProviderConfigStore,
  PartyRecordStore,
  UserManagementStore,
}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.index.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.platform.apiserver.execution.*
import com.digitalasset.canton.platform.apiserver.execution.StoreBackedCommandExecutor.AuthenticateContract
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.apiserver.services.*
import com.digitalasset.canton.platform.apiserver.services.admin.*
import com.digitalasset.canton.platform.apiserver.services.command.interactive.InteractiveSubmissionServiceImpl
import com.digitalasset.canton.platform.apiserver.services.command.{
  CommandInspectionServiceImpl,
  CommandServiceImpl,
  CommandSubmissionServiceImpl,
}
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.config.{
  CommandServiceConfig,
  InteractiveSubmissionServiceConfig,
  PartyManagementServiceConfig,
  UserManagementServiceConfig,
}
import com.digitalasset.canton.platform.store.dao.events.LfValueTranslation
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.*
import io.grpc.BindableService
import io.grpc.protobuf.services.ProtoReflectionService
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.time.Instant
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait ApiServices extends AutoCloseable {
  val services: Iterable[BindableService]

  def withServices(otherServices: immutable.Seq[BindableService]): ApiServices
}

private final case class ApiServicesBundle(services: immutable.Seq[BindableService])
    extends ApiServices {

  override def withServices(otherServices: immutable.Seq[BindableService]): ApiServices =
    copy(services = services ++ otherServices)

  override def close(): Unit =
    services.foreach {
      case closeable: AutoCloseable => closeable.close()
      case _ => ()
    }
}

object ApiServices {

  def apply(
      participantId: Ref.ParticipantId,
      syncService: state.SyncService,
      indexService: IndexService,
      userManagementStore: UserManagementStore,
      identityProviderConfigStore: IdentityProviderConfigStore,
      partyRecordStore: PartyRecordStore,
      authorizer: Authorizer,
      engine: Engine,
      timeProvider: TimeProvider,
      timeProviderType: TimeProviderType,
      submissionTracker: SubmissionTracker,
      commandProgressTracker: CommandProgressTracker,
      commandConfig: CommandServiceConfig,
      optTimeServiceBackend: Option[TimeServiceBackend],
      servicesExecutionContext: ExecutionContext,
      metrics: LedgerApiServerMetrics,
      healthChecks: HealthChecks,
      seedService: SeedService,
      managementServiceTimeout: FiniteDuration,
      checkOverloaded: TraceContext => Option[state.SubmissionResult],
      ledgerFeatures: LedgerFeatures,
      maxDeduplicationDuration: config.NonNegativeFiniteDuration,
      userManagementServiceConfig: UserManagementServiceConfig,
      partyManagementServiceConfig: PartyManagementServiceConfig,
      engineLoggingConfig: EngineLoggingConfig,
      meteringReportKey: MeteringReportKey,
      authenticateContract: AuthenticateContract,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
      dynParamGetter: DynamicDomainParameterGetter,
      interactiveSubmissionServiceConfig: InteractiveSubmissionServiceConfig,
      lfValueTranslation: LfValueTranslation,
      logger: TracedLogger,
  )(implicit
      materializer: Materializer,
      esf: ExecutionSequencerFactory,
      tracer: Tracer,
  ): ApiServices = {
    implicit val traceContext: TraceContext = TraceContext.empty

    val activeContractsService: IndexActiveContractsService = indexService
    val transactionsService: IndexTransactionsService = indexService
    val eventQueryService: IndexEventQueryService = indexService
    val contractStore: ContractStore = indexService
    val maximumLedgerTimeService: MaximumLedgerTimeService = indexService
    val completionsService: IndexCompletionsService = indexService
    val partyManagementService: IndexPartyManagementService = indexService
    val meteringStore: MeteringStore = indexService

    def createServices(
        checkOverloaded: TraceContext => Option[state.SubmissionResult]
    )(implicit
        executionContext: ExecutionContext
    ): List[BindableService] = {

      val transactionServiceRequestValidator =
        new UpdateServiceRequestValidator(
          partyValidator = new PartyValidator(PartyNameChecker.AllowAllParties)
        )

      val apiInspectionServiceOpt =
        Option
          .when(ledgerFeatures.commandInspectionService.supported)(
            new CommandInspectionServiceAuthorization(
              CommandInspectionServiceImpl.createApiService(
                commandProgressTracker,
                telemetry,
                loggerFactory,
              ),
              authorizer,
            )
          )

      val (ledgerApiV2Services, ledgerApiUpdateService) = {
        val apiTimeServiceOpt =
          optTimeServiceBackend.map(tsb =>
            new TimeServiceAuthorization(
              new ApiTimeService(tsb, telemetry, loggerFactory),
              authorizer,
            )
          )
        val apiCommandCompletionService = new ApiCommandCompletionService(
          completionsService,
          metrics,
          telemetry,
          loggerFactory,
        )
        val apiEventQueryService =
          new ApiEventQueryService(eventQueryService, telemetry, loggerFactory)
        val apiPackageService = new ApiPackageService(syncService, telemetry, loggerFactory)
        val apiUpdateService =
          new ApiUpdateService(
            transactionsService,
            metrics,
            telemetry,
            loggerFactory,
            transactionServiceRequestValidator,
          )
        val apiStateService =
          new ApiStateService(
            acsService = activeContractsService,
            syncService = syncService,
            txService = transactionsService,
            metrics = metrics,
            telemetry = telemetry,
            loggerFactory = loggerFactory,
          )
        val apiVersionService =
          new ApiVersionService(
            ledgerFeatures,
            userManagementServiceConfig,
            partyManagementServiceConfig,
            telemetry,
            loggerFactory,
          )

        val v2Services = apiTimeServiceOpt.toList :::
          List(
            new CommandCompletionServiceAuthorization(apiCommandCompletionService, authorizer),
            new EventQueryServiceAuthorization(apiEventQueryService, authorizer),
            new PackageServiceAuthorization(apiPackageService, authorizer),
            new UpdateServiceAuthorization(apiUpdateService, authorizer),
            new StateServiceAuthorization(apiStateService, authorizer),
            apiVersionService,
          )

        v2Services -> Some(apiUpdateService)
      }

      val syncServiceBackedApiServices =
        intitializeSyncServiceBackedApiServices(
          ledgerApiUpdateService,
          checkOverloaded,
        )

      val apiReflectionService = ProtoReflectionService.newInstance()

      val apiHealthService = new GrpcHealthService(healthChecks, telemetry, loggerFactory)

      val userManagementServices: List[BindableService] =
        if (userManagementServiceConfig.enabled) {
          val apiUserManagementService =
            new ApiUserManagementService(
              userManagementStore = userManagementStore,
              maxUsersPageSize = userManagementServiceConfig.maxUsersPageSize,
              submissionIdGenerator = SubmissionIdGenerator.Random,
              identityProviderExists = new IdentityProviderExists(identityProviderConfigStore),
              partyRecordExist = new PartyRecordsExist(partyRecordStore),
              indexPartyManagementService = partyManagementService,
              telemetry = telemetry,
              loggerFactory = loggerFactory,
            )
          val identityProvider =
            new ApiIdentityProviderConfigService(
              identityProviderConfigStore,
              telemetry,
              loggerFactory,
            )
          List(
            new UserManagementServiceAuthorization(
              apiUserManagementService,
              authorizer,
              loggerFactory,
            ),
            new IdentityProviderConfigServiceAuthorization(identityProvider, authorizer),
          )
        } else {
          List.empty
        }

      val apiMeteringReportService =
        new ApiMeteringReportService(
          participantId,
          meteringStore,
          meteringReportKey,
          telemetry,
          loggerFactory,
        )

      ledgerApiV2Services :::
        apiInspectionServiceOpt.toList :::
        syncServiceBackedApiServices :::
        List(
          apiReflectionService,
          apiHealthService,
          new MeteringReportServiceAuthorization(apiMeteringReportService, authorizer),
        ) ::: userManagementServices
    }

    def intitializeSyncServiceBackedApiServices(
        ledgerApiV2Enabled: Option[ApiUpdateService],
        checkOverloaded: TraceContext => Option[state.SubmissionResult],
    )(implicit
        executionContext: ExecutionContext
    ): List[BindableService] = {
      val commandExecutor = new TimedCommandExecutor(
        new LedgerTimeAwareCommandExecutor(
          new StoreBackedCommandExecutor(
            engine,
            participantId,
            syncService,
            contractStore,
            authenticateContract,
            metrics,
            engineLoggingConfig,
            loggerFactory,
            dynParamGetter,
            timeProvider,
          ),
          new ResolveMaximumLedgerTime(maximumLedgerTimeService, loggerFactory),
          maxRetries = 3,
          metrics,
          loggerFactory,
        ),
        metrics,
      )

      val validateUpgradingPackageResolutions =
        new ValidateUpgradingPackageResolutionsImpl(
          getPackageMetadataSnapshot = syncService.getPackageMetadataSnapshot(_)
        )
      val commandsValidator = new CommandsValidator(
        validateUpgradingPackageResolutions = validateUpgradingPackageResolutions
      )
      val commandSubmissionService =
        CommandSubmissionServiceImpl.createApiService(
          syncService,
          timeProvider,
          timeProviderType,
          seedService,
          commandExecutor,
          checkOverloaded,
          metrics,
          loggerFactory,
        )

      val apiPartyManagementService = ApiPartyManagementService.createApiService(
        partyManagementService,
        new IdentityProviderExists(identityProviderConfigStore),
        partyManagementServiceConfig.maxPartiesPageSize,
        partyRecordStore,
        transactionsService,
        syncService,
        managementServiceTimeout,
        telemetry = telemetry,
        loggerFactory = loggerFactory,
      )

      val apiPackageManagementService =
        ApiPackageManagementService.createApiService(
          packageSyncService = syncService,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

      val participantPruningService = ApiParticipantPruningService.createApiService(
        indexService,
        syncService,
        metrics,
        telemetry,
        loggerFactory,
      )

      val ledgerApiV2Services = ledgerApiV2Enabled.toList.flatMap { apiUpdateService =>
        val apiSubmissionService = new ApiCommandSubmissionService(
          commandsValidator = commandsValidator,
          commandSubmissionService = commandSubmissionService,
          submissionSyncService = syncService,
          currentLedgerTime = () => timeProvider.getCurrentTime,
          currentUtcTime = () => Instant.now,
          maxDeduplicationDuration = maxDeduplicationDuration.asJava,
          submissionIdGenerator = SubmissionIdGenerator.Random,
          tracker = commandProgressTracker,
          metrics = metrics,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )
        val apiCommandService = CommandServiceImpl.createApiService(
          commandsValidator = commandsValidator,
          submissionTracker = submissionTracker,
          // Using local services skips the gRPC layer, improving performance.
          submit = apiSubmissionService.submitWithTraceContext,
          defaultTrackingTimeout = commandConfig.defaultTrackingTimeout,
          transactionServices = new CommandServiceImpl.TransactionServices(
            getTransactionTreeById = apiUpdateService.getTransactionTreeById,
            getTransactionById = apiUpdateService.getTransactionById,
          ),
          timeProvider = timeProvider,
          maxDeduplicationDuration = maxDeduplicationDuration,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

        val apiInteractiveSubmissionService =
          Option.when(interactiveSubmissionServiceConfig.enabled) {
            val interactiveSubmissionService =
              InteractiveSubmissionServiceImpl.createApiService(
                syncService,
                timeProvider,
                timeProviderType,
                seedService,
                commandExecutor,
                metrics,
                checkOverloaded,
                lfValueTranslation,
                loggerFactory,
              )

            new ApiInteractiveSubmissionService(
              commandsValidator = commandsValidator,
              interactiveSubmissionService = interactiveSubmissionService,
              currentLedgerTime = () => timeProvider.getCurrentTime,
              currentUtcTime = () => Instant.now,
              maxDeduplicationDuration = maxDeduplicationDuration.asJava,
              submissionIdGenerator = SubmissionIdGenerator.Random,
              metrics = metrics,
              telemetry = telemetry,
              loggerFactory = loggerFactory,
            )
          }

        List(
          new CommandSubmissionServiceAuthorization(apiSubmissionService, authorizer),
          new CommandServiceAuthorization(apiCommandService, authorizer),
        ) ++ apiInteractiveSubmissionService.toList.map(
          new InteractiveSubmissionServiceAuthorization(_, authorizer)
        )
      }

      List(
        new PartyManagementServiceAuthorization(apiPartyManagementService, authorizer),
        new PackageManagementServiceAuthorization(apiPackageManagementService, authorizer),
        new ParticipantPruningServiceAuthorization(participantPruningService, authorizer),
      ) ::: ledgerApiV2Services
    }

    logger.info(engine.info.toString)
    ApiServicesBundle(createServices(checkOverloaded)(servicesExecutionContext))
  }
}
