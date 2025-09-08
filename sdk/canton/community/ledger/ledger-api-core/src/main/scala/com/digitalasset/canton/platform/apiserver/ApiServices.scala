// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.tracing.Telemetry
import com.digitalasset.canton.auth.Authorizer
import com.digitalasset.canton.config
import com.digitalasset.canton.interactive.InteractiveSubmissionEnricher
import com.digitalasset.canton.interactive.InteractiveSubmissionEnricher.PackageResolver
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
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.PackagePreferenceBackend
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.platform.apiserver.execution.*
import com.digitalasset.canton.platform.apiserver.execution.ContractAuthenticators.ContractAuthenticatorFn
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
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.*
import io.grpc.BindableService
import io.grpc.protobuf.services.ProtoReflectionServiceV1
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

private final case class ApiServicesBundle(
    services: immutable.Seq[BindableService],
    loggerFactory: NamedLoggerFactory,
) extends ApiServices
    with NamedLogging {

  override def withServices(otherServices: immutable.Seq[BindableService]): ApiServices =
    copy(services = services ++ otherServices)

  override def close(): Unit = {
    services.foreach {
      case closeable: AutoCloseable =>
        noTracingLogger.debug(s"Closing $closeable")
        closeable.close()
        noTracingLogger.debug(s"Successfully closed $closeable")
      case nonCloseable =>
        noTracingLogger.debug(s"Omit closing $nonCloseable, as it is not closeable")
    }
    noTracingLogger.info(s"Successfully closed all API services")
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
      transactionSubmissionTracker: SubmissionTracker,
      reassignmentSubmissionTracker: SubmissionTracker,
      partyAllocationTracker: PartyAllocation.Tracker,
      commandProgressTracker: CommandProgressTracker,
      commandConfig: CommandServiceConfig,
      optTimeServiceBackend: Option[TimeServiceBackend],
      queryExecutionContext: ExecutionContext,
      commandExecutionContext: ExecutionContext,
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
      contractAuthenticator: ContractAuthenticatorFn,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
      dynParamGetter: DynamicSynchronizerParameterGetter,
      interactiveSubmissionServiceConfig: InteractiveSubmissionServiceConfig,
      interactiveSubmissionEnricher: InteractiveSubmissionEnricher,
      logger: TracedLogger,
      packagePreferenceBackend: PackagePreferenceBackend,
  )(implicit
      materializer: Materializer,
      esf: ExecutionSequencerFactory,
      tracer: Tracer,
  ): ApiServices = {
    implicit val traceContext: TraceContext = TraceContext.empty
    val activeContractsService: IndexActiveContractsService = indexService
    val updateService: IndexUpdateService = indexService
    val eventQueryService: IndexEventQueryService = indexService
    val contractStore: ContractStore = indexService
    val maximumLedgerTimeService: MaximumLedgerTimeService = indexService
    val completionsService: IndexCompletionsService = indexService
    val partyManagementService: IndexPartyManagementService = indexService

    val (readServices, ledgerApiUpdateService) = {
      implicit val ec: ExecutionContext = queryExecutionContext

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

      val (ledgerApiServices, ledgerApiUpdateService) = {
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
            updateService,
            metrics,
            telemetry,
            loggerFactory,
          )
        val apiStateService =
          new ApiStateService(
            acsService = activeContractsService,
            syncService = syncService,
            updateService = updateService,
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

        val services = apiTimeServiceOpt.toList :::
          List(
            new CommandCompletionServiceAuthorization(apiCommandCompletionService, authorizer),
            new EventQueryServiceAuthorization(apiEventQueryService, authorizer),
            new PackageServiceAuthorization(apiPackageService, authorizer),
            new UpdateServiceAuthorization(apiUpdateService, authorizer),
            new StateServiceAuthorization(apiStateService, authorizer),
            apiVersionService,
          )

        services -> apiUpdateService
      }

      val apiReflectionService = ProtoReflectionServiceV1.newInstance()

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

      val readServices = ledgerApiServices :::
        apiInspectionServiceOpt.toList :::
        List(
          apiReflectionService,
          apiHealthService,
        ) ::: userManagementServices
      readServices -> ledgerApiUpdateService
    }

    val writeServices = {
      implicit val ec: ExecutionContext = commandExecutionContext

      val packageLoader = new DeduplicatingPackageLoader()

      val packageResolver: PackageResolver = (packageId: Ref.PackageId) =>
        (tc: TraceContext) =>
          FutureUnlessShutdown.outcomeF(
            packageLoader.loadPackage(
              packageId,
              syncService.getLfArchive(_)(tc),
              metrics.execution.getLfPackage,
            )
          )

      val commandInterpreter =
        new StoreBackedCommandInterpreter(
          engine = engine,
          participant = participantId,
          packageResolver = packageResolver,
          contractStore = contractStore,
          contractAuthenticator = contractAuthenticator,
          metrics = metrics,
          config = engineLoggingConfig,
          prefetchingRecursionLevel = commandConfig.contractPrefetchingDepth,
          loggerFactory = loggerFactory,
          dynParamGetter = dynParamGetter,
          timeProvider = timeProvider,
        )

      val commandExecutor =
        new TimedCommandExecutor(
          new LedgerTimeAwareCommandExecutor(
            delegate = CommandExecutor(
              syncService = syncService,
              commandInterpreter = commandInterpreter,
              topologyAwarePackageSelectionEnabled = ledgerFeatures.topologyAwarePackageSelection,
              loggerFactory = loggerFactory,
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
        validateDisclosedContracts = new ValidateDisclosedContracts(contractAuthenticator),
        validateUpgradingPackageResolutions = validateUpgradingPackageResolutions,
        topologyAwarePackageSelectionEnabled = ledgerFeatures.topologyAwarePackageSelection,
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
        userManagementStore,
        new IdentityProviderExists(identityProviderConfigStore),
        partyManagementServiceConfig.maxPartiesPageSize,
        partyRecordStore,
        syncService,
        managementServiceTimeout,
        telemetry = telemetry,
        partyAllocationTracker = partyAllocationTracker,
        submissionIdGenerator =
          ApiPartyManagementService.CreateSubmissionId.forParticipant(participantId),
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
      val updateServices = new CommandServiceImpl.UpdateServices(
        getTransactionTreeById = ledgerApiUpdateService.getTransactionTreeById,
        getUpdateById = ledgerApiUpdateService.getUpdateById,
      )
      val apiCommandService = CommandServiceImpl.createApiService(
        commandsValidator = commandsValidator,
        transactionSubmissionTracker = transactionSubmissionTracker,
        reassignmentSubmissionTracker = reassignmentSubmissionTracker,
        // Using local services skips the gRPC layer, improving performance.
        submit = apiSubmissionService.submitWithTraceContext,
        submitReassignment = apiSubmissionService.submitReassignmentWithTraceContext,
        defaultTrackingTimeout = commandConfig.defaultTrackingTimeout,
        updateServices = updateServices,
        timeProvider = timeProvider,
        maxDeduplicationDuration = maxDeduplicationDuration,
        telemetry = telemetry,
        loggerFactory = loggerFactory,
      )

      val apiInteractiveSubmissionService = {
        val interactiveSubmissionService =
          InteractiveSubmissionServiceImpl.createApiService(
            updateServices,
            syncService,
            seedService,
            commandExecutor,
            metrics,
            checkOverloaded,
            interactiveSubmissionEnricher,
            interactiveSubmissionServiceConfig,
            contractStore,
            packagePreferenceBackend,
            transactionSubmissionTracker,
            commandConfig.defaultTrackingTimeout,
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
        new PartyManagementServiceAuthorization(apiPartyManagementService, authorizer),
        new PackageManagementServiceAuthorization(apiPackageManagementService, authorizer),
        new ParticipantPruningServiceAuthorization(participantPruningService, authorizer),
        new InteractiveSubmissionServiceAuthorization(apiInteractiveSubmissionService, authorizer),
      )
    }

    logger.info(engine.info.toString)
    ApiServicesBundle(readServices ::: writeServices, loggerFactory)
  }
}
