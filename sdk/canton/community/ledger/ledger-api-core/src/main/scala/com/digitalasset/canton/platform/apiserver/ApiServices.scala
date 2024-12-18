// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.lf.engine.*
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.*
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.grpc.GrpcHealthService
import com.digitalasset.canton.ledger.api.health.HealthChecks
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.api.validation.*
import com.digitalasset.canton.ledger.participant.state.index.v2.*
import com.digitalasset.canton.ledger.participant.state.v2.ReadService
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.configuration.{
  EngineLoggingConfig,
  LedgerConfigurationInitializer,
  LedgerConfigurationSubscription,
}
import com.digitalasset.canton.platform.apiserver.execution.*
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.apiserver.services.*
import com.digitalasset.canton.platform.apiserver.services.admin.*
import com.digitalasset.canton.platform.apiserver.services.command.{
  CommandCompletionServiceImpl,
  CommandInspectionServiceImpl,
  CommandServiceImpl,
  CommandSubmissionServiceImpl,
}
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.apiserver.services.transaction.{
  EventQueryServiceImpl,
  TransactionServiceImpl,
}
import com.digitalasset.canton.platform.config.{
  CommandServiceConfig,
  PartyManagementServiceConfig,
  UserManagementServiceConfig,
}
import com.digitalasset.canton.platform.localstore.PackageMetadataStore
import com.digitalasset.canton.platform.localstore.api.{
  IdentityProviderConfigStore,
  PartyRecordStore,
  UserManagementStore,
}
import com.digitalasset.canton.platform.services.time.TimeProviderType
import com.digitalasset.canton.platform.store.cache.PruningOffsetCache
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.BindableService
import io.grpc.protobuf.services.ProtoReflectionService
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait ApiServices {
  val services: Iterable[BindableService]

  def withServices(otherServices: immutable.Seq[BindableService]): ApiServices
}

private final case class ApiServicesBundle(services: immutable.Seq[BindableService])
    extends ApiServices {

  override def withServices(otherServices: immutable.Seq[BindableService]): ApiServices =
    copy(services = services ++ otherServices)

}

object ApiServices {

  final class Owner(
      participantId: Ref.ParticipantId,
      optWriteService: Option[state.WriteService],
      readService: ReadService,
      indexService: IndexService,
      userManagementStore: UserManagementStore,
      packageMetadataStore: PackageMetadataStore,
      identityProviderConfigStore: IdentityProviderConfigStore,
      partyRecordStore: PartyRecordStore,
      authorizer: Authorizer,
      engine: Engine,
      authorityResolver: AuthorityResolver,
      timeProvider: TimeProvider,
      timeProviderType: TimeProviderType,
      submissionTracker: SubmissionTracker,
      commandProgressTracker: CommandProgressTracker,
      configurationLoadTimeout: FiniteDuration,
      commandConfig: CommandServiceConfig,
      optTimeServiceBackend: Option[TimeServiceBackend],
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      healthChecks: HealthChecks,
      seedService: SeedService,
      managementServiceTimeout: FiniteDuration,
      checkOverloaded: TraceContext => Option[state.SubmissionResult],
      ledgerFeatures: LedgerFeatures,
      userManagementServiceConfig: UserManagementServiceConfig,
      partyManagementServiceConfig: PartyManagementServiceConfig,
      apiStreamShutdownTimeout: FiniteDuration,
      engineLoggingConfig: EngineLoggingConfig,
      meteringReportKey: MeteringReportKey,
      enableExplicitDisclosure: Boolean,
      serializableContractAuthenticators: SerializableContractAuthenticators,
      telemetry: Telemetry,
      val loggerFactory: NamedLoggerFactory,
      dynParamGetter: DynamicDomainParameterGetter,
      pruningOffsetCache: PruningOffsetCache,
  )(implicit
      materializer: Materializer,
      esf: ExecutionSequencerFactory,
      tracer: Tracer,
  ) extends ResourceOwner[ApiServices]
      with NamedLogging {
    private val configurationService: IndexConfigurationService = indexService
    private val identityService: IdentityProvider = indexService
    private val packagesService: IndexPackagesService = indexService
    private val activeContractsService: IndexActiveContractsService = indexService
    private val transactionsService: IndexTransactionsService = indexService
    private val eventQueryService: IndexEventQueryService = indexService
    private val contractStore: ContractStore = indexService
    private val maximumLedgerTimeService: MaximumLedgerTimeService = indexService
    private val completionsService: IndexCompletionsService = indexService
    private val partyManagementService: IndexPartyManagementService = indexService
    private val configManagementService: IndexConfigManagementService = indexService
    private val meteringStore: MeteringStore = indexService

    private val configurationInitializer = new LedgerConfigurationInitializer(
      indexService = indexService,
      materializer = materializer,
      servicesExecutionContext = servicesExecutionContext,
      loggerFactory = loggerFactory,
    )

    override def acquire()(implicit context: ResourceContext): Resource[ApiServices] = {
      logger.info(engine.info.toString)(TraceContext.empty)
      for {
        currentLedgerConfiguration <- configurationInitializer.initialize(
          configurationLoadTimeout = configurationLoadTimeout
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
        checkOverloaded: TraceContext => Option[state.SubmissionResult],
    )(implicit
        executionContext: ExecutionContext
    ): List[BindableService] = {

      val transactionServiceRequestValidator =
        new TransactionServiceRequestValidator(
          ledgerId = ledgerId,
          partyValidator = new PartyValidator(PartyNameChecker.AllowAllParties),
        )

      val apiTransactionService =
        TransactionServiceImpl.create(
          ledgerId,
          transactionsService,
          metrics,
          telemetry,
          loggerFactory,
          transactionServiceRequestValidator,
        )

      val apiEventQueryService =
        EventQueryServiceImpl.create(ledgerId, eventQueryService, telemetry, loggerFactory)

      val apiLedgerIdentityService =
        ApiLedgerIdentityService.create(ledgerId, telemetry, loggerFactory)

      val apiVersionService =
        ApiVersionService.create(
          ledgerFeatures,
          userManagementServiceConfig = userManagementServiceConfig,
          partyManagementServiceConfig = partyManagementServiceConfig,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

      val apiPackageService =
        ApiPackageService.create(ledgerId, packagesService, telemetry, loggerFactory)

      val apiConfigurationService =
        ApiLedgerConfigurationService.create(
          ledgerId,
          configurationService,
          telemetry,
          loggerFactory,
        )

      val apiCompletionService =
        CommandCompletionServiceImpl.createApiService(
          ledgerId,
          completionsService,
          metrics,
          telemetry,
          loggerFactory,
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

      val apiActiveContractsService =
        ApiActiveContractsService.create(
          ledgerId,
          activeContractsService,
          metrics,
          telemetry,
          loggerFactory,
        )

      val apiTimeServiceOpt =
        optTimeServiceBackend.map(tsb =>
          new TimeServiceAuthorization(
            ApiTimeService
              .create(ledgerId, tsb, apiStreamShutdownTimeout, telemetry, loggerFactory),
            authorizer,
          )
        )

      val writeServiceBackedApiServices =
        intitializeWriteServiceBackedApiServices(
          ledgerId,
          ledgerConfigurationSubscription,
          apiTransactionService,
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

      apiTimeServiceOpt.toList :::
        apiInspectionServiceOpt.toList :::
        writeServiceBackedApiServices :::
        List(
          new LedgerIdentityServiceAuthorization(apiLedgerIdentityService, authorizer),
          new PackageServiceAuthorization(apiPackageService, authorizer),
          new LedgerConfigurationServiceAuthorization(apiConfigurationService, authorizer),
          new TransactionServiceAuthorization(apiTransactionService, authorizer),
          new EventQueryServiceAuthorization(apiEventQueryService, authorizer),
          new CommandCompletionServiceAuthorization(apiCompletionService, authorizer),
          new ActiveContractsServiceAuthorization(apiActiveContractsService, authorizer),
          apiReflectionService,
          apiHealthService,
          apiVersionService,
          new MeteringReportServiceAuthorization(apiMeteringReportService, authorizer),
        ) ::: userManagementServices
    }

    private def intitializeWriteServiceBackedApiServices(
        ledgerId: LedgerId,
        ledgerConfigurationSubscription: LedgerConfigurationSubscription,
        apiTransactionService: ApiTransactionService,
        checkOverloaded: TraceContext => Option[state.SubmissionResult],
    )(implicit
        executionContext: ExecutionContext
    ): List[BindableService] = {
      optWriteService.toList.flatMap { writeService =>
        val commandExecutor = new TimedCommandExecutor(
          new LedgerTimeAwareCommandExecutor(
            new StoreBackedCommandExecutor(
              engine,
              participantId,
              packagesService,
              contractStore,
              authorityResolver,
              serializableContractAuthenticators.upgrade,
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
          ValidateUpgradingPackageResolutions(packageMetadataStore)
        val commandsValidator = CommandsValidator(
          ledgerId = ledgerId,
          validateUpgradingPackageResolutions = validateUpgradingPackageResolutions,
          enableExplicitDisclosure = enableExplicitDisclosure,
          authenticateSerializableContract = serializableContractAuthenticators.input,
        )
        val (apiSubmissionService, commandSubmissionService) =
          CommandSubmissionServiceImpl.createApiService(
            writeService,
            commandsValidator,
            timeProvider,
            timeProviderType,
            ledgerConfigurationSubscription,
            seedService,
            commandExecutor,
            checkOverloaded,
            commandProgressTracker,
            metrics,
            telemetry,
            loggerFactory,
          )

        // Note: the command service uses the command submission, command completion, and transaction
        // services internally. These connections do not use authorization, authorization wrappers are
        // only added here to all exposed services.
        val apiCommandService = CommandServiceImpl.createApiService(
          commandsValidator = commandsValidator,
          submissionTracker = submissionTracker,
          // Using local services skips the gRPC layer, improving performance.
          submit = apiSubmissionService.submitWithTraceContext,
          defaultTrackingTimeout = commandConfig.defaultTrackingTimeout,
          transactionServices = new CommandServiceImpl.TransactionServices(
            getTransactionById = apiTransactionService.getTransactionById,
            getFlatTransactionById = apiTransactionService.getFlatTransactionById,
          ),
          timeProvider = timeProvider,
          ledgerConfigurationSubscription = ledgerConfigurationSubscription,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

        val apiPartyManagementService = ApiPartyManagementService.createApiService(
          partyManagementService,
          new IdentityProviderExists(identityProviderConfigStore),
          partyManagementServiceConfig.maxPartiesPageSize,
          partyRecordStore,
          transactionsService,
          writeService,
          managementServiceTimeout,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

        val apiPackageManagementService = ApiPackageManagementService.createApiService(
          indexService,
          transactionsService,
          writeService,
          readService,
          managementServiceTimeout,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

        val apiConfigManagementService = ApiConfigManagementService.createApiService(
          configManagementService,
          writeService,
          timeProvider,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

        List(
          new CommandSubmissionServiceAuthorization(apiSubmissionService, authorizer),
          new CommandServiceAuthorization(apiCommandService, authorizer),
          new PartyManagementServiceAuthorization(apiPartyManagementService, authorizer),
          new PackageManagementServiceAuthorization(apiPackageManagementService, authorizer),
          new ConfigManagementServiceAuthorization(apiConfigManagementService, authorizer),
          new ParticipantPruningServiceAuthorization(
            ApiParticipantPruningService.createApiService(
              indexService,
              writeService,
              pruningOffsetCache,
              metrics,
              telemetry,
              loggerFactory,
            ),
            authorizer,
          ),
        )
      }
    }
  }
}
