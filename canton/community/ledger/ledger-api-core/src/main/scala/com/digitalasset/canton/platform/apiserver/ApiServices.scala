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
import com.digitalasset.canton.ledger.localstore.api.{
  IdentityProviderConfigStore,
  PartyRecordStore,
  UserManagementStore,
}
import com.digitalasset.canton.ledger.participant.state.index.v2.*
import com.digitalasset.canton.ledger.participant.state.v2.ReadService
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.configuration.{
  LedgerConfigurationInitializer,
  LedgerConfigurationSubscription,
}
import com.digitalasset.canton.platform.apiserver.execution.StoreBackedCommandExecutor.AuthenticateContract
import com.digitalasset.canton.platform.apiserver.execution.*
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.apiserver.services.*
import com.digitalasset.canton.platform.apiserver.services.admin.*
import com.digitalasset.canton.platform.apiserver.services.command.CommandSubmissionServiceImpl
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.config.{CommandServiceConfig, UserManagementServiceConfig}
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadataStore
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.BindableService
import io.grpc.protobuf.services.ProtoReflectionService
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.time.Instant
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
      apiStreamShutdownTimeout: FiniteDuration,
      meteringReportKey: MeteringReportKey,
      authenticateContract: AuthenticateContract,
      telemetry: Telemetry,
      val loggerFactory: NamedLoggerFactory,
      upgradingEnabled: Boolean,
      dynParamGetter: DynamicDomainParameterGetter,
  )(implicit
      materializer: Materializer,
      esf: ExecutionSequencerFactory,
      tracer: Tracer,
  ) extends ResourceOwner[ApiServices]
      with NamedLogging {

    private val identityService: IdentityProvider = indexService
    private val packagesService: IndexPackagesService = indexService
    private val activeContractsService: IndexActiveContractsService = indexService
    private val transactionsService: IndexTransactionsService = indexService
    private val eventQueryService: IndexEventQueryService = indexService
    private val contractStore: ContractStore = indexService
    private val maximumLedgerTimeService: MaximumLedgerTimeService = indexService
    private val completionsService: IndexCompletionsService = indexService
    private val partyManagementService: IndexPartyManagementService = indexService
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

      val transactionFilterValidator = new TransactionFilterValidator(upgradingEnabled)
      val transactionServiceRequestValidator =
        new TransactionServiceRequestValidator(
          ledgerId = ledgerId,
          partyValidator = new PartyValidator(PartyNameChecker.AllowAllParties),
          transactionFilterValidator = transactionFilterValidator,
        )

      val (ledgerApiV2Services, ledgerApiUpdateService) = {
        val apiTimeServiceOpt =
          optTimeServiceBackend.map(tsb =>
            new TimeServiceV2Authorization(
              new ApiTimeServiceV2(tsb, telemetry, loggerFactory),
              authorizer,
            )
          )
        val apiCommandCompletionService = new ApiCommandCompletionServiceV2(
          completionsService,
          metrics,
          telemetry,
          loggerFactory,
        )
        val apiEventQueryService =
          new ApiEventQueryServiceV2(eventQueryService, telemetry, loggerFactory)
        val apiPackageService = new ApiPackageServiceV2(packagesService, telemetry, loggerFactory)
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
            readService = readService,
            txService = transactionsService,
            metrics = metrics,
            telemetry = telemetry,
            loggerFactory = loggerFactory,
            transactionFilterValidator = transactionFilterValidator,
          )
        val apiVersionService =
          new ApiVersionServiceV2(
            ledgerFeatures,
            userManagementServiceConfig,
            telemetry,
            loggerFactory,
          )

        val v2Services = apiTimeServiceOpt.toList :::
          List(
            new CommandCompletionServiceV2Authorization(apiCommandCompletionService, authorizer),
            new EventQueryServiceV2Authorization(apiEventQueryService, authorizer),
            new PackageServiceV2Authorization(apiPackageService, authorizer),
            new UpdateServiceAuthorization(apiUpdateService, authorizer),
            new StateServiceAuthorization(apiStateService, authorizer),
            apiVersionService,
          )

        v2Services -> Some(apiUpdateService)
      }

      val writeServiceBackedApiServices =
        intitializeWriteServiceBackedApiServices(
          ledgerId,
          ledgerConfigurationSubscription,
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
        writeServiceBackedApiServices :::
        List(
          apiReflectionService,
          apiHealthService,
          new MeteringReportServiceAuthorization(apiMeteringReportService, authorizer),
        ) ::: userManagementServices
    }

    private def intitializeWriteServiceBackedApiServices(
        ledgerId: LedgerId,
        ledgerConfigurationSubscription: LedgerConfigurationSubscription,
        ledgerApiV2Enabled: Option[ApiUpdateService],
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
              authenticateContract,
              metrics,
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
          upgradingEnabled = upgradingEnabled,
        )
        val (_, commandSubmissionService) =
          CommandSubmissionServiceImpl.createApiService(
            writeService,
            commandsValidator,
            timeProvider,
            timeProviderType,
            ledgerConfigurationSubscription,
            seedService,
            commandExecutor,
            checkOverloaded,
            metrics,
            telemetry,
            loggerFactory,
          )

        val apiPartyManagementService = ApiPartyManagementService.createApiService(
          partyManagementService,
          new IdentityProviderExists(identityProviderConfigStore),
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
          packageMetadataStore,
          writeService,
          managementServiceTimeout,
          engine,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

        val participantPruningService = ApiParticipantPruningService.createApiService(
          indexService,
          writeService,
          readService,
          metrics,
          telemetry,
          loggerFactory,
        )

        val ledgerApiV2Services = ledgerApiV2Enabled.toList.flatMap { apiUpdateService =>
          val apiSubmissionServiceV2 = new ApiCommandSubmissionServiceV2(
            commandsValidator = commandsValidator,
            commandSubmissionService = commandSubmissionService,
            writeService = writeService,
            currentLedgerTime = () => timeProvider.getCurrentTime,
            currentUtcTime = () => Instant.now,
            maxDeduplicationDuration = () =>
              ledgerConfigurationSubscription.latestConfiguration().map(_.maxDeduplicationDuration),
            submissionIdGenerator = SubmissionIdGenerator.Random,
            metrics = metrics,
            telemetry = telemetry,
            loggerFactory = loggerFactory,
          )
          val apiCommandService = new ApiCommandServiceV2(
            commandsValidator = commandsValidator,
            transactionServices = new ApiCommandServiceV2.TransactionServices(
              getTransactionTreeById = apiUpdateService.getTransactionTreeById,
              getTransactionById = apiUpdateService.getTransactionById,
            ),
            submissionTracker = submissionTracker,
            submit = apiSubmissionServiceV2.submitWithTraceContext,
            defaultTrackingTimeout = commandConfig.defaultTrackingTimeout,
            currentLedgerTime = () => timeProvider.getCurrentTime,
            currentUtcTime = () => Instant.now,
            maxDeduplicationDuration = () =>
              ledgerConfigurationSubscription.latestConfiguration().map(_.maxDeduplicationDuration),
            generateSubmissionId = SubmissionIdGenerator.Random,
            telemetry = telemetry,
            loggerFactory = loggerFactory,
          )

          List(
            new CommandSubmissionServiceV2Authorization(apiSubmissionServiceV2, authorizer),
            new CommandServiceV2Authorization(apiCommandService, authorizer),
          )
        }

        List(
          new PartyManagementServiceAuthorization(apiPartyManagementService, authorizer),
          new PackageManagementServiceAuthorization(apiPackageManagementService, authorizer),
          new ParticipantPruningServiceAuthorization(participantPruningService, authorizer),
        ) ::: ledgerApiV2Services
      }
    }
  }
}
