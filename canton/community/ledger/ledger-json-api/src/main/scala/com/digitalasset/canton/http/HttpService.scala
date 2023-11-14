// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.Materializer
import ch.qos.logback.classic.Level as LogLevel
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.jwt.JwtDecoder
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContextOf
import com.daml.metrics.akkahttp.HttpMetricsInterceptor
import com.daml.ports.{Port, PortFiles}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.http.json.{
  ApiValueToJsValueConverter,
  DomainJsonDecoder,
  DomainJsonEncoder,
  JsValueToApiValueConverter,
}
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.http.util.ApiValueToLfValueConverter
import com.digitalasset.canton.http.util.FutureUtil.*
import com.digitalasset.canton.http.util.Logging.InstanceUUID
import com.digitalasset.canton.ledger.api.domain as LedgerApiDomain
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.digitalasset.canton.ledger.client.services.pkg.withoutledgerid.PackageClient
import com.digitalasset.canton.ledger.client.withoutledgerid.LedgerClient as DamlLedgerClient
import com.digitalasset.canton.ledger.service.LedgerReader
import com.digitalasset.canton.ledger.service.LedgerReader.PackageStore
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import io.grpc.Channel
import io.grpc.health.v1.health.{HealthCheckRequest, HealthGrpc}
import scalaz.*
import scalaz.Scalaz.*

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}

class HttpService(
    startSettings: StartSettings,
    channel: Channel,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    asys: ActorSystem,
    mat: Materializer,
    esf: ExecutionSequencerFactory,
    lc: LoggingContextOf[InstanceUUID],
    metrics: HttpApiMetrics,
) extends ResourceOwner[ServerBinding]
    with NamedLogging
    with NoTracing {
  import HttpService.doLoad

  private type ET[A] = EitherT[Future, HttpService.Error, A]

  private val directEc = DirectExecutionContext(noTracingLogger)

  private def isLogLevelEqualOrBelowDebug(logLevel: Option[LogLevel]) =
    logLevel.exists(!_.isGreaterOrEqual(LogLevel.INFO))

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def acquire()(implicit context: ResourceContext): Resource[ServerBinding] = Resource({
    logger.info(s"Starting JSON API server, ${lc.makeString}")

    import startSettings.*
    val DummyApplicationId: ApplicationId = ApplicationId("HTTP-JSON-API-Gateway")

    implicit val settings: ServerSettings = ServerSettings(asys).withTransparentHeadRequests(true)
    val clientConfig = LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(DummyApplicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
    )

    val ledgerClient: DamlLedgerClient = DamlLedgerClient(channel, clientConfig, loggerFactory)
    import akka.http.scaladsl.server.Directives.*
    val bindingEt: EitherT[Future, HttpService.Error, ServerBinding] = for {
      _ <- eitherT(Future.successful(\/-(ledgerClient)))
      packageCache = LedgerReader.LoadCache.freshCache()

      packageService = new PackageService(
        reloadPackageStoreIfChanged =
          doLoad(ledgerClient.packageClient, LedgerReader(loggerFactory), packageCache),
        loggerFactory = loggerFactory,
      )

      ledgerClientJwt = LedgerClientJwt(loggerFactory)

      commandService = new CommandService(
        ledgerClientJwt.submitAndWaitForTransaction(ledgerClient),
        ledgerClientJwt.submitAndWaitForTransactionTree(ledgerClient),
        loggerFactory,
      )

      contractsService = new ContractsService(
        packageService.resolveContractTypeId,
        packageService.allTemplateIds,
        ledgerClientJwt.getByContractId(ledgerClient),
        ledgerClientJwt.getByContractKey(ledgerClient),
        ledgerClientJwt.getActiveContracts(ledgerClient),
        ledgerClientJwt.getCreatesAndArchivesSince(ledgerClient),
        loggerFactory,
      )

      partiesService = new PartiesService(
        ledgerClientJwt.listKnownParties(ledgerClient),
        ledgerClientJwt.getParties(ledgerClient),
        ledgerClientJwt.allocateParty(ledgerClient),
      )

      packageManagementService = new PackageManagementService(
        ledgerClientJwt.listPackages(ledgerClient),
        ledgerClientJwt.getPackage(ledgerClient),
        { case (jwt, ledgerId, byteString) =>
          implicit lc =>
            ledgerClientJwt
              .uploadDar(ledgerClient)(directEc)(
                jwt,
                ledgerId,
                byteString,
              )(lc)
              .flatMap(_ => packageService.reload(jwt, ledgerId))
              .map(_ => ())
        },
      )

      meteringReportService = new MeteringReportService(
        { case (jwt, request) =>
          implicit lc =>
            ledgerClientJwt
              .getMeteringReport(ledgerClient)(directEc)(jwt, request)(
                lc
              )
        }
      )

      ledgerHealthService = HealthGrpc.stub(channel)

      healthService = new HealthService(() => ledgerHealthService.check(HealthCheckRequest()))

      _ = metrics.health.registerHealthGauge(
        HttpApiMetrics.ComponentName,
        () => healthService.ready().map(_.checks.forall(_.result)),
      )

      (encoder, decoder) = HttpService.buildJsonCodecs(packageService)

      jsonEndpoints = new Endpoints(
        allowNonHttps,
        HttpService.decodeJwt,
        commandService,
        contractsService,
        partiesService,
        packageManagementService,
        meteringReportService,
        healthService,
        encoder,
        decoder,
        debugLoggingOfHttpBodies,
        ledgerClient.userManagementClient,
        ledgerClient.identityClient,
        loggerFactory,
      )

      websocketService = new WebSocketService(
        contractsService,
        packageService.resolveContractTypeId,
        decoder,
        wsConfig,
        loggerFactory,
      )

      websocketEndpoints = new WebsocketEndpoints(
        HttpService.decodeJwt,
        websocketService,
        ledgerClient.userManagementClient,
        ledgerClient.identityClient,
        loggerFactory,
      )

      rateDurationSizeMetrics = HttpMetricsInterceptor.rateDurationSizeMetrics(metrics.http)

      defaultEndpoints =
        rateDurationSizeMetrics apply concat(
          jsonEndpoints.all: Route,
          websocketEndpoints.transactionWebSocket,
        )

      allEndpoints = concat(
        staticContentConfig.cata(
          c => concat(StaticContentEndpoints.all(c, loggerFactory), defaultEndpoints),
          defaultEndpoints,
        ),
        EndpointsCompanion.notFound(logger),
      )

      binding <- liftET[HttpService.Error](
        Http()
          .newServerAt(address, httpPort.getOrElse(0))
          .withSettings(settings)
          .bind(allEndpoints)
      )

      _ <- either(portFile.cata(f => HttpService.createPortFile(f, binding), \/-(()))): ET[Unit]

    } yield binding

    (bindingEt.run: Future[HttpService.Error \/ ServerBinding]).flatMap {
      case -\/(error) => Future.failed(new RuntimeException(error.message))
      case \/-(binding) => Future.successful(binding)
    }
  })(binding => {
    logger.info(s"Stopping JSON API server..., ${lc.makeString}")
    binding.unbind().void
  })
}

object HttpService {
  // TODO(#13303) Check that this is intended to be used as ValidateJwt in prod code
  //              and inline.
  // Decode JWT without any validation
  private val decodeJwt: EndpointsCompanion.ValidateJwt =
    jwt => JwtDecoder.decode(jwt).leftMap(e => EndpointsCompanion.Unauthorized(e.shows))

  def doLoad(
      packageClient: PackageClient,
      ledgerReader: LedgerReader,
      loadCache: LedgerReader.LoadCache,
  )(jwt: Jwt, ledgerId: LedgerApiDomain.LedgerId)(ids: Set[String])(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[PackageService.ServerError \/ Option[PackageStore]] =
    ledgerReader
      .loadPackageStoreUpdates(
        packageClient,
        loadCache,
        some(jwt.value),
        ledgerId,
      )(ids)
      .map(_.leftMap(e => PackageService.ServerError(e)))

  def buildJsonCodecs(
      packageService: PackageService
  )(implicit ec: ExecutionContext): (DomainJsonEncoder, DomainJsonDecoder) = {

    val lfTypeLookup = LedgerReader.damlLfTypeLookup(() => packageService.packageStore) _
    val jsValueToApiValueConverter = new JsValueToApiValueConverter(lfTypeLookup)

    val apiValueToJsValueConverter = new ApiValueToJsValueConverter(
      ApiValueToLfValueConverter.apiValueToLfValue
    )

    val encoder = new DomainJsonEncoder(
      apiValueToJsValueConverter.apiRecordToJsObject,
      apiValueToJsValueConverter.apiValueToJsValue,
    )

    val decoder = new DomainJsonDecoder(
      packageService.resolveContractTypeId,
      packageService.resolveTemplateRecordType,
      packageService.resolveChoiceArgType,
      packageService.resolveKeyType,
      jsValueToApiValueConverter.jsValueToApiValue,
      jsValueToApiValueConverter.jsValueToLfValue,
    )

    (encoder, decoder)
  }

  private[http] def createPortFile(
      file: Path,
      binding: akka.http.scaladsl.Http.ServerBinding,
  ): HttpService.Error \/ Unit = {
    import com.digitalasset.canton.http.util.ErrorOps.*
    PortFiles.write(file, Port(binding.localAddress.getPort)).liftErr(Error.apply)
  }

  final case class Error(message: String)
}
