// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.http.dbbackend.ContractDao
import com.daml.http.json.{
  ApiValueToJsValueConverter,
  DomainJsonDecoder,
  DomainJsonEncoder,
  JsValueToApiValueConverter,
}
import com.daml.http.util.ApiValueToLfValueConverter
import com.daml.http.util.FutureUtil._
import com.daml.http.util.IdentifierConverters.apiLedgerId
import com.daml.http.util.Logging.InstanceUUID
import com.daml.jwt.JwtDecoder
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.client.services.pkg.PackageClient
import com.daml.ledger.client.{LedgerClient => DamlLedgerClient}
import com.daml.ledger.service.LedgerReader
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.Metrics
import com.daml.ports.{Port, PortFiles}
import io.grpc.health.v1.health.{HealthCheckRequest, HealthGrpc}
import scalaz.Scalaz._
import scalaz._

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}
import ch.qos.logback.classic.{Level => LogLevel}
import com.daml.jwt.domain.Jwt

object HttpService {

  private val logger = ContextualizedLogger.get(getClass)

  // used only to populate a required field in LedgerClientConfiguration
  private val DummyApplicationId: ApplicationId = ApplicationId("HTTP-JSON-API-Gateway")

  // default to 10 minutes for ledger connection retry period when ledger is not ready on start
  private val MaxInitialLedgerConnectRetryAttempts = 600

  private type ET[A] = EitherT[Future, Error, A]

  object Error {
    def fromLedgerClientError(e: LedgerClientBase.Error): Error = Error(e.message)
  }

  final case class Error(message: String)

  def start(
      startSettings: StartSettings,
      contractDao: Option[ContractDao] = None,
      validateJwt: EndpointsCompanion.ValidateJwt = decodeJwt,
  )(implicit
      asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
      metrics: Metrics,
  ): Future[Error \/ (ServerBinding, Option[ContractDao])] = {

    logger.info("HTTP Server pre-startup")

    import startSettings._

    implicit val settings: ServerSettings = ServerSettings(asys).withTransparentHeadRequests(true)

    val clientConfig = LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(DummyApplicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = tlsConfig.client,
      maxInboundMessageSize = maxInboundMessageSize,
    )

    import akka.http.scaladsl.server.Directives._
    val bindingEt: EitherT[Future, Error, (ServerBinding, Option[ContractDao])] = for {
      client <- eitherT(
        ledgerClient(
          ledgerHost,
          ledgerPort,
          clientConfig,
          startSettings.nonRepudiation,
        )
      ): ET[DamlLedgerClient]

      pkgManagementClient <- eitherT(
        ledgerClient(
          ledgerHost,
          ledgerPort,
          packageMaxInboundMessageSize.fold(clientConfig)(size =>
            clientConfig.copy(maxInboundMessageSize = size)
          ),
          startSettings.nonRepudiation,
        )
      ): ET[DamlLedgerClient]

      ledgerId = apiLedgerId(client.ledgerId): lar.LedgerId

      _ = logger.info(s"Connected to Ledger: ${ledgerId: lar.LedgerId}")
      _ = logger.info(s"contractDao: ${contractDao.toString}")

      packageService = new PackageService(jwt =>
        doLoad(pkgManagementClient.packageClient, _, some(jwt.value))
      )

      commandService = new CommandService(
        LedgerClientJwt.submitAndWaitForTransaction(client),
        LedgerClientJwt.submitAndWaitForTransactionTree(client),
      )

      contractsService = new ContractsService(
        packageService.resolveTemplateId,
        packageService.allTemplateIds,
        LedgerClientJwt.getActiveContracts(client),
        LedgerClientJwt.getCreatesAndArchivesSince(client),
        LedgerClientJwt.getTermination(client),
        LedgerReader.damlLfTypeLookup(() => packageService.packageStore),
        contractDao,
      )

      partiesService = new PartiesService(
        LedgerClientJwt.listKnownParties(client),
        LedgerClientJwt.getParties(client),
        LedgerClientJwt.allocateParty(client),
      )

      packageManagementService = new PackageManagementService(
        LedgerClientJwt.listPackages(pkgManagementClient),
        LedgerClientJwt.getPackage(pkgManagementClient),
        uploadDarAndReloadPackages(
          LedgerClientJwt.uploadDar(pkgManagementClient),
          jwt => implicit lc => packageService.reload(jwt),
        ),
      )

      ledgerHealthService = HealthGrpc.stub(client.channel)

      healthService = new HealthService(
        () => ledgerHealthService.check(HealthCheckRequest()),
        contractDao,
        healthTimeoutSeconds,
      )

      (encoder, decoder) = buildJsonCodecs(packageService)

      jsonEndpoints = new Endpoints(
        allowNonHttps,
        validateJwt,
        commandService,
        contractsService,
        partiesService,
        packageManagementService,
        healthService,
        encoder,
        decoder,
        logLevel.exists(!_.isGreaterOrEqual(LogLevel.INFO)), // Everything below DEBUG enables this
      )

      websocketService = new WebSocketService(
        contractsService,
        packageService.resolveTemplateId,
        decoder,
        LedgerReader.damlLfTypeLookup(() => packageService.packageStore),
        wsConfig,
      )

      websocketEndpoints = new WebsocketEndpoints(
        validateJwt,
        websocketService,
      )

      defaultEndpoints =
        concat(
          jsonEndpoints.all,
          websocketEndpoints.transactionWebSocket,
        )

      allEndpoints = concat(
        staticContentConfig.cata(
          c => concat(StaticContentEndpoints.all(c), defaultEndpoints),
          defaultEndpoints,
        ),
        EndpointsCompanion.notFound,
      )

      binding <- liftET[Error](
        Http()
          .newServerAt(address, httpPort)
          .withSettings(settings)
          .bind(allEndpoints)
      )

      _ <- either(portFile.cata(f => createPortFile(f, binding), \/-(()))): ET[Unit]

    } yield (binding, contractDao)

    bindingEt.run: Future[Error \/ (ServerBinding, Option[ContractDao])]
  }

  private[http] def doLoad(packageClient: PackageClient, ids: Set[String], tokenM: Option[String])(
      implicit ec: ExecutionContext
  ): Future[PackageService.ServerError \/ Option[PackageStore]] =
    LedgerReader
      .loadPackageStoreUpdates(packageClient, tokenM)(ids)
      .map(_.leftMap(e => PackageService.ServerError(e)))

  def stop(
      f: Future[Error \/ (ServerBinding, Option[ContractDao])]
  )(implicit
      ec: ExecutionContext,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[Unit] = {
    logger.info("Stopping server...")
    f.collect { case \/-((a, dao)) =>
      dao.foreach(_.close())
      a.unbind().void
    }.join
  }

  // Decode JWT without any validation
  private[http] val decodeJwt: EndpointsCompanion.ValidateJwt =
    jwt => JwtDecoder.decode(jwt).leftMap(e => EndpointsCompanion.Unauthorized(e.shows))

  private[http] def buildJsonCodecs(
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
      packageService.resolveTemplateId,
      packageService.resolveTemplateRecordType,
      packageService.resolveChoiceArgType,
      packageService.resolveKeyType,
      jsValueToApiValueConverter.jsValueToApiValue,
      jsValueToApiValueConverter.jsValueToLfValue,
    )

    (encoder, decoder)
  }

  private def ledgerClient(
      ledgerHost: String,
      ledgerPort: Int,
      clientConfig: LedgerClientConfiguration,
      nonRepudiationConfig: nonrepudiation.Configuration.Cli,
  )(implicit
      ec: ExecutionContext,
      aesf: ExecutionSequencerFactory,
      lc: LoggingContextOf[InstanceUUID],
  ): Future[Error \/ DamlLedgerClient] =
    LedgerClient
      .fromRetried(
        ledgerHost,
        ledgerPort,
        clientConfig,
        nonRepudiationConfig,
        MaxInitialLedgerConnectRetryAttempts,
      )
      .map(
        _.leftMap(Error.fromLedgerClientError)
      )

  private def createPortFile(
      file: Path,
      binding: akka.http.scaladsl.Http.ServerBinding,
  ): Error \/ Unit = {
    import util.ErrorOps._
    PortFiles.write(file, Port(binding.localAddress.getPort)).liftErr(Error.apply)
  }

  private def uploadDarAndReloadPackages(
      f: LedgerClientJwt.UploadDarFile,
      g: Jwt => LoggingContextOf[InstanceUUID] => Future[PackageService.Error \/ Unit],
  )(implicit ec: ExecutionContext): LedgerClientJwt.UploadDarFile =
    (x, y) => implicit lc => f(x, y)(lc).flatMap(_ => g(x)(lc).flatMap(toFuture(_): Future[Unit]))
}
