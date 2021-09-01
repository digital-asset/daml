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
import com.daml.http.util.Logging.InstanceUUID
import com.daml.jwt.JwtDecoder
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.client.services.pkg.withoutledgerid.PackageClient
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
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
import com.daml.ledger.api.{domain => LedgerApiDomain}

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

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
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

      _ = logger.info(s"contractDao: ${contractDao.toString}")

      packageService = new PackageService((jwt, ledgerId) =>
        ids => doLoad(pkgManagementClient.packageClient, ids, jwt, ledgerId)
      )

      commandService = new CommandService(
        (jwt, req) => LedgerClientJwt.submitAndWaitForTransaction(client)(jwt, req),
        (jwt, req) => LedgerClientJwt.submitAndWaitForTransactionTree(client)(jwt, req),
      )

      contractsService = new ContractsService(
        packageService.resolveTemplateId,
        packageService.allTemplateIds,
        (jwt, ledgerId, filter, verbose) =>
          LedgerClientJwt.getActiveContracts(client)(jwt, ledgerId, filter, verbose),
        (jwt, ledgerId, filter, offset, terminates) =>
          LedgerClientJwt
            .getCreatesAndArchivesSince(client)(jwt, ledgerId, filter, offset, terminates),
        LedgerClientJwt.getTermination(client).apply,
        LedgerReader.damlLfTypeLookup(() => packageService.packageStore),
        contractDao,
      )

      partiesService = new PartiesService(
        LedgerClientJwt.listKnownParties(client).apply,
        (jwt, partyIds) => LedgerClientJwt.getParties(client)(jwt, partyIds),
        (jwt, identifierHint, displayName) =>
          LedgerClientJwt.allocateParty(client)(jwt, identifierHint, displayName),
      )

      packageManagementService = new PackageManagementService(
        (jwt, ledgerId) =>
          lc => LedgerClientJwt.listPackages(pkgManagementClient)(jwt, ledgerId)(lc),
        { case (jwt, ledgerId, packageId) =>
          lc => LedgerClientJwt.getPackage(pkgManagementClient)(jwt, ledgerId, packageId)(lc)
        },
        { case (jwt, ledgerId, byteString) =>
          implicit lc =>
            LedgerClientJwt
              .uploadDar(pkgManagementClient)(jwt, ledgerId, byteString)(lc)
              .flatMap(_ => packageService.reload(jwt, ledgerId))
              .map(_ => ())
        },
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

  private[http] def doLoad(
      packageClient: PackageClient,
      ids: Set[String],
      jwt: Jwt,
      ledgerId: LedgerApiDomain.LedgerId,
  )(implicit
      ec: ExecutionContext
  ): Future[PackageService.ServerError \/ Option[PackageStore]] =
    LedgerReader
      .loadPackageStoreUpdates(
        packageClient,
        some(jwt.value),
        ledgerId,
      )(ids)
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

}
