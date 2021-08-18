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
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}

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

  final case class LazyF[F[_], A, B](private val fn: A => F[B]) {
    @volatile private var value: F[B] = _

    // For calls providing the argument always, this
    // function will be called, which is after initialization
    // set to be a simple getter function. That should perform
    // in the long run better than having to check the variables value
    // every time.
    @volatile private var getValueFun: A => F[B] =
      arg =>
        // Try to get the value first without synchronization
        // We don't want to unnecessarily block threads.
        if (value == null)
          // Now if there is an argument, we will set the value.
          // However, before doing that we ensure once again
          // that no value is available, but this time synchronized.
          // This guarantees that we create the value at most only once.
          synchronized {
            if (value == null) {
              val res = fn(arg)
              value = res
              getValueFun = _ => value
              res
            } else value
          }
        else value

    def get(arg: A): F[B] = getValueFun(arg)

    def get(): F[B] = if (value == null)
      throw new Exception("No value provided for unfinished lazy function")
    else value
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
      channel <- EitherT.rightT(Future.successful {
        val builder = NettyChannelBuilder.forAddress(ledgerHost, ledgerPort)
        clientConfig.sslContext.fold(builder.usePlaintext())(
          builder.sslContext(_).negotiationType(NegotiationType.TLS)
        )
        builder.maxInboundMetadataSize(clientConfig.maxInboundMetadataSize)
        builder.maxInboundMessageSize(clientConfig.maxInboundMessageSize)
        builder.build()
      })

      lazyLedgerClient =
        LazyF[Future, Jwt, DamlLedgerClient] { case Jwt(token) =>
          LedgerClient
            .fromRetried(
              channel,
              ledgerHost,
              ledgerPort,
              clientConfig.copy(token = Some(token)),
              MaxInitialLedgerConnectRetryAttempts,
            )
            .map(_.valueOr(err => throw new Exception(err.message)))
        }

      _ = logger.info(s"contractDao: ${contractDao.toString}")

      packageService = new PackageService(jwt =>
        ids =>
          lazyLedgerClient
            .get(jwt)
            .flatMap(pkgManagementClient =>
              doLoad(pkgManagementClient.packageClient, ids, some(jwt.value))
            )
      )

      commandService = new CommandService(
        (jwt, req) =>
          lazyLedgerClient
            .get(jwt)
            .flatMap(LedgerClientJwt.submitAndWaitForTransaction(_)(jwt, req)),
        (jwt, req) =>
          lazyLedgerClient
            .get(jwt)
            .flatMap(LedgerClientJwt.submitAndWaitForTransactionTree(_)(jwt, req)),
      )

      contractsService = new ContractsService(
        packageService.resolveTemplateId,
        packageService.allTemplateIds,
        (jwt, filter, verbose) =>
          akka.stream.scaladsl.Source
            .future(lazyLedgerClient.get(jwt))
            .flatMapConcat(client =>
              LedgerClientJwt.getActiveContracts(client)(jwt, filter, verbose)
            ),
        (jwt, filter, offset, terminates) =>
          akka.stream.scaladsl.Source
            .future(lazyLedgerClient.get(jwt))
            .flatMapConcat(client =>
              LedgerClientJwt.getCreatesAndArchivesSince(client)(jwt, filter, offset, terminates)
            ),
        jwt =>
          lazyLedgerClient
            .get(jwt)
            .flatMap(LedgerClientJwt.getTermination(_) apply jwt),
        LedgerReader.damlLfTypeLookup(() => packageService.packageStore),
        contractDao,
      )

      partiesService = new PartiesService(
        jwt =>
          lazyLedgerClient
            .get(jwt)
            .flatMap(LedgerClientJwt.listKnownParties(_) apply jwt),
        (jwt, partyIds) =>
          lazyLedgerClient
            .get(jwt)
            .flatMap(LedgerClientJwt.getParties(_)(jwt, partyIds)),
        (jwt, identifierHint, displayName) =>
          lazyLedgerClient
            .get(jwt)
            .flatMap(LedgerClientJwt.allocateParty(_)(jwt, identifierHint, displayName)),
      )

      packageManagementService = new PackageManagementService(
        jwt =>
          lc =>
            lazyLedgerClient
              .get(jwt)
              .flatMap(pkgManagementClient =>
                LedgerClientJwt.listPackages(pkgManagementClient)(jwt)(lc)
              ),
        { case (jwt, packageId) =>
          lc =>
            lazyLedgerClient
              .get(jwt)
              .flatMap(pkgManagementClient =>
                LedgerClientJwt.getPackage(pkgManagementClient)(jwt, packageId)(lc)
              )
        },
        { case (jwt, byteString) =>
          lc =>
            lazyLedgerClient
              .get(jwt)
              .flatMap { pkgManagementClient =>
                val res =
                  uploadDarAndReloadPackages(
                    LedgerClientJwt.uploadDar(pkgManagementClient),
                    jwt => implicit lc => packageService.reload(jwt),
                  )
                res(jwt, byteString)(lc)
              }
        },
      )

      ledgerHealthService = HealthGrpc.stub(channel)

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
