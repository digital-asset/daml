// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.file.Path

import akka.actor.{ActorSystem, Cancellable}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.Materializer
import com.daml.auth.TokenHolder
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
import com.daml.jwt.JwtDecoder
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.client.services.pkg.PackageClient
import com.daml.ledger.service.LedgerReader
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.ports.{Port, PortFiles}
import com.daml.scalautil.Statement.discard
import com.daml.util.ExceptionOps._
import com.typesafe.scalalogging.StrictLogging
import io.grpc.health.v1.health.{HealthCheckRequest, HealthGrpc}
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

import ch.qos.logback.classic.{Level => LogLevel}

object HttpService extends StrictLogging {

  val DefaultPackageReloadInterval: FiniteDuration = FiniteDuration(5, "s")
  val DefaultMaxInboundMessageSize: Int = 4194304
  val DefaultHealthTimeoutSeconds: Int = 5

  // used only to populate a required field in LedgerClientConfiguration
  private val DummyApplicationId: ApplicationId = ApplicationId("HTTP-JSON-API-Gateway")

  private type ET[A] = EitherT[Future, Error, A]

  final case class Error(message: String)

  // defined separately from Config so
  //  1. it is absolutely lexically apparent what `import startSettings._` means
  //  2. avoid incorporating other Config'd things into "the shared args to start"
  trait StartSettings {
    val ledgerHost: String
    val ledgerPort: Int
    val address: String
    val httpPort: Int
    val portFile: Option[Path]
    val tlsConfig: TlsConfiguration
    val wsConfig: Option[WebsocketConfig]
    val accessTokenFile: Option[Path]
    val allowNonHttps: Boolean
    val staticContentConfig: Option[StaticContentConfig]
    val packageReloadInterval: FiniteDuration
    val packageMaxInboundMessageSize: Option[Int]
    val maxInboundMessageSize: Int
    val healthTimeoutSeconds: Int
    val logLevel: Option[LogLevel]
  }

  trait DefaultStartSettings extends StartSettings {
    override val staticContentConfig: Option[StaticContentConfig] = None
    override val packageReloadInterval: FiniteDuration = DefaultPackageReloadInterval
    override val packageMaxInboundMessageSize: Option[Int] = None
    override val maxInboundMessageSize: Int = DefaultMaxInboundMessageSize
    override val healthTimeoutSeconds: Int = DefaultHealthTimeoutSeconds
  }

  def start(
      startSettings: StartSettings,
      contractDao: Option[ContractDao] = None,
      validateJwt: EndpointsCompanion.ValidateJwt = decodeJwt,
  )(implicit
      asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext,
  ): Future[Error \/ (ServerBinding, Option[ContractDao])] = {
    import startSettings._

    implicit val settings: ServerSettings = ServerSettings(asys).withTransparentHeadRequests(true)

    val tokenHolder = accessTokenFile.map(new TokenHolder(_))

    val clientConfig = LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(DummyApplicationId),
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = tlsConfig.client,
      token = tokenHolder.flatMap(_.token),
      maxInboundMessageSize = maxInboundMessageSize,
    )

    val bindingEt: EitherT[Future, Error, (ServerBinding, Option[ContractDao])] = for {
      client <- eitherT(
        ledgerClient(ledgerHost, ledgerPort, clientConfig)
      ): ET[LedgerClient]

      pkgManagementClient <- eitherT(
        ledgerClient(
          ledgerHost,
          ledgerPort,
          packageMaxInboundMessageSize.fold(clientConfig)(size =>
            clientConfig.copy(maxInboundMessageSize = size)
          ),
        )
      ): ET[LedgerClient]

      ledgerId = apiLedgerId(client.ledgerId): lar.LedgerId

      _ = logger.info(s"Connected to Ledger: ${ledgerId: lar.LedgerId}")
      _ = logger.info(s"contractDao: ${contractDao.toString}")

      packageService = new PackageService(
        loadPackageStoreUpdates(pkgManagementClient.packageClient, tokenHolder)
      )

      // load all packages right away
      _ <- eitherT(packageService.reload).leftMap(e => Error(e.shows)): ET[Unit]

      _ = schedulePackageReload(packageService, packageReloadInterval)

      commandService = new CommandService(
        packageService.resolveTemplateId,
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
          () => packageService.reload(ec),
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
        jsonEndpoints.all orElse websocketEndpoints.transactionWebSocket orElse EndpointsCompanion.notFound

      allEndpoints = staticContentConfig.cata(
        c => StaticContentEndpoints.all(c) orElse defaultEndpoints,
        defaultEndpoints,
      )

      binding <- liftET[Error](
        Http().newServerAt(address, httpPort).withSettings(settings).bind(allEndpoints)
      )

      _ <- either(portFile.cata(f => createPortFile(f, binding), \/-(()))): ET[Unit]

    } yield (binding, contractDao)

    bindingEt.run: Future[Error \/ (ServerBinding, Option[ContractDao])]
  }

  private[http] def refreshToken(
      holderM: Option[TokenHolder]
  )(implicit ec: ExecutionContext): Future[PackageService.ServerError \/ Option[String]] =
    Future(
      holderM
        .traverse { holder =>
          holder.refresh()
          holder.token
            .map(\/-(_))
            .getOrElse(-\/(PackageService.ServerError("Unable to load token")))
        }
    )

  private[http] def doLoad(packageClient: PackageClient, ids: Set[String], tokenM: Option[String])(
      implicit ec: ExecutionContext
  ): Future[PackageService.ServerError \/ Option[PackageStore]] =
    LedgerReader
      .loadPackageStoreUpdates(packageClient, tokenM)(ids)
      .map(_.leftMap(e => PackageService.ServerError(e)))

  private[http] def loadPackageStoreUpdates(
      packageClient: PackageClient,
      holderM: Option[TokenHolder],
  )(implicit ec: ExecutionContext): PackageService.ReloadPackageStore =
    (ids: Set[String]) => refreshToken(holderM).flatMap(_.traverseM(doLoad(packageClient, ids, _)))

  // We can reuse the token we use for packages since both
  // require only public claims
  private[http] def getLedgerEnd(client: LedgerClient, holderM: Option[TokenHolder])(implicit
      ec: ExecutionContext
  ): () => Future[Unit] =
    () => {
      for {
        token <- refreshToken(holderM).flatMap(x =>
          toFuture(x: PackageService.Error \/ Option[String])
        )
        _ <- client.transactionClient.getLedgerEnd(token)
      } yield ()
    }

  def stop(
      f: Future[Error \/ (ServerBinding, Option[ContractDao])]
  )(implicit ec: ExecutionContext): Future[Unit] = {
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
  ): (DomainJsonEncoder, DomainJsonDecoder) = {

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

  private def schedulePackageReload(
      packageService: PackageService,
      pollInterval: FiniteDuration,
  )(implicit asys: ActorSystem, ec: ExecutionContext): Cancellable = {
    val maxWait = pollInterval * 10

    // scheduleWithFixedDelay will wait for the previous task to complete before triggering the next one
    // that is exactly why the task calls Await.result on the Future
    asys.scheduler.scheduleWithFixedDelay(pollInterval, pollInterval)(() => {

      val f: Future[PackageService.Error \/ Unit] = packageService.reload

      f.onComplete {
        case scala.util.Failure(e) => logger.error("Package reload failed", e)
        case scala.util.Success(-\/(e)) => logger.error("Package reload failed: " + e.shows)
        case scala.util.Success(\/-(_)) =>
      }

      try {
        discard { Await.result(f, maxWait) }
      } catch {
        case e: scala.concurrent.TimeoutException =>
          logger.error(s"Package reload timed out after: $maxWait", e)
      }
    })
  }

  private def ledgerClient(
      ledgerHost: String,
      ledgerPort: Int,
      clientConfig: LedgerClientConfiguration,
  )(implicit ec: ExecutionContext, aesf: ExecutionSequencerFactory): Future[Error \/ LedgerClient] =
    LedgerClient
      .singleHost(ledgerHost, ledgerPort, clientConfig)
      .map(_.right)
      .recover { case NonFatal(e) =>
        \/.left(Error(s"Cannot connect to the ledger server, error: ${e.description}"))
      }

  private def createPortFile(
      file: Path,
      binding: akka.http.scaladsl.Http.ServerBinding,
  ): Error \/ Unit = {
    import util.ErrorOps._
    PortFiles.write(file, Port(binding.localAddress.getPort)).liftErr(Error)
  }

  private def uploadDarAndReloadPackages(
      f: LedgerClientJwt.UploadDarFile,
      g: () => Future[PackageService.Error \/ Unit],
  )(implicit ec: ExecutionContext): LedgerClientJwt.UploadDarFile =
    (x, y) => f(x, y).flatMap(_ => g().flatMap(toFuture(_): Future[Unit]))
}
