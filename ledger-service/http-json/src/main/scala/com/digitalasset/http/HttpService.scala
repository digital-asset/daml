// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.file.Path

import akka.actor.{ActorSystem, Cancellable}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.auth.TokenHolder
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.http.Statement.discard
import com.daml.http.dbbackend.ContractDao
import com.daml.http.json.{
  ApiValueToJsValueConverter,
  DomainJsonDecoder,
  DomainJsonEncoder,
  JsValueToApiValueConverter
}
import com.daml.http.util.ApiValueToLfValueConverter
import com.daml.util.ExceptionOps._
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
  LedgerIdRequirement
}
import com.daml.ledger.client.services.pkg.PackageClient
import com.daml.ledger.service.LedgerReader
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.ports.{Port, PortFiles}
import com.typesafe.scalalogging.StrictLogging
import io.grpc.netty.NettyChannelBuilder
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

object HttpService extends StrictLogging {

  val DefaultPackageReloadInterval: FiniteDuration = FiniteDuration(5, "s")
  val DefaultMaxInboundMessageSize: Int = 4194304

  private type ET[A] = EitherT[Future, Error, A]

  final case class Error(message: String)

  // defined separately from Config so
  //  1. it is absolutely lexically apparent what `import startSettings._` means
  //  2. avoid incorporating other Config'd things into "the shared args to start"
  trait StartSettings {
    val ledgerHost: String
    val ledgerPort: Int
    val applicationId: ApplicationId
    val address: String
    val httpPort: Int
    val portFile: Option[Path]
    val tlsConfig: TlsConfiguration
    val wsConfig: Option[WebsocketConfig]
    val accessTokenFile: Option[Path]
    val allowNonHttps: Boolean
    val staticContentConfig: Option[StaticContentConfig]
    val packageReloadInterval: FiniteDuration
    val maxInboundMessageSize: Int
  }

  trait DefaultStartSettings extends StartSettings {
    override val staticContentConfig: Option[StaticContentConfig] = None
    override val packageReloadInterval: FiniteDuration = DefaultPackageReloadInterval
    override val maxInboundMessageSize: Int = DefaultMaxInboundMessageSize
  }

  def start(
      startSettings: StartSettings,
      contractDao: Option[ContractDao] = None,
      validateJwt: EndpointsCompanion.ValidateJwt = decodeJwt,
  )(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext,
  ): Future[Error \/ ServerBinding] = {
    import startSettings._

    implicit val settings: ServerSettings = ServerSettings(asys)

    val tokenHolder = accessTokenFile.map(new TokenHolder(_))

    val clientConfig = LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
      commandClient = CommandClientConfiguration.default,
      sslContext = tlsConfig.client,
      token = tokenHolder.flatMap(_.token),
    )

    val bindingEt: EitherT[Future, Error, ServerBinding] = for {
      client <- eitherT(client(ledgerHost, ledgerPort, clientConfig, maxInboundMessageSize)): ET[
        LedgerClient,
      ]

      ledgerId = apiLedgerId(client.ledgerId): lar.LedgerId

      _ = logger.info(s"Connected to Ledger: ${ledgerId: lar.LedgerId}")
      _ = logger.info(s"contractDao: ${contractDao.toString}")

      packageService = new PackageService(
        loadPackageStoreUpdates(client.packageClient, tokenHolder),
      )

      // load all packages right away
      _ <- eitherT(packageService.reload).leftMap(e => Error(e.shows)): ET[Unit]

      _ = schedulePackageReload(packageService, packageReloadInterval)

      commandService = new CommandService(
        packageService.resolveTemplateId,
        LedgerClientJwt.submitAndWaitForTransaction(client),
        LedgerClientJwt.submitAndWaitForTransactionTree(client),
        TimeProvider.UTC,
      )

      contractsService = new ContractsService(
        packageService.resolveTemplateId,
        packageService.allTemplateIds,
        LedgerClientJwt.getActiveContracts(client),
        LedgerClientJwt.getCreatesAndArchivesSince(client),
        LedgerClientJwt.getTermination(client),
        LedgerReader.damlLfTypeLookup(packageService.packageStore _),
        contractDao,
      )

      partiesService = new PartiesService(
        LedgerClientJwt.listKnownParties(client),
        LedgerClientJwt.getParties(client),
        LedgerClientJwt.allocateParty(client)
      )

      packageManagementService = new PackageManagementService(
        LedgerClientJwt.listPackages(client),
        LedgerClientJwt.getPackage(client),
        uploadDarAndReloadPackages(
          LedgerClientJwt.uploadDar(client),
          () => packageService.reload(ec))
      )

      (encoder, decoder) = buildJsonCodecs(ledgerId, packageService)

      jsonEndpoints = new Endpoints(
        ledgerId,
        allowNonHttps,
        validateJwt,
        commandService,
        contractsService,
        partiesService,
        packageManagementService,
        encoder,
        decoder,
      )

      websocketService = new WebSocketService(
        contractsService,
        packageService.resolveTemplateId,
        encoder,
        decoder,
        LedgerReader.damlLfTypeLookup(packageService.packageStore _),
        wsConfig,
      )

      websocketEndpoints = new WebsocketEndpoints(
        ledgerId,
        validateJwt,
        websocketService,
      )

      defaultEndpoints = jsonEndpoints.all orElse websocketEndpoints.transactionWebSocket orElse EndpointsCompanion.notFound

      allEndpoints = staticContentConfig.cata(
        c => StaticContentEndpoints.all(c) orElse defaultEndpoints,
        defaultEndpoints
      )

      binding <- liftET[Error](
        Http().bindAndHandleAsync(allEndpoints, address, httpPort, settings = settings),
      )

      _ <- either(portFile.cata(f => createPortFile(f, binding), \/-(()))): ET[Unit]

    } yield binding

    bindingEt.run: Future[Error \/ ServerBinding]
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private[http] def refreshToken(
      holderM: Option[TokenHolder],
  )(implicit ec: ExecutionContext): Future[PackageService.ServerError \/ Option[String]] =
    Future(
      holderM
        .traverseU { holder =>
          holder.refresh()
          holder.token
            .map(\/-(_))
            .getOrElse(-\/(PackageService.ServerError("Unable to load token")))
        },
    )

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private[http] def doLoad(packageClient: PackageClient, ids: Set[String], tokenM: Option[String])(
      implicit ec: ExecutionContext,
  ): Future[PackageService.ServerError \/ Option[PackageStore]] =
    LedgerReader
      .loadPackageStoreUpdates(packageClient, tokenM)(ids)
      .map(_.leftMap(e => PackageService.ServerError(e)))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private[http] def loadPackageStoreUpdates(
      packageClient: PackageClient,
      holderM: Option[TokenHolder],
  )(implicit ec: ExecutionContext): PackageService.ReloadPackageStore =
    (ids: Set[String]) => refreshToken(holderM).flatMap(_.traverseM(doLoad(packageClient, ids, _)))

  def stop(f: Future[Error \/ ServerBinding])(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info("Stopping server...")
    f.collect { case \/-(a) => a.unbind().void }.join
  }

  // Decode JWT without any validation
  private val decodeJwt: EndpointsCompanion.ValidateJwt =
    jwt => JwtDecoder.decode(jwt).leftMap(e => EndpointsCompanion.Unauthorized(e.shows))

  private[http] def buildJsonCodecs(
      ledgerId: lar.LedgerId,
      packageService: PackageService,
  ): (DomainJsonEncoder, DomainJsonDecoder) = {

    val lfTypeLookup = LedgerReader.damlLfTypeLookup(packageService.packageStore _) _
    val jsValueToApiValueConverter = new JsValueToApiValueConverter(lfTypeLookup)

    val apiValueToJsValueConverter = new ApiValueToJsValueConverter(
      ApiValueToLfValueConverter.apiValueToLfValue,
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

  private def client(
      ledgerHost: String,
      ledgerPort: Int,
      clientConfig: LedgerClientConfiguration,
      maxInboundMessageSize: Int,
  )(implicit ec: ExecutionContext, aesf: ExecutionSequencerFactory): Future[Error \/ LedgerClient] =
    LedgerClient
      .fromBuilder(
        NettyChannelBuilder
          .forAddress(ledgerHost, ledgerPort)
          .maxInboundMessageSize(maxInboundMessageSize),
        clientConfig,
      )
      .map(_.right)
      .recover {
        case NonFatal(e) =>
          \/.left(Error(s"Cannot connect to the ledger server, error: ${e.description}"))
      }

  private def createPortFile(
      file: Path,
      binding: akka.http.scaladsl.Http.ServerBinding): Error \/ Unit = {
    import util.ErrorOps._
    PortFiles.write(file, Port(binding.localAddress.getPort)).liftErr(Error)
  }

  private def uploadDarAndReloadPackages(
      f: LedgerClientJwt.UploadDarFile,
      g: () => Future[PackageService.Error \/ Unit])(
      implicit ec: ExecutionContext): LedgerClientJwt.UploadDarFile =
    (x, y) => f(x, y).flatMap(_ => g().flatMap(toFuture(_): Future[Unit]))
}
