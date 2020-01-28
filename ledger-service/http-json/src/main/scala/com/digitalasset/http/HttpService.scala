// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.nio.file.Path

import akka.actor.{ActorSystem, Cancellable}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.Materializer
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.auth.TokenHolder
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.http.dbbackend.ContractDao
import com.digitalasset.http.json.{
  ApiValueToJsValueConverter,
  DomainJsonDecoder,
  DomainJsonEncoder,
  JsValueToApiValueConverter,
}
import com.digitalasset.http.util.ApiValueToLfValueConverter
import com.digitalasset.util.ExceptionOps._
import com.digitalasset.http.util.FutureUtil._
import com.digitalasset.http.util.IdentifierConverters.apiLedgerId
import com.digitalasset.jwt.JwtDecoder
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.ledger.service.LedgerReader
import com.digitalasset.ledger.service.LedgerReader.PackageStore
import com.typesafe.scalalogging.StrictLogging
import io.grpc.netty.NettyChannelBuilder
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object HttpService extends StrictLogging {

  val DefaultPackageReloadInterval: FiniteDuration = FiniteDuration(5, "s")
  val DefaultMaxInboundMessageSize: Int = 4194304

  private type ET[A] = EitherT[Future, Error, A]

  final case class Error(message: String)

  def start(
      ledgerHost: String,
      ledgerPort: Int,
      applicationId: ApplicationId,
      address: String,
      httpPort: Int,
      wsConfig: Option[WebsocketConfig],
      accessTokenFile: Option[Path],
      contractDao: Option[ContractDao] = None,
      staticContentConfig: Option[StaticContentConfig] = None,
      packageReloadInterval: FiniteDuration = DefaultPackageReloadInterval,
      maxInboundMessageSize: Int = DefaultMaxInboundMessageSize,
      validateJwt: EndpointsCompanion.ValidateJwt = decodeJwt,
  )(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext,
  ): Future[Error \/ ServerBinding] = {

    implicit val settings: ServerSettings = ServerSettings(asys)

    val tokenHolder = accessTokenFile.map(new TokenHolder(_))

    val clientConfig = LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
      commandClient = CommandClientConfiguration.default,
      sslContext = None,
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

      partiesService = new PartiesService(() => client.partyManagementClient.listKnownParties())

      (encoder, decoder) = buildJsonCodecs(ledgerId, packageService)

      jsonEndpoints = new Endpoints(
        ledgerId,
        validateJwt,
        commandService,
        contractsService,
        partiesService,
        encoder,
        decoder,
      )

      websocketService = new WebSocketService(
        contractsService,
        packageService.resolveTemplateId,
        encoder,
        decoder,
        wsConfig,
      )

      websocketEndpoints = new WebsocketEndpoints(
        ledgerId,
        validateJwt,
        websocketService,
      )

      allEndpoints = staticContentConfig.cata(
        c =>
          StaticContentEndpoints
            .all(c) orElse jsonEndpoints.all orElse websocketEndpoints.transactionWebSocket orElse EndpointsCompanion.notFound,
        jsonEndpoints.all orElse websocketEndpoints.transactionWebSocket orElse EndpointsCompanion.notFound,
      )

      binding <- liftET[Error](
        Http().bindAndHandleAsync(allEndpoints, address, httpPort, settings = settings),
      )

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
      packageService.resolveChoiceRecordType,
      packageService.resolveKeyType,
      jsValueToApiValueConverter.jsObjectToApiRecord,
      jsValueToApiValueConverter.jsValueToApiValue,
      jsValueToApiValueConverter.jsValueToLfValue,
    )

    (encoder, decoder)
  }

  private def schedulePackageReload(
      packageService: PackageService,
      pollInterval: FiniteDuration,
  )(implicit asys: ActorSystem, ec: ExecutionContext): Cancellable = {
    val packageServiceReload: Unit = {

      val f: Future[PackageService.Error \/ Unit] = packageService.reload
      f.onComplete {
        case scala.util.Failure(e) => logger.error("Package reload failed", e)
        case scala.util.Success(-\/(e)) => logger.error("Package reload failed: " + e.shows)
        case scala.util.Success(\/-(_)) =>
      }
    }
    val runnableReload = new Runnable() { def run() = packageServiceReload }
    asys.scheduler.scheduleAtFixedRate(pollInterval, pollInterval)(runnableReload)
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
}
