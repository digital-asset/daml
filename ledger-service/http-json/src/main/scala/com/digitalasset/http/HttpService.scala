// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.{ActorSystem, Cancellable}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.Materializer
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.http.Main.JdbcConfig
import com.digitalasset.http.dbbackend.ContractDao
import com.digitalasset.http.json.{
  ApiValueToJsValueConverter,
  DomainJsonDecoder,
  DomainJsonEncoder,
  JsValueToApiValueConverter
}
import com.digitalasset.http.util.ApiValueToLfValueConverter
import com.digitalasset.http.util.FutureUtil._
import com.digitalasset.http.util.IdentifierConverters.apiLedgerId
import com.digitalasset.jwt.JwtDecoder
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.ledger.client.services.admin.PartyManagementClient
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.ledger.service.LedgerReader
import com.typesafe.scalalogging.StrictLogging
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.{util => u}

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
      jdbcConfig: Option[JdbcConfig] = None,
      packageReloadInterval: FiniteDuration = DefaultPackageReloadInterval,
      maxInboundMessageSize: Int = DefaultMaxInboundMessageSize,
      validateJwt: Endpoints.ValidateJwt = decodeJwt)(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext
  ): Future[Error \/ ServerBinding] = {

    val clientConfig = LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
      commandClient = CommandClientConfiguration.default,
      sslContext = None
    )

    val bindingEt: EitherT[Future, Error, ServerBinding] = for {
      clientChannel <- either(
        clientChannel(ledgerHost, ledgerPort, clientConfig, maxInboundMessageSize)
      ): ET[io.grpc.Channel]

      client <- rightT(LedgerClient.forChannel(clientConfig, clientChannel)): ET[LedgerClient]

      ledgerId = apiLedgerId(client.ledgerId): lar.LedgerId

      _ = logger.info(s"Connected to Ledger: ${ledgerId: lar.LedgerId}")

      packageService = new PackageService(loadPackageStoreUpdates(client.packageClient))

      partyManagement <- either(partyManagementClient(clientChannel)): ET[PartyManagementClient]

      // load all packages right away
      _ <- eitherT(packageService.reload).leftMap(e => Error(e.shows)): ET[Unit]

      _ = schedulePackageReload(packageService, packageReloadInterval)

      contractDao = jdbcConfig.map(c => ContractDao(c.driver, c.url, c.user, c.password))
      _ <- rightT(initalizeDbIfConfigured(contractDao))

      commandService = new CommandService(
        packageService.resolveTemplateId,
        LedgerClientJwt.submitAndWaitForTransaction(clientConfig, clientChannel),
        TimeProvider.UTC)

      contractsService = new ContractsService(
        packageService.resolveTemplateIds,
        LedgerClientJwt.getActiveContracts(clientConfig, clientChannel),
        LedgerReader.damlLfTypeLookup(packageService.packageStore _)
      )

      partiesService = new PartiesService(() => partyManagement.listKnownParties())

      (encoder, decoder) = buildJsonCodecs(ledgerId, packageService)

      endpoints = new Endpoints(
        ledgerId,
        validateJwt,
        commandService,
        contractsService,
        partiesService,
        encoder,
        decoder,
      )

      binding <- liftET[Error](Http().bindAndHandleAsync(endpoints.all, address, httpPort))

    } yield binding

    val bindingF: Future[Error \/ ServerBinding] = bindingEt.run

    bindingF.onComplete {
      case u.Failure(e) => logger.error("Cannot start server", e)
      case u.Success(-\/(e)) => logger.info(s"Cannot start server: $e")
      case u.Success(\/-(a)) => logger.info(s"Started server: $a")
    }

    bindingF
  }

  private[http] def loadPackageStoreUpdates(packageClient: PackageClient)(
      implicit ec: ExecutionContext): PackageService.ReloadPackageStore =
    (ids: Set[String]) =>
      LedgerReader
        .loadPackageStoreUpdates(packageClient)(ids)
        .map(_.leftMap(e => PackageService.ServerError(e)))

  def stop(f: Future[Error \/ ServerBinding])(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info("Stopping server...")
    f.collect { case \/-(a) => a.unbind().void }.join
  }

  // Decode JWT without any validation
  private val decodeJwt: Endpoints.ValidateJwt =
    jwt => JwtDecoder.decode(jwt).leftMap(e => Endpoints.Unauthorized(e.shows))

  private[http] def buildJsonCodecs(
      ledgerId: lar.LedgerId,
      packageService: PackageService): (DomainJsonEncoder, DomainJsonDecoder) = {

    val lfTypeLookup = LedgerReader.damlLfTypeLookup(packageService.packageStore _) _
    val jsValueToApiValueConverter = new JsValueToApiValueConverter(lfTypeLookup)
    val jsObjectToApiRecord = jsValueToApiValueConverter.jsObjectToApiRecord _
    val jsValueToApiValue = jsValueToApiValueConverter.jsValueToApiValue _
    val apiValueToJsValueConverter = new ApiValueToJsValueConverter(
      ApiValueToLfValueConverter.apiValueToLfValue)
    val apiValueToJsValue = apiValueToJsValueConverter.apiValueToJsValue _
    val apiRecordToJsObject = apiValueToJsValueConverter.apiRecordToJsObject _

    val encoder = new DomainJsonEncoder(apiRecordToJsObject, apiValueToJsValue)
    val decoder = new DomainJsonDecoder(
      packageService.resolveTemplateId,
      jsObjectToApiRecord,
      jsValueToApiValue)

    (encoder, decoder)
  }

  private def schedulePackageReload(packageService: PackageService, pollInterval: FiniteDuration)(
      implicit asys: ActorSystem,
      ec: ExecutionContext): Cancellable =
    asys.scheduler.schedule(pollInterval, pollInterval) {
      val f: Future[PackageService.Error \/ Unit] = packageService.reload
      f.onComplete {
        case scala.util.Failure(e) => logger.error("Package reload failed", e)
        case scala.util.Success(-\/(e)) => logger.error("Package reload failed: " + e.shows)
        case scala.util.Success(\/-(_)) =>
      }
    }

  private def clientChannel(
      ledgerHost: String,
      ledgerPort: Int,
      clientConfig: LedgerClientConfiguration,
      maxInboundMessageSize: Int)(
      implicit ec: ExecutionContext,
      aesf: ExecutionSequencerFactory): Error \/ io.grpc.Channel =
    LedgerClientJwt
      .singleHostChannel(ledgerHost, ledgerPort, clientConfig, maxInboundMessageSize)(ec, aesf)
      .leftMap(e => Error(s"Cannot connect to the ledger server, error: ${e.getMessage}"))

  private def partyManagementClient(channel: io.grpc.Channel)(
      implicit ec: ExecutionContext): Error \/ PartyManagementClient =
    \/.fromTryCatchNonFatal(new PartyManagementClient(PartyManagementServiceGrpc.stub(channel)))
      .leftMap(e =>
        Error(s"Cannot create an instance of PartyManagementClient, error: ${e.getMessage}"))

  private def initalizeDbIfConfigured(a: Option[ContractDao]): Future[Unit] =
    a.fold(Future.successful(()))(c => c.initialize.unsafeToFuture)
}
