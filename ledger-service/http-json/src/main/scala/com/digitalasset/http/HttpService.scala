// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.Materializer
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.http.PackageService.TemplateIdMap
import com.digitalasset.http.json.{
  ApiValueToJsValueConverter,
  DomainJsonDecoder,
  DomainJsonEncoder,
  JsValueToApiValueConverter
}
import com.digitalasset.http.util.FutureUtil.liftET
import com.digitalasset.http.util.IdentifierConverters.apiLedgerId
import com.digitalasset.http.util.{ApiValueToLfValueConverter, FutureUtil}
import com.digitalasset.jwt.JwtDecoder
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.ledger.service.LedgerReader
import com.digitalasset.ledger.service.LedgerReader.PackageStore
import com.typesafe.scalalogging.StrictLogging
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.{ExecutionContext, Future}
import scala.{util => u}

object HttpService extends StrictLogging {

  val DefaultMaxInboundMessageSize: Int = 4194304

  private type ET[A] = EitherT[Future, Error, A]

  final case class Error(message: String)

  def start(
      ledgerHost: String,
      ledgerPort: Int,
      applicationId: ApplicationId,
      httpPort: Int,
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
      clientChannel <- FutureUtil
        .either(LedgerClientJwt
          .singleHostChannel(ledgerHost, ledgerPort, clientConfig, maxInboundMessageSize)(ec, aesf))
        .leftMap(e => Error(e.getMessage)): ET[io.grpc.Channel]

      client <- FutureUtil
        .rightT(LedgerClient.forChannel(clientConfig, clientChannel)): ET[LedgerClient]

      ledgerId = apiLedgerId(client.ledgerId): lar.LedgerId

      _ = logger.info(s"Connected to Ledger: ${ledgerId: lar.LedgerId}")

      packageStore <- FutureUtil
        .eitherT(LedgerReader.createPackageStore(client.packageClient))
        .leftMap(e => Error(e))

      templateIdMap = PackageService.getTemplateIdMap(packageStore)

      commandService = new CommandService(
        PackageService.resolveTemplateId(templateIdMap),
        LedgerClientJwt.submitAndWaitForTransaction(clientConfig, clientChannel),
        TimeProvider.UTC)

      contractsService = new ContractsService(
        PackageService.resolveTemplateIds(templateIdMap),
        LedgerClientJwt.getActiveContracts(clientConfig, clientChannel))

      (encoder, decoder) = buildJsonCodecs(ledgerId, packageStore, templateIdMap)

      endpoints = new Endpoints(
        ledgerId,
        validateJwt,
        commandService,
        contractsService,
        encoder,
        decoder,
      )

      binding <- liftET[Error](Http().bindAndHandleAsync(endpoints.all, "localhost", httpPort))

    } yield binding

    val bindingF: Future[Error \/ ServerBinding] = bindingEt.run

    bindingF.onComplete {
      case u.Failure(e) => logger.error("Cannot start server", e)
      case u.Success(-\/(e)) => logger.info(s"Cannot start server: $e")
      case u.Success(\/-(a)) => logger.info(s"Started server: $a")
    }

    bindingF
  }

  def stop(f: Future[Error \/ ServerBinding])(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info("Stopping server...")
    f.collect { case \/-(a) => a.unbind().void }.join
  }

  // Decode JWT without any validation
  private val decodeJwt: Endpoints.ValidateJwt =
    jwt => JwtDecoder.decode(jwt).leftMap(e => Endpoints.Unauthorized(e.shows))

  private[http] def buildJsonCodecs(
      ledgerId: lar.LedgerId,
      packageStore: PackageStore,
      templateIdMap: TemplateIdMap): (DomainJsonEncoder, DomainJsonDecoder) = {

    val resolveTemplateId = PackageService.resolveTemplateId(templateIdMap) _
    val lfTypeLookup = LedgerReader.damlLfTypeLookup(packageStore) _
    val jsValueToApiValueConverter = new JsValueToApiValueConverter(lfTypeLookup)
    val jsObjectToApiRecord = jsValueToApiValueConverter.jsObjectToApiRecord _
    val jsValueToApiValue = jsValueToApiValueConverter.jsValueToApiValue _
    val apiValueToJsValueConverter = new ApiValueToJsValueConverter(
      ApiValueToLfValueConverter.apiValueToLfValue)
    val apiValueToJsValue = apiValueToJsValueConverter.apiValueToJsValue _
    val apiRecordToJsObject = apiValueToJsValueConverter.apiRecordToJsObject _

    val encoder = new DomainJsonEncoder(apiRecordToJsObject, apiValueToJsValue)
    val decoder = new DomainJsonDecoder(resolveTemplateId, jsObjectToApiRecord, jsValueToApiValue)

    (encoder, decoder)
  }
}
