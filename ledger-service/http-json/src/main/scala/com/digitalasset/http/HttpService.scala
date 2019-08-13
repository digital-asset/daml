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

  final case class Error(message: String)

  def start(
      ledgerHost: String,
      ledgerPort: Int,
      applicationId: ApplicationId,
      httpPort: Int,
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

    val bindingS: EitherT[Future, Error, ServerBinding] = for {
      client <- liftET[Error](
        LedgerClient.singleHost(ledgerHost, ledgerPort, clientConfig)(ec, aesf))

      ledgerId = apiLedgerId(client.ledgerId): lar.LedgerId

      _ = logger.info(s"Connected to Ledger: ${ledgerId: lar.LedgerId}")

      packageStore <- FutureUtil
        .eitherT(LedgerReader.createPackageStore(client.packageClient))
        .leftMap(e => Error(e))

      templateIdMap = PackageService.getTemplateIdMap(packageStore)

      commandService = new CommandService(
        PackageService.resolveTemplateId(templateIdMap),
        client.commandServiceClient.submitAndWaitForTransaction,
        TimeProvider.UTC)

      contractsService = new ContractsService(
        PackageService.resolveTemplateIds(templateIdMap),
        client.activeContractSetClient)

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

    val bindingF: Future[Error \/ ServerBinding] = bindingS.run

    bindingF.onComplete {
      case u.Failure(e) => logger.error("Cannot start server", e)
      case u.Success(-\/(e)) => logger.info(s"Cannot start server: $e")
      case u.Success(\/-(a)) => logger.info(s"Started server: $a")
    }

    bindingF
  }

  def stop(f: Future[Error \/ ServerBinding])(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info("Stopping server...")
    f.collect { case \/-(a) => a.unbind() }.join
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
