// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.http.PackageService.TemplateIdMap
import com.digitalasset.http.json.{
  ApiValueToJsValueConverter,
  DomainJsonDecoder,
  DomainJsonEncoder,
  JsValueToApiValueConverter
}
import com.digitalasset.http.util.ApiValueToLfValueConverter
import com.digitalasset.http.util.FutureUtil._
import com.digitalasset.http.util.LedgerIds.convertLedgerId
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

  def start(ledgerHost: String, ledgerPort: Int, applicationId: ApplicationId, httpPort: Int)(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext): Future[Error \/ ServerBinding] = {

    import EitherT._

    val clientConfig = LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
      commandClient = CommandClientConfiguration.default,
      sslContext = None
    )

    val bindingS: EitherT[Future, Error, ServerBinding] = for {
      client <- liftET[Error](
        LedgerClient.singleHost(ledgerHost, ledgerPort, clientConfig)(ec, aesf))

      ledgerId = convertLedgerId(client.ledgerId): lar.LedgerId

      packageStore <- eitherT(LedgerReader.createPackageStore(client.packageClient))
        .leftMap(httpServiceError)

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
        commandService,
        contractsService,
        encoder,
        decoder,
      )

      binding <- liftET[Error](
        Http().bindAndHandle(Flow.fromFunction(endpoints.all), "localhost", httpPort))

    } yield binding

    val bindingF: Future[Error \/ ServerBinding] = bindingS.run

    bindingF.onComplete {
      case u.Failure(e) => logger.error("Cannot start server", e)
      case u.Success(-\/(e)) => logger.info(s"Cannot start server: $e")
      case u.Success(\/-(a)) => logger.info(s"Started server: $a")
    }

    bindingF
  }

  private def httpServiceError(e: String): Error = Error(e)

  def stop(f: Future[Error \/ ServerBinding])(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info("Stopping server...")
    f.collect { case \/-(a) => a.unbind() }.join
  }

  private[http] def buildJsonCodecs(
      ledgerId: lar.LedgerId,
      packageStore: PackageStore,
      templateIdMap: TemplateIdMap): (DomainJsonEncoder, DomainJsonDecoder) = {

    val resolveTemplateId = PackageService.resolveTemplateId(templateIdMap) _
    val lfTypeLookup = LedgerReader.damlLfTypeLookup(packageStore) _
    val jsValueToApiValueConverter = new JsValueToApiValueConverter(lfTypeLookup)
    val jsObjectToApiRecord = jsValueToApiValueConverter.jsObjectToApiRecord _
    val apiValueToLfValue = ApiValueToLfValueConverter.apiValueToLfValue(ledgerId)
    val apiValueToJsValueConverter = new ApiValueToJsValueConverter(apiValueToLfValue)
    val apiValueToJsValue = apiValueToJsValueConverter.apiValueToJsValue _
    val apiRecordToJsObject = apiValueToJsValueConverter.apiRecordToJsObject _

    val encoder = new DomainJsonEncoder(apiRecordToJsObject, apiValueToJsValue)
    val decoder = new DomainJsonDecoder(resolveTemplateId, jsObjectToApiRecord)

    (encoder, decoder)
  }
}
