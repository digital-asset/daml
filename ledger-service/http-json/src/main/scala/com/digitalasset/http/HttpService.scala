// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.Materializer
import akka.stream.scaladsl.Flow
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.http.util.FutureUtil._
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.typesafe.scalalogging.StrictLogging
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.{ExecutionContext, Future}
import scala.{util => u}

object HttpService extends StrictLogging {

  type Error = String

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
      packageService = new PackageService(client.packageClient)
      templateIdMap <- eitherT(packageService.getTemplateIdMap())
      contractsService = new ContractsService(templateIdMap, client.activeContractSetClient)
      endpoints = new Endpoints(contractsService)
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

  def stop(f: Future[Error \/ ServerBinding])(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info("Stopping server...")
    f.collect { case \/-(a) => a.unbind() }.join
  }
}
