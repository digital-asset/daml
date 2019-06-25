// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import com.digitalasset.http.util.FutureUtil._
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import scala.{util => u}
import com.typesafe.scalalogging.StrictLogging
import scalaz.Scalaz._
import scalaz._

import scala.concurrent.Future

object Main extends App with StrictLogging {

  type Error = String

  if (args.length != 3) {
    logger.error("Usage: LEDGER_HOST LEDGER_PORT HTTP_PORT")
    System.exit(-1)
  }

  private val ledgerHost = args(0)
  private val ledgerPort = args(1).toInt
  private val httpPort = args(2).toInt

  logger.info(s"ledgerHost: $ledgerHost, ledgerPort: $ledgerPort, httpPort: $httpPort")

  implicit val asys = ActorSystem("dummy-http-json-ledger-api")
  implicit val mat = ActorMaterializer()
  private val aesf = new AkkaExecutionSequencerPool("clientPool")(asys)
  implicit val ec = asys.dispatcher

  private val applicationId = ApplicationId("HTTP-JSON-API-Dummy-Gateway")

  private val clientConfig = LedgerClientConfiguration(
    applicationId = ApplicationId.unwrap(applicationId),
    ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
    commandClient = CommandClientConfiguration.default,
    sslContext = None
  )

//  val clientF: Future[LedgerClient] =
//    LedgerClient.singleHost(ledgerHost, ledgerPort, clientConfig)(ec, aesf)

  import EitherT._

  val bindingS: EitherT[Future, Error, ServerBinding] = for {
    client <- liftET[Error](LedgerClient.singleHost(ledgerHost, ledgerPort, clientConfig)(ec, aesf))
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

  sys.addShutdownHook {
    logger.info("Shutting down")
    bindingF
      .collect { case \/-(a) => a.unbind() }
      .join
      .onComplete(_ => asys.terminate())
  }

  Thread.sleep(Long.MaxValue)
}
