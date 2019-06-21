// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}

import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future

object DummyMain extends App with StrictLogging {

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

  private val clientF: Future[LedgerClient] =
    LedgerClient.singleHost(ledgerHost, ledgerPort, clientConfig)(ec, aesf)

  val endpoints = new Endpoints()
  val bindingFuture = Http().bindAndHandle(Flow.fromFunction(endpoints.all), "localhost", httpPort)

  sys.addShutdownHook {
    logger.info("Shutting down")
    bindingFuture.flatMap(_.unbind()).onComplete(_ => asys.terminate())
  }

  Thread.sleep(Long.MaxValue)
}
