// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext

object Main extends App with StrictLogging {

  if (args.length != 3) {
    logger.error("Usage: LEDGER_HOST LEDGER_PORT HTTP_PORT")
    System.exit(-1)
  }

  private val ledgerHost = args(0)
  private val ledgerPort = args(1).toInt
  private val httpPort = args(2).toInt

  logger.info(s"ledgerHost: $ledgerHost, ledgerPort: $ledgerPort, httpPort: $httpPort")

  implicit val asys: ActorSystem = ActorSystem("dummy-http-json-ledger-api")
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val aesf: ExecutionSequencerFactory = new AkkaExecutionSequencerPool("clientPool")(asys)
  implicit val ec: ExecutionContext = asys.dispatcher

  private val applicationId = ApplicationId("HTTP-JSON-API-Gateway")

  val serviceF = HttpService.start(ledgerHost, ledgerPort, applicationId, httpPort)

  sys.addShutdownHook {
    HttpService
      .stop(serviceF)
      .onComplete(_ => asys.terminate())
  }
}
