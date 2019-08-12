// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.typesafe.scalalogging.StrictLogging
import scalaz.\/

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Main extends StrictLogging {

  object ErrorCodes {
    val InvalidUsage = 100
    val StartupError = 101
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      logger.error("Usage: LEDGER_HOST LEDGER_PORT HTTP_PORT")
      System.exit(ErrorCodes.InvalidUsage)
    }

    val ledgerHost = args(0)
    val ledgerPort = args(1).toInt
    val httpPort = args(2).toInt

    logger.info(s"ledgerHost: $ledgerHost, ledgerPort: $ledgerPort, httpPort: $httpPort")

    implicit val asys: ActorSystem = ActorSystem("dummy-http-json-ledger-api")
    implicit val mat: ActorMaterializer = ActorMaterializer()
    implicit val aesf: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("clientPool")(asys)
    implicit val ec: ExecutionContext = asys.dispatcher

    val applicationId = ApplicationId("HTTP-JSON-API-Gateway")

    val serviceF: Future[HttpService.Error \/ ServerBinding] =
      HttpService.start(ledgerHost, ledgerPort, applicationId, httpPort)

    sys.addShutdownHook {
      HttpService
        .stop(serviceF)
        .onComplete { fa =>
          logFailure("Shutdown error", fa)
          asys.terminate()
          ()
        }
    }

    serviceF.onComplete {
      case Success(_) =>
      case Failure(_) =>
        // no reason to log this failure, HttpService.start supposed to report it
        Await.result(asys.terminate(), 10.seconds)
        System.exit(ErrorCodes.StartupError)
    }
  }

  private def logFailure[A](msg: String, fa: Try[A]): Unit = fa match {
    case Failure(e) => logger.error(msg, e)
    case _ =>
  }
}
