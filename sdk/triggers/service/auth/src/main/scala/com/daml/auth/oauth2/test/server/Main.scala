// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.oauth2.test.server

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

object Main extends StrictLogging {
  def main(args: Array[String]): Unit = {
    Config.parseConfig(args) match {
      case Some(config) => main(config)
      case None => sys.exit(1)
    }
  }

  private def main(config: Config): Unit = {
    implicit val system: ActorSystem = ActorSystem("system")
    implicit val executionContext: ExecutionContext = system.dispatcher

    def terminate() = Await.result(system.terminate(), 10.seconds)

    val bindingFuture = Server(config).start()

    sys.addShutdownHook {
      Server
        .stop(bindingFuture)
        .onComplete { _ =>
          terminate()
        }
    }

    bindingFuture.onComplete {
      case Success(binding) =>
        logger.info(s"Started server: $binding")
      case Failure(e) =>
        logger.error(s"Failed to start server: $e")
        terminate()
    }
  }
}
