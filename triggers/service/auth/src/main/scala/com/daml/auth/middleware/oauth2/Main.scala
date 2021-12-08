// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import akka.actor.ActorSystem
import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

object Main extends StrictLogging {
  def main(args: Array[String]): Unit = {
    val cli = Cli.parse(args)
    cli.map(_.loadConfig) match {
      case Some(Right(cfg)) => main(cfg)
      case Some(Left(err)) =>
        logger.error(s"Error starting oauth2-middleware: ${err.msg}")
        sys.exit(1)
      case None =>
        sys.exit(1)
    }
  }

  private def main(config: Config): Unit = {
    implicit val system: ActorSystem = ActorSystem("system")
    implicit val executionContext: ExecutionContext = system.dispatcher

    def terminate() = Await.result(system.terminate(), 10.seconds)

    val bindingFuture = Server.start(config)

    sys.addShutdownHook {
      Server
        .stop(bindingFuture)
        .onComplete { _ =>
          terminate()
        }
    }

    logger.debug(s"Configuration $config")

    bindingFuture.onComplete {
      case Success(binding) =>
        logger.info(s"Started server: $binding")
      case Failure(e) =>
        logger.error(s"Failed to start server: $e")
        terminate()
    }
  }
}
