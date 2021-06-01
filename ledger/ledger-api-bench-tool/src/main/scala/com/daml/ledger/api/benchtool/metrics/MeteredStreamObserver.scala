// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.typed.{ActorRef, ActorSystem}
import com.daml.ledger.api.benchtool.util.ObserverWithResult
import org.slf4j.Logger

import scala.concurrent.Future

class MeteredStreamObserver[T](
    val streamName: String,
    logger: Logger,
    metricsManager: ActorRef[MetricsManager.Message],
)(implicit system: ActorSystem[_])
    extends ObserverWithResult[T, MetricsManager.Message.MetricsResult](logger) {
  import MetricsManager.Message._

  override def onNext(value: T): Unit = {
    metricsManager ! NewValue(value)
    super.onNext(value)
  }

  override def completeWith(): Future[MetricsResult] = {
    // TODO: abstract over the ask pattern (possibly a container with the actor and a method for asking)
    import akka.actor.typed.scaladsl.AskPattern._
    import akka.util.Timeout

    import scala.concurrent.duration._
    logger.debug(withStreamName(s"Sending $StreamCompleted notification."))
    implicit val timeout: Timeout = 3.seconds
    metricsManager.ask(StreamCompleted(_))
  }

}
