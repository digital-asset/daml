// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.platform.server.api.validation.ErrorFactories

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, TimeoutException}

object SynchronousResponse {

  def pollUntilPersisted[T](
      source: Source[T, _],
      timeToLive: FiniteDuration,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): Future[T] =
    source
      .completionTimeout(timeToLive)
      .runWith(Sink.head)
      .recoverWith {
        case _: TimeoutException =>
          Future.failed(ErrorFactories.aborted("Request timed out"))
      }

}
