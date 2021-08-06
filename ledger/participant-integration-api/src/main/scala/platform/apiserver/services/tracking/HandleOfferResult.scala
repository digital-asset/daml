// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import akka.stream.QueueOfferResult
import com.daml.platform.server.api.ApiException
import io.grpc.{Status => GrpcStatus}

import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}

private[tracking] object HandleOfferResult {
  val toGrpcStatus: PartialFunction[Try[QueueOfferResult], Option[GrpcStatus]] = {
    case Failure(t) =>
      t match {
        case i: IllegalStateException
            if i.getMessage == "You have to wait for previous offer to be resolved to send another request" =>
          Some(
            GrpcStatus.RESOURCE_EXHAUSTED
              .withDescription("Ingress buffer is full")
          )
        case _ =>
          Some(
            GrpcStatus.ABORTED
              .withDescription(s"Failure: ${t.getClass.getSimpleName}: ${t.getMessage}")
              .withCause(t)
          )

      }
    case Success(QueueOfferResult.Failure(t)) =>
      Some(
        GrpcStatus.ABORTED
          .withDescription(s"Failed to enqueue: ${t.getClass.getSimpleName}: ${t.getMessage}")
          .withCause(t)
      )
    case Success(QueueOfferResult.Dropped) =>
      Some(
        GrpcStatus.RESOURCE_EXHAUSTED
          .withDescription("Ingress buffer is full")
      )
    case Success(QueueOfferResult.QueueClosed) =>
      Some(GrpcStatus.ABORTED.withDescription("Queue closed"))
    case Success(QueueOfferResult.Enqueued) => None // Promise will be completed downstream.
  }

  def completePromise(promise: Promise[_]): PartialFunction[Try[QueueOfferResult], Unit] =
    toGrpcStatus.andThen(_.foreach(s => promise.tryFailure(new ApiException(s))))
}
