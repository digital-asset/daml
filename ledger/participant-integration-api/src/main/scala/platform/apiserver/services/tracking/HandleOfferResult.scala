// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import akka.stream.QueueOfferResult
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionSuccess,
  StartingExecutionFailure,
  TrackedCompletionFailure,
}
import io.grpc.{Status => GrpcStatus}

import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}

private[tracking] object HandleOfferResult {
  val toGrpcStatus: PartialFunction[Try[QueueOfferResult], GrpcStatus] = {
    case Failure(t) =>
      t match {
        case i: IllegalStateException
            if i.getMessage == "You have to wait for previous offer to be resolved to send another request" =>
          GrpcStatus.RESOURCE_EXHAUSTED
            .withDescription("Ingress buffer is full")
        case _ =>
          GrpcStatus.ABORTED
            .withDescription(s"Failure: ${t.getClass.getSimpleName}: ${t.getMessage}")
            .withCause(t)
      }
    case Success(QueueOfferResult.Failure(t)) =>
      GrpcStatus.ABORTED
        .withDescription(s"Failed to enqueue: ${t.getClass.getSimpleName}: ${t.getMessage}")
        .withCause(t)
    case Success(QueueOfferResult.Dropped) =>
      GrpcStatus.RESOURCE_EXHAUSTED
        .withDescription("Ingress buffer is full")
    case Success(QueueOfferResult.QueueClosed) =>
      GrpcStatus.ABORTED.withDescription("Queue closed")
  }

  def completePromise(
      promise: Promise[Either[TrackedCompletionFailure, CompletionSuccess]]
  ): PartialFunction[Try[QueueOfferResult], Unit] =
    toGrpcStatus.andThen(status => {
      promise.trySuccess(Left(StartingExecutionFailure(status)))
      ()
    })
}
