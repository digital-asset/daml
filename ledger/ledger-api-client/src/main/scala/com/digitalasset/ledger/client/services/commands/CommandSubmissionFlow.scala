// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.commands

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.digitalasset.grpc.{GrpcException, GrpcStatus}
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.util.Ctx
import com.google.protobuf.empty.Empty
import com.google.rpc.status.Status

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object CommandSubmissionFlow {

  private val defaultOutput = Success(Status.defaultInstance)

  def apply[Context](
      submit: SubmitRequest => Future[Empty],
      maxInFlight: Int): Flow[Ctx[Context, SubmitRequest], Ctx[Context, Try[Empty]], NotUsed] = {
    Flow[Ctx[Context, SubmitRequest]]
      .log("submission at client", _.value.commands.fold("")(_.commandId))
      .mapAsyncUnordered(maxInFlight) {
        case ctx @ Ctx(context, request) =>
          submit(request)
            .transform { tryResponse =>
              Success(
                Ctx(
                  context,
                  tryResponse
                ))
            }(DirectExecutionContext)
      }
  }

  def getStatus(tryResponse: Try[Empty]): Try[Status] = {
    tryResponse.fold[Try[Status]](
      {
        case GrpcException(GrpcStatus(code, description), _) =>
          Success(Status(code.value(), description.getOrElse("")))
        case NonFatal(other) => Failure(other)
      },
      _ => {
        defaultOutput
      }
    )
  }
}
