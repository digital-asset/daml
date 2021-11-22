// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.dec.DirectExecutionContext
import com.daml.telemetry.TelemetryContext
import com.daml.util.Ctx
import com.google.protobuf.empty.Empty

import scala.concurrent.Future
import scala.util.{Success, Try}

object CommandSubmissionFlow {

  def apply[Context, Submission](
      submit: TelemetryContext => Submission => Future[Empty],
      maxInFlight: Int,
  ): Flow[Ctx[Context, Submission], Ctx[Context, Try[Empty]], NotUsed] = {
    Flow[Ctx[Context, Submission]]
      //.log("submission at client", _.value.commands.commandId)
      .mapAsyncUnordered(maxInFlight) { case Ctx(context, submission, telemetryContext) =>
        submit(telemetryContext)(submission)
          .transform { tryResponse =>
            Success(
              Ctx(
                context,
                tryResponse,
                telemetryContext,
              )
            )
          }(DirectExecutionContext)
      }
  }

}
