// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.client.services.commands

import com.daml.ledger.api.v2.command_submission_service.{SubmitRequest, SubmitResponse}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.util.Ctx
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import scala.concurrent.Future
import scala.util.{Success, Try}

object CommandSubmissionFlow {

  def apply[Context](
      submit: SubmitRequest => Future[SubmitResponse],
      maxInFlight: Int,
      loggerFactory: NamedLoggerFactory,
  ): Flow[Ctx[Context, CommandSubmission], Ctx[Context, Try[SubmitResponse]], NotUsed] = {
    Flow[Ctx[Context, CommandSubmission]]
      .log("submission at client", _.value.commands.commandId)
      .mapAsyncUnordered(maxInFlight) { case Ctx(context, submission, telemetryContext) =>
        telemetryContext
          .runInOpenTelemetryScope {
            submit(SubmitRequest.of(Some(submission.commands)))
              .transform { tryResponse =>
                Success(
                  Ctx(
                    context,
                    tryResponse,
                    telemetryContext,
                  )
                )
              }(DirectExecutionContext(loggerFactory.getLogger(getClass)))
          }
      }
  }
}
