// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.client.configuration.CommandClientConfiguration
import com.daml.util.Ctx
import com.google.protobuf.duration.Duration

import scala.annotation.nowarn

object CommandUpdaterFlow {

  @nowarn("msg=deprecated")
  def apply[Context](
      config: CommandClientConfiguration,
      submissionIdGenerator: SubmissionIdGenerator,
      applicationId: String,
      ledgerIdToUse: LedgerId,
  ): Flow[Ctx[Context, CommandSubmission], Ctx[Context, CommandSubmission], NotUsed] =
    Flow[Ctx[Context, CommandSubmission]]
      .map(_.map { case submission @ CommandSubmission(commands, _) =>
        if (LedgerId(commands.ledgerId) != ledgerIdToUse)
          throw new IllegalArgumentException(
            s"Failing fast on submission request of command ${commands.commandId} with invalid ledger ID ${commands.ledgerId} (client expected $ledgerIdToUse)"
          )
        if (commands.applicationId != applicationId)
          throw new IllegalArgumentException(
            s"Failing fast on submission request of command ${commands.commandId} with invalid application ID ${commands.applicationId} (client expected $applicationId)"
          )
        val nonEmptySubmissionId = if (commands.submissionId.isEmpty) {
          submissionIdGenerator.generate()
        } else {
          commands.submissionId
        }
        val updatedDeduplicationPeriod = commands.deduplicationPeriod match {
          case DeduplicationPeriod.Empty =>
            DeduplicationPeriod.DeduplicationTime(
              Duration
                .of(
                  config.defaultDeduplicationTime.getSeconds,
                  config.defaultDeduplicationTime.getNano,
                )
            )
          case existing => existing
        }
        submission.copy(commands =
          commands.copy(
            submissionId = nonEmptySubmissionId,
            deduplicationPeriod = updatedDeduplicationPeriod,
          )
        )
      })
}
