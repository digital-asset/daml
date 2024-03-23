// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.retrying

import java.time.Instant

import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.validation.CommandsValidator
import com.daml.ledger.client.binding.log.Labels.{
  ERROR_CODE,
  ERROR_DETAILS,
  ERROR_MESSAGE,
  WORKFLOW_ID,
}
import com.daml.ledger.client.services.commands.CommandSubmission
import com.google.rpc.status.Status
import com.typesafe.scalalogging.LazyLogging

object RetryLogger extends LazyLogging {

  def logFatal(submission: CommandSubmission, status: Status, nrOfRetries: Int): Unit = {
    logger.warn(
      s"Encountered fatal error when submitting command after $nrOfRetries retries, therefore retry halted: " +
        format(submission.commands, status)
    )
  }

  def logStopRetrying(
      submission: CommandSubmission,
      status: Status,
      nrOfRetries: Int,
      firstSubmissionTime: Instant,
  ): Unit = {
    logger.warn(
      s"Retrying of command stopped after $nrOfRetries retries. Attempting since $firstSubmissionTime: " +
        format(submission.commands, status)
    )
  }

  def logNonFatal(submission: CommandSubmission, status: Status, nrOfRetries: Int): Unit = {
    logger.warn(
      s"Encountered non-fatal error when submitting command after $nrOfRetries retries, therefore will retry: " +
        format(submission.commands, status)
    )
  }

  private def format(commands: Commands, status: Status): String =
    format(
      (BIM, commands.commandId),
      (ACT_AS, CommandsValidator.effectiveSubmitters(commands).actAs),
      (WORKFLOW_ID, commands.workflowId),
      (ERROR_CODE, status.code),
      (ERROR_MESSAGE, status.message),
      (ERROR_DETAILS, status.details.mkString(",")),
    )

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable", "org.wartremover.warts.Any"))
  private def format(fs: (String, Any)*): String = fs.map(f => s"${f._1} = ${f._2}").mkString(", ")

  private val ACT_AS = "act-as"
  private val BIM = "bim"
}
