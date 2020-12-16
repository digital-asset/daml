// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.retrying

import java.time.Instant

import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.validation.CommandsValidator
import com.daml.ledger.client.binding.log.Labels.{
  ERROR_CODE,
  ERROR_DETAILS,
  ERROR_MESSAGE,
  WORKFLOW_ID
}
import com.google.rpc.status.Status
import com.typesafe.scalalogging.LazyLogging

object RetryLogger extends LazyLogging {

  def logFatal(request: SubmitRequest, status: Status, nrOfRetries: Int): Unit = {
    logger.warn(
      s"Encountered fatal error when submitting command after $nrOfRetries retries, therefore retry halted: " +
        format(request, status))
  }

  def logStopRetrying(
      request: SubmitRequest,
      status: Status,
      nrOfRetries: Int,
      firstSubmissionTime: Instant): Unit = {
    logger.warn(
      s"Retrying of command stopped after $nrOfRetries retries. Attempting since $firstSubmissionTime: " +
        format(request, status)
    )
  }

  def logNonFatal(request: SubmitRequest, status: Status, nrOfRetries: Int): Unit = {
    logger.warn(
      s"Encountered non-fatal error when submitting command after $nrOfRetries retries, therefore will retry: " +
        format(request, status)
    )
  }

  private def format(request: SubmitRequest, status: Status): String = {
    val effectiveActAs = request.commands.map(c => CommandsValidator.effectiveSubmitters(c).actAs)
    format(
      (BIM, request.commands.map(_.commandId)),
      (ACT_AS, effectiveActAs),
      (WORKFLOW_ID, request.commands.map(_.workflowId)),
      (ERROR_CODE, status.code),
      (ERROR_MESSAGE, status.message),
      (ERROR_DETAILS, status.details.mkString(","))
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable", "org.wartremover.warts.Any"))
  private def format(fs: (String, Any)*): String = fs.map(f => s"${f._1} = ${f._2}").mkString(", ")

  private val ACT_AS = "act-as"
  private val BIM = "bim"
}
