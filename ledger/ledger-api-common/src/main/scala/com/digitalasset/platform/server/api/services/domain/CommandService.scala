package com.daml.platform.server.api.services.domain

import java.time.Duration

import com.daml.ledger.api.messages.command.submission.SubmitRequest
import com.daml.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
}

import scala.concurrent.Future

trait CommandService {

  def submitAndWait(
      request: SubmitRequest,
      trackingTimeout: Option[Duration],
  ): Future[SubmitAndWaitForTransactionIdResponse]

  def submitAndWaitForTransaction(
      request: SubmitRequest,
      trackingTimeout: Option[Duration],
  ): Future[SubmitAndWaitForTransactionResponse]

  def submitAndWaitForTransactionTree(
      request: SubmitRequest,
      trackingTimeout: Option[Duration],
  ): Future[SubmitAndWaitForTransactionTreeResponse]
}
