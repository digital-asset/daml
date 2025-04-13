// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.services

import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForReassignmentRequest,
  SubmitAndWaitForReassignmentResponse,
  SubmitAndWaitForTransactionRequest,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
  SubmitAndWaitResponse,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

trait CommandService {
  def submitAndWait(request: SubmitAndWaitRequest)(
      loggingContext: LoggingContextWithTrace
  ): Future[SubmitAndWaitResponse]

  def submitAndWaitForTransaction(
      request: SubmitAndWaitForTransactionRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitForTransactionResponse]

  def submitAndWaitForReassignment(
      request: SubmitAndWaitForReassignmentRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitForReassignmentResponse]

  def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitForTransactionTreeResponse]
}
