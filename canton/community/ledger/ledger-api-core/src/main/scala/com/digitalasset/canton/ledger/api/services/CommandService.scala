// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.services

import com.daml.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.google.protobuf.empty.Empty

import scala.concurrent.Future

trait CommandService {
  def submitAndWait(request: SubmitAndWaitRequest)(
      loggingContext: LoggingContextWithTrace
  ): Future[Empty]

  def submitAndWaitForTransactionId(
      request: SubmitAndWaitRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitForTransactionIdResponse]

  def submitAndWaitForTransaction(
      request: SubmitAndWaitRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitForTransactionResponse]

  def submitAndWaitForTransactionTree(
      request: SubmitAndWaitRequest
  )(loggingContext: LoggingContextWithTrace): Future[SubmitAndWaitForTransactionTreeResponse]
}
