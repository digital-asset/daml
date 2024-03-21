// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.services

import com.digitalasset.canton.ledger.api.messages.command.submission.SubmitRequest
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

trait CommandSubmissionService {
  def submit(
      request: SubmitRequest
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit]
}
