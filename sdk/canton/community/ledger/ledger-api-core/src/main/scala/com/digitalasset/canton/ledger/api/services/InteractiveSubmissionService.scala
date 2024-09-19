// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.services

import com.daml.ledger.api.v2.interactive_submission_service.PrepareSubmissionResponse
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.PrepareRequest
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

object InteractiveSubmissionService {
  final case class PrepareRequest(commands: domain.Commands)
}

trait InteractiveSubmissionService {
  def prepare(request: PrepareRequest)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PrepareSubmissionResponse]
}
