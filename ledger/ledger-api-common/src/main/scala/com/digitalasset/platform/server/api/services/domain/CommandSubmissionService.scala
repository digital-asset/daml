// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.services.domain

import com.daml.ledger.api.messages.command.submission.SubmitRequest

import scala.concurrent.Future

trait CommandSubmissionService {
  def submit(request: SubmitRequest): Future[Unit]
}
