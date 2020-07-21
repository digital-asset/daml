// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.ledger.api.domain.{Commands => ApiCommands}
import com.daml.lf.crypto
import com.daml.logging.LoggingContext
import com.daml.platform.store.ErrorCause

import scala.concurrent.{ExecutionContext, Future}

trait CommandExecutor {
  def execute(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
  )(
      implicit ec: ExecutionContext,
      logCtx: LoggingContext,
  ): Future[Either[ErrorCause, CommandExecutionResult]]
}
