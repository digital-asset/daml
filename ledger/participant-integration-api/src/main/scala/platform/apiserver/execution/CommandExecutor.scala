// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.error.ErrorCause
import com.daml.ledger.api.domain.{Commands => ApiCommands}
import com.daml.ledger.configuration.Configuration
import com.daml.lf.crypto
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] trait CommandExecutor {
  def execute(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[ErrorCause, CommandExecutionResult]]
}
