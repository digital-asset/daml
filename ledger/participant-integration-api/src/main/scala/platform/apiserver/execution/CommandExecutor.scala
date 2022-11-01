// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.ledger.api.domain.{Commands => ApiCommands}
import com.daml.ledger.configuration.Configuration
import com.daml.lf.crypto
import com.daml.logging.LoggingContext
import com.daml.platform.error.ErrorCause

import scala.concurrent.Future

private[apiserver] trait CommandExecutor {
  def execute(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Either[ErrorCause, CommandExecutionResult]]
}
