// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.error.ErrorCause
import com.daml.ledger.api.domain
import com.daml.ledger.configuration.Configuration
import com.daml.lf.crypto
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] class TimedCommandExecutor(
    delegate: CommandExecutor,
    metrics: Metrics,
) extends CommandExecutor {

  override def execute(
      commands: domain.Commands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[ErrorCause, CommandExecutionResult]] =
    Timed.timedAndTrackedFuture(
      metrics.daml.execution.total,
      metrics.daml.execution.totalRunning,
      delegate.execute(commands, submissionSeed, ledgerConfiguration),
    )

}
