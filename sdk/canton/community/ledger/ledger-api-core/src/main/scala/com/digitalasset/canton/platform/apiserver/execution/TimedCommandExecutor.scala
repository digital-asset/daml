// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.daml.metrics.Timed
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.TimerAndTrackOnShutdownSyntax
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.daml.lf.crypto

import scala.concurrent.ExecutionContext

private[apiserver] class TimedCommandExecutor(
    delegate: CommandExecutor,
    metrics: LedgerApiServerMetrics,
) extends CommandExecutor {

  override def execute(
      commands: domain.Commands,
      submissionSeed: crypto.Hash,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Either[ErrorCause, CommandExecutionResult]] =
    Timed.timedAndTrackedFutureUS(
      metrics.execution.total,
      metrics.execution.totalRunning,
      delegate.execute(commands, submissionSeed),
    )

}
