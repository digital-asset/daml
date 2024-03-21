// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.daml.lf.crypto
import com.daml.metrics.Timed
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.services.ErrorCause

import scala.concurrent.Future

private[apiserver] class TimedCommandExecutor(
    delegate: CommandExecutor,
    metrics: Metrics,
) extends CommandExecutor {

  override def execute(
      commands: domain.Commands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Either[ErrorCause, CommandExecutionResult]] =
    Timed.timedAndTrackedFuture(
      metrics.daml.execution.total,
      metrics.daml.execution.totalRunning,
      delegate.execute(commands, submissionSeed, ledgerConfiguration),
    )

}
