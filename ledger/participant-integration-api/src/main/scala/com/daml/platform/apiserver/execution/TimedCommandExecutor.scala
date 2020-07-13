// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.ledger.api.domain
import com.daml.lf.crypto
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.ErrorCause

import scala.concurrent.{ExecutionContext, Future}

class TimedCommandExecutor(
    delegate: CommandExecutor,
    metrics: Metrics,
) extends CommandExecutor {

  override def execute(
      commands: domain.Commands,
      submissionSeed: crypto.Hash,
  )(
      implicit ec: ExecutionContext,
      logCtx: LoggingContext,
  ): Future[Either[ErrorCause, CommandExecutionResult]] =
    Timed.future(metrics.daml.execution.total, delegate.execute(commands, submissionSeed))

}
