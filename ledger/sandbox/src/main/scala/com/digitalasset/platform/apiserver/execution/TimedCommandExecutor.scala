// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.metrics.MetricName
import com.daml.lf.crypto.Hash
import com.daml.logging.LoggingContext
import com.daml.platform.metrics.timedFuture
import com.daml.platform.store.ErrorCause

import scala.concurrent.{ExecutionContext, Future}

class TimedCommandExecutor(
    delegate: CommandExecutor,
    metricRegistry: MetricRegistry,
    metricPrefix: MetricName,
) extends CommandExecutor {

  private val timer = metricRegistry.timer(metricPrefix :+ "total")

  override def execute(
      commands: domain.Commands,
      submissionSeed: Option[Hash],
  )(
      implicit ec: ExecutionContext,
      logCtx: LoggingContext,
  ): Future[Either[ErrorCause, CommandExecutionResult]] =
    timedFuture(timer, delegate.execute(commands, submissionSeed))

}
