// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.metrics.ParticipantMetrics

import scala.concurrent.ExecutionContext

object Main {
  def main(args: Array[String]): Unit =
    IntegrityChecker.runAndExit(args, preExecutionCommitStrategySupportFactory _)

  def batchingCommitStrategySupportFactory(
      metrics: ParticipantMetrics,
      executionContext: ExecutionContext,
  ) = new LogAppendingCommitStrategySupport(metrics)(executionContext)

  def preExecutionCommitStrategySupportFactory(
      metrics: ParticipantMetrics,
      executionContext: ExecutionContext,
  ) = new RawPreExecutingCommitStrategySupport(metrics)(executionContext)
}
