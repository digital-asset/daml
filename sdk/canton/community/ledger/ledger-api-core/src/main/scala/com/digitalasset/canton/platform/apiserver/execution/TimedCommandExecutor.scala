// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import com.daml.metrics.Timed
import com.digitalasset.canton.ledger.api.Commands
import com.digitalasset.canton.ledger.participant.state.RoutingSynchronizerState
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.TimerAndTrackOnShutdownSyntax
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.daml.lf.crypto.Hash

private[apiserver] class TimedCommandExecutor(
    delegate: CommandExecutor,
    metrics: LedgerApiServerMetrics,
) extends CommandExecutor {

  override def execute(
      commands: Commands,
      submissionSeed: Hash,
      routingSynchronizerState: RoutingSynchronizerState,
      forExternallySigned: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, ErrorCause, CommandExecutionResult] =
    // TODO(#23334): introduce timedAndTrackedEitherTFUS and use it here
    EitherT(
      Timed.timedAndTrackedFutureUS(
        metrics.execution.total,
        metrics.execution.totalRunning,
        delegate
          .execute(commands, submissionSeed, routingSynchronizerState, forExternallySigned)
          .value,
      )
    )
}
