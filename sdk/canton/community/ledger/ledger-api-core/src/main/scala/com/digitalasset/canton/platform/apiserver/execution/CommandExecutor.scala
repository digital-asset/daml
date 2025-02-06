// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.digitalasset.canton.ledger.api.Commands as ApiCommands
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.daml.lf.crypto

import scala.concurrent.ExecutionContext

private[apiserver] trait CommandExecutor {
  def execute(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Either[ErrorCause, CommandExecutionResult]]
}
