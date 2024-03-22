// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.daml.lf.crypto
import com.digitalasset.canton.ledger.api.domain.Commands as ApiCommands
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.services.ErrorCause

import scala.concurrent.Future

private[apiserver] trait CommandExecutor {
  def execute(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Either[ErrorCause, CommandExecutionResult]]
}
