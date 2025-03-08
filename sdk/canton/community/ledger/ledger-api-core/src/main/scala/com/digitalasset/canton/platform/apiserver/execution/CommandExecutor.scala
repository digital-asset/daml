// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import com.digitalasset.canton.ledger.api.Commands
import com.digitalasset.canton.ledger.participant.state.SyncService
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.daml.lf.crypto.Hash

import scala.concurrent.ExecutionContext

trait CommandExecutor {

  /** Executes the command and returns the command execution result with the rank of the
    * synchronizer that should be used for routing.
    *
    * @param commands
    *   The commands to be processed
    * @param submissionSeed
    *   The submission seed
    * @param forExternallySigned
    *   Whether the command should be processed for external signing. If true, the command's
    *   submitters are not required to have submission rights on the participant.
    * @return
    *   the command execution result with the routing synchronizer
    */
  def execute(
      commands: Commands,
      submissionSeed: Hash,
      forExternallySigned: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, ErrorCause, CommandExecutionResult]
}

object CommandExecutor {
  def apply(
      syncService: SyncService,
      commandInterpreter: CommandInterpreter,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): CommandExecutor =
    new DefaultCommandExecutor(
      syncService = syncService,
      commandInterpreter = commandInterpreter,
      loggerFactory = loggerFactory,
    )
}

private[execution] class DefaultCommandExecutor(
    syncService: SyncService,
    commandInterpreter: CommandInterpreter,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends NamedLogging
    with CommandExecutor {

  override def execute(
      commands: Commands,
      submissionSeed: Hash,
      forExternallySigned: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): EitherT[FutureUnlessShutdown, ErrorCause, CommandExecutionResult] = {
    logger.debug("Processing command with the default package preference selection algorithm")
    for {
      commandInterpretationResult <- EitherT(
        commandInterpreter.interpret(commands, submissionSeed)
      )
      routingSynchronizerResult <- syncService
        .selectRoutingSynchronizer(
          commandInterpretationResult.submitterInfo,
          commandInterpretationResult.transaction,
          commandInterpretationResult.transactionMeta,
          commandInterpretationResult.processedDisclosedContracts.map(_.contractId).toList,
          commandInterpretationResult.optSynchronizerId,
          transactionUsedForExternalSigning = forExternallySigned,
        )
        .leftMap[ErrorCause](ErrorCause.RoutingFailed(_))
      (synchronizerRank, routingSynchronizerState) = routingSynchronizerResult
    } yield commandInterpretationResult.toCommandExecutionResult(
      synchronizerRank,
      routingSynchronizerState,
    )
  }
}
