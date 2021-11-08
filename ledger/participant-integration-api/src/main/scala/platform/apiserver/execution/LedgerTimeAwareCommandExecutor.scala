// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.error.ErrorCause
import com.daml.ledger.api.domain.Commands
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.lf.crypto
import com.daml.lf.data.Time
import com.daml.lf.value.Value.ContractId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class LedgerTimeAwareCommandExecutor(
    delegate: CommandExecutor,
    contractStore: ContractStore,
    maxRetries: Int,
    metrics: Metrics,
) extends CommandExecutor {

  private val logger = ContextualizedLogger.get(this.getClass)

  /** Executes a command, advancing the ledger time as necessary.
    *
    * The command execution result is guaranteed to satisfy causal monotonicity, i.e.,
    * the resulting transaction has a ledger time greater than or equal to the ledger time of any used contract.
    */
  override def execute(
      commands: Commands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[ErrorCause, CommandExecutionResult]] =
    loop(commands, submissionSeed, ledgerConfiguration, maxRetries)

  private[this] def loop(
      commands: Commands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
      retriesLeft: Int,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[ErrorCause, CommandExecutionResult]] = {
    def retryOrError(newCommands: Commands) = {
      if (retriesLeft > 0) {
        metrics.daml.execution.retry.mark()
        logger.info(s"Restarting the computation")
        loop(newCommands, submissionSeed, ledgerConfiguration, retriesLeft - 1)
      } else {
        logger.info(
          s"Did not find a suitable ledger time after ${maxRetries - retriesLeft} retries. Giving up."
        )
        Future.successful(Left(ErrorCause.LedgerTime(maxRetries)))
      }
    }
    delegate
      .execute(commands, submissionSeed, ledgerConfiguration)
      .flatMap {
        case e @ Left(_) =>
          // Permanently failed
          Future.successful(e)
        case Right(cer) =>
          // Command execution was successful.
          // Check whether the ledger time used for input is consistent with the output,
          // and advance output time or re-execute the command if necessary.
          val usedContractIds: Set[ContractId] = cer.transaction
            .inputContracts[ContractId]
            .collect { case id: ContractId => id }
          if (usedContractIds.isEmpty)
            Future.successful(Right(cer))
          else
            contractStore
              .lookupMaximumLedgerTime(usedContractIds)
              .flatMap {
                case Left(missingContracts) =>
                  logger.info(
                    s"Did not find the ledger time for some contracts. This can happen if there is contention on contracts used by the transaction. Missing contracts: ${missingContracts
                      .mkString(", ")}."
                  )
                  retryOrError(commands)

                case Right(maxUsedTime) =>
                  if (maxUsedTime.forall(_ <= commands.commands.ledgerEffectiveTime)) {
                    Future.successful(Right(cer))
                  } else if (!cer.dependsOnLedgerTime) {
                    logger.info(
                      s"Advancing ledger effective time for the output from ${commands.commands.ledgerEffectiveTime} to $maxUsedTime"
                    )
                    Future.successful(Right(advanceOutputTime(cer, maxUsedTime)))
                  } else {
                    logger.info(
                      s"Ledger time was used in the Daml code, but a different ledger effective time $maxUsedTime was determined afterwards."
                    )
                    val advancedCommands = advanceInputTime(commands, maxUsedTime)
                    retryOrError(advancedCommands)
                  }
              }
              .recover {
                // An error while looking up the maximum ledger time for the used contracts. The nature of this error is not known.
                // Not retrying automatically. All other automatically retryable cases are covered by the logic above.
                case error =>
                  logger.info(
                    s"Lookup of maximum ledger time failed after ${maxRetries - retriesLeft}. Used contracts: ${usedContractIds
                      .mkString(", ")}. Details: $error"
                  )
                  Left(ErrorCause.LedgerTime(maxRetries - retriesLeft))
              }
      }
  }

  // Does nothing if `newTime` is empty. This happens if the transaction only regarded divulged contracts.
  private[this] def advanceOutputTime(
      res: CommandExecutionResult,
      newTime: Option[Time.Timestamp],
  ): CommandExecutionResult =
    newTime.fold(res)(t =>
      res.copy(transactionMeta = res.transactionMeta.copy(ledgerEffectiveTime = t))
    )

  // Does nothing if `newTime` is empty. This happens if the transaction only regarded divulged contracts.
  private[this] def advanceInputTime(cmd: Commands, newTime: Option[Time.Timestamp]): Commands =
    newTime.fold(cmd)(t => cmd.copy(commands = cmd.commands.copy(ledgerEffectiveTime = t)))
}
