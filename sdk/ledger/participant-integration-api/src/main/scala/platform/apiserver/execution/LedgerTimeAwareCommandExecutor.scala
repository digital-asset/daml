// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.error.definitions.ErrorCause
import com.daml.ledger.api.domain.Commands
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.index.v2.{ContractStore, MaximumLedgerTime}
import com.daml.lf.crypto
import com.daml.lf.data.Time
import com.daml.lf.value.Value.ContractId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[apiserver] final class LedgerTimeAwareCommandExecutor(
    delegate: CommandExecutor,
    contractStore: ContractStore,
    maxRetries: Int,
    metrics: Metrics,
)(implicit
    ec: ExecutionContext
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
      loggingContext: LoggingContext
  ): Future[Either[ErrorCause, CommandExecutionResult]] =
    loop(commands, submissionSeed, ledgerConfiguration, maxRetries)

  private[this] def loop(
      commands: Commands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
      retriesLeft: Int,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Either[ErrorCause, CommandExecutionResult]] =
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

          def failed = Future.successful(Left(ErrorCause.LedgerTime(maxRetries - retriesLeft)))
          def success(c: CommandExecutionResult) = Future.successful(Right(c))
          def retry(c: Commands) = {
            metrics.daml.execution.retry.mark()
            loop(c, submissionSeed, ledgerConfiguration, retriesLeft - 1)
          }

          contractStore
            .lookupMaximumLedgerTimeAfterInterpretation(usedContractIds)
            .transformWith {
              case Success(MaximumLedgerTime.NotAvailable) =>
                success(cer)

              case Success(MaximumLedgerTime.Max(maxUsedTime))
                  if maxUsedTime <= commands.commands.ledgerEffectiveTime =>
                success(cer)

              case Success(MaximumLedgerTime.Max(maxUsedTime)) if !cer.dependsOnLedgerTime =>
                logger.debug(
                  s"Advancing ledger effective time for the output from ${commands.commands.ledgerEffectiveTime} to $maxUsedTime"
                )
                success(advanceOutputTime(cer, maxUsedTime))

              case Success(MaximumLedgerTime.Max(maxUsedTime)) =>
                if (retriesLeft > 0) {
                  logger.debug(
                    s"Restarting the computation with new ledger effective time $maxUsedTime"
                  )
                  retry(advanceInputTime(commands, maxUsedTime))
                } else {
                  failed
                }

              case Success(MaximumLedgerTime.Archived(contracts)) =>
                if (retriesLeft > 0) {
                  logger.info(
                    s"Some input contracts are archived: ${contracts.mkString("[", ", ", "]")} Restarting the computation."
                  )
                  retry(commands)
                } else {
                  logger.info(
                    s"Lookup of maximum ledger time failed after ${maxRetries - retriesLeft}. Used contracts: ${usedContractIds
                        .mkString("[", ", ", "]")}."
                  )
                  failed
                }

              // An error while looking up the maximum ledger time for the used contracts. The nature of this error is not known.
              // Not retrying automatically. All other automatically retry-able cases are covered by the logic above.
              case Failure(error) =>
                logger.info(
                  s"Lookup of maximum ledger time failed after ${maxRetries - retriesLeft}. Used contracts: ${usedContractIds
                      .mkString("[", ", ", "]")}. Details: $error"
                )
                failed
            }
      }

  private[this] def advanceOutputTime(
      res: CommandExecutionResult,
      newTime: Time.Timestamp,
  ): CommandExecutionResult =
    res.copy(transactionMeta = res.transactionMeta.copy(ledgerEffectiveTime = newTime))

  private[this] def advanceInputTime(cmd: Commands, newTime: Time.Timestamp): Commands =
    cmd.copy(commands = cmd.commands.copy(ledgerEffectiveTime = newTime))
}
