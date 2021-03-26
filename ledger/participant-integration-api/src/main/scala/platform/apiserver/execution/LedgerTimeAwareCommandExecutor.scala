// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.ledger.api.domain.Commands
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.lf.crypto
import com.daml.lf.data.Time
import com.daml.lf.value.Value.ContractId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.ErrorCause

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
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[ErrorCause, CommandExecutionResult]] =
    loop(commands, submissionSeed, maxRetries)

  private[this] def loop(
      commands: Commands,
      submissionSeed: crypto.Hash,
      retriesLeft: Int,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[ErrorCause, CommandExecutionResult]] = {
    delegate
      .execute(commands, submissionSeed)
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
            .collect { case id: ContractId =>
              id
            }
          if (usedContractIds.isEmpty)
            Future.successful(Right(cer))
          else
            contractStore
              .lookupMaximumLedgerTime(usedContractIds)
              .flatMap(maxUsedInstant => {
                val maxUsedTime = maxUsedInstant.map(Time.Timestamp.assertFromInstant)
                if (maxUsedTime.forall(_ <= commands.commands.ledgerEffectiveTime)) {
                  Future.successful(Right(cer))
                } else if (!cer.dependsOnLedgerTime) {
                  logger.debug(
                    s"Advancing ledger effective time for the output from ${commands.commands.ledgerEffectiveTime} to $maxUsedTime"
                  )
                  Future.successful(Right(advanceOutputTime(cer, maxUsedTime)))
                } else if (retriesLeft > 0) {
                  metrics.daml.execution.retry.mark()
                  logger.debug(
                    s"Restarting the computation with new ledger effective time $maxUsedTime"
                  )
                  loop(advanceInputTime(commands, maxUsedTime), submissionSeed, retriesLeft - 1)
                } else {
                  Future.successful(Left(ErrorCause.LedgerTime(maxRetries)))
                }
              })
              .recover {
                // An error while looking up the maximum ledger time for the used contracts
                // most likely means that one of the contracts is already not active anymore,
                // which can happen under contention.
                // A retry would only be successful in case the archived contracts were referenced by key.
                // Direct references to archived contracts will result in the same error.
                case error =>
                  logger.info(
                    s"Lookup of maximum ledger time failed. This can happen if there is contention on contracts used by the transaction. Used contracts: ${usedContractIds
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
