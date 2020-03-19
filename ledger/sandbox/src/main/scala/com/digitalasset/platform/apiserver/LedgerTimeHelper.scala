// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.time.Instant

import com.daml.ledger.participant.state.index.v2.ContractStore
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.api.domain.{Commands => ApiCommands}
import com.digitalasset.platform.store.ErrorCause

import scala.concurrent.{ExecutionContext, Future}

final class LedgerTimeHelper(
  contractStore: ContractStore,
  commandExecutor: CommandExecutor,
  maxRetries: Int = 3,
) {

  /**
   * Executes a command, advancing the ledger time as necessary.
   *
   * The command execution result is guaranteed to satisfy causal monotonicity, i.e.,
   * the resulting transaction has a ledger time greater than or equal to the ledger time of any used contract.
   */
  def execute(
    commands: ApiCommands,
    submissionSeed: Option[crypto.Hash],
  )(implicit ec: ExecutionContext): Future[Either[ErrorCause, CommandExecutionResult]] =
    loop(commands, submissionSeed, maxRetries)

  private[this] def loop(
    commands: ApiCommands,
    submissionSeed: Option[crypto.Hash],
    retriesLeft: Int,
  )(implicit ec: ExecutionContext): Future[Either[ErrorCause, CommandExecutionResult]] = {
    commandExecutor.execute(
      commands.submitter,
      submissionSeed,
      commands,
      contractStore.lookupActiveContract(commands.submitter, _),
      contractStore.lookupContractKey(commands.submitter, _),
      commands.commands
    ).flatMap {
      case e @ Left(ErrorCause.DamlLf(_)) =>
        // Permanently failed
        Future.successful(e)
      case Right(cer) =>
        // Command execution was successful.
        // Check whether the ledger time used for input is consistent with the output,
        // and advance output time or re-execute the command if necessary.
        val usedContractIds: Set[AbsoluteContractId] = cer.transaction.inputContracts[Transaction.TContractId].collect[AbsoluteContractId, Set[AbsoluteContractId]] {
          case id: AbsoluteContractId => id
        }
        if (usedContractIds.isEmpty)
          Future.successful(Right(cer))
        else
          contractStore
            .lookupMaximumLedgerTime(usedContractIds)
            .flatMap(maxUsedTime => {
              if (!maxUsedTime.isAfter(commands.ledgerEffectiveTime))
                Future.successful(Right(cer))
              else if (!cer.dependsOnLedgerTime)
                Future.successful(Right(advanceOutputTime(cer, maxUsedTime)))
              else if (retriesLeft > 0)
                loop(advanceInputTime(commands, maxUsedTime), submissionSeed, retriesLeft - 1)
              else
                Future.successful(Left(ErrorCause.LedgerTime(maxRetries)))
            })
    }
  }

  private[this] def advanceOutputTime(res: CommandExecutionResult, t: Instant): CommandExecutionResult =
    res.copy(transactionMeta = res.transactionMeta.copy(ledgerEffectiveTime = Time.Timestamp.assertFromInstant(t)))

  private[this] def advanceInputTime(cmd: ApiCommands, t: Instant): ApiCommands =
    cmd.copy(ledgerEffectiveTime = t)
}