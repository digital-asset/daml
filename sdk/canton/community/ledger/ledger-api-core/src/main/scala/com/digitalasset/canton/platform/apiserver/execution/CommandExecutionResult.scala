// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.{RoutingSynchronizerState, SynchronizerRank}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.{FatContractInstance, GlobalKey, SubmittedTransaction}
import com.digitalasset.daml.lf.value.Value

/** The result of command execution.
  *
  * @param submitterInfo
  *   The submitter info
  * @param optSynchronizerId
  *   The ID of the synchronizer where the submitter wants the transaction to be sequenced
  * @param transactionMeta
  *   The transaction meta-data
  * @param transaction
  *   The transaction
  * @param dependsOnLedgerTime
  *   True if the output of command execution depends in any way on the ledger time, as specified
  *   through [[com.digitalasset.daml.lf.command.ApiCommands.ledgerEffectiveTime]]. If this value is
  *   false, then the ledger time of the resulting transaction
  *   ([[state.TransactionMeta.ledgerEffectiveTime]]) can safely be changed after command
  *   interpretation.
  * @param interpretationTimeNanos
  *   Wall-clock time that interpretation took for the engine.
  * @param globalKeyMapping
  *   Input key mapping inferred by interpretation. The map should contain all contract keys that
  *   were used during interpretation. A value of None means no contract was found with this
  *   contract key.
  * @param processedDisclosedContracts
  *   The disclosed contracts used as part of command interpretation. Note that this may be a subset
  *   of the `disclosed_contracts` provided as part of the command submission by the client, as
  *   superfluously-provided contracts are discarded by the Daml engine.
  */
private[canton] final case class CommandInterpretationResult(
    submitterInfo: state.SubmitterInfo,
    transactionMeta: state.TransactionMeta,
    transaction: SubmittedTransaction,
    dependsOnLedgerTime: Boolean,
    interpretationTimeNanos: Long,
    globalKeyMapping: Map[GlobalKey, Option[Value.ContractId]],
    processedDisclosedContracts: ImmArray[FatContractInstance],
    // TODO(#23334): Consider removing the prescribed synchronizer decision from command interpreter
    //               and factor this field out of here as well.
    optSynchronizerId: Option[SynchronizerId],
) {
  def toCommandExecutionResult(
      synchronizerRank: SynchronizerRank,
      routingSynchronizerState: RoutingSynchronizerState,
  ): CommandExecutionResult =
    CommandExecutionResult(this, synchronizerRank, routingSynchronizerState)
}

/** The result of command execution.
  *
  * @param commandInterpretationResult
  *   The result of command interpretation
  * @param synchronizerRank
  *   The rank of the synchronizer that should be used for routing
  * @param routingSynchronizerState
  *   the synchronizer state that was used for computing the synchronizer rank and should be used
  *   for the rest of phase 1 of the transaction protocol.
  */
private[apiserver] final case class CommandExecutionResult(
    commandInterpretationResult: CommandInterpretationResult,
    synchronizerRank: SynchronizerRank,
    routingSynchronizerState: RoutingSynchronizerState,
)
