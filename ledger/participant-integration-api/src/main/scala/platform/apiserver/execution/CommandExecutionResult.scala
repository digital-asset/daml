// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.command.ProcessedDisclosedContract
import com.daml.lf.data.ImmArray
import com.daml.lf.transaction.{GlobalKey, SubmittedTransaction, Versioned}
import com.daml.lf.value.Value

/** The result of command execution.
  *
  * @param submitterInfo            The submitter info
  * @param transactionMeta          The transaction meta-data
  * @param transaction              The transaction
  * @param dependsOnLedgerTime      True if the output of command execution depends in any way
  *                                 on the ledger time, as specified through
  *                                 [[com.daml.lf.command.ApiCommands.ledgerEffectiveTime]].
  *                                 If this value is false, then the ledger time of the resulting
  *                                 transaction ([[state.TransactionMeta.ledgerEffectiveTime]])
  *                                 can safely be changed after command interpretation.
  * @param interpretationTimeNanos  Wall-clock time that interpretation took for the engine.
  * @param globalKeyMapping         Input key mapping inferred by interpretation.
  *                                 The map should contain all contract keys that were used during interpretation.
  *                                 A value of None means no contract was found with this contract key.
  * @param usedDisclosedContracts   The disclosed contracts used as part of command interpretation.
  *                                 Note that this may be a subset of the `disclosed_contracts`
  *                                 provided as part of the command submission by the client,
  *                                 as superfluously-provided contracts are discarded by the Daml engine.
  */
private[apiserver] final case class CommandExecutionResult(
    submitterInfo: state.SubmitterInfo,
    transactionMeta: state.TransactionMeta,
    transaction: SubmittedTransaction,
    dependsOnLedgerTime: Boolean,
    interpretationTimeNanos: Long,
    globalKeyMapping: Map[GlobalKey, Option[Value.ContractId]],
    usedDisclosedContracts: ImmArray[Versioned[ProcessedDisclosedContract]],
)
