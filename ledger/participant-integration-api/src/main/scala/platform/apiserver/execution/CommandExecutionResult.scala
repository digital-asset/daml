// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.transaction.SubmittedTransaction

/** The result of command execution.
  *
  * @param submitterInfo            The submitter info
  * @param transactionMeta          The transaction meta-data
  * @param transaction              The transaction
  * @param dependsOnLedgerTime      True if the output of command execution depends in any way
  *                                 on the ledger time, as specified through
  *                                 [[com.daml.lf.command.Commands.ledgerEffectiveTime]].
  *                                 If this value is false, then the ledger time of the resulting
  *                                 transaction ([[state.TransactionMeta.ledgerEffectiveTime]])
  *                                 can safely be changed after command interpretation.
  * @param interpretationTimeNanos  Wall-clock time that interpretation took for the engine.
  */
private[apiserver] final case class CommandExecutionResult(
    submitterInfo: state.SubmitterInfo,
    transactionMeta: state.TransactionMeta,
    transaction: SubmittedTransaction,
    dependsOnLedgerTime: Boolean,
    interpretationTimeNanos: Long,
)
