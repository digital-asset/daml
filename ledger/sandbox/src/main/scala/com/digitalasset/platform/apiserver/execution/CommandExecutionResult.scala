// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.ledger.participant.state.v1.{SubmitterInfo, TransactionMeta}
import com.daml.lf.transaction.Transaction

/**
  * The result of command execution.
  *
  * @param submitterInfo       The submitter info
  * @param transactionMeta     The transaction meta-data
  * @param dependsOnLedgerTime True if the output of command execution depends in any way
  *                            on the ledger time, as specified through
  *                            [[com.daml.lf.command.Commands.ledgerEffectiveTime]].
  *                            If this value is false, then the ledger time of the resulting
  *                            transaction ([[TransactionMeta.ledgerEffectiveTime]]) can safely be
  *                            changed after command interpretation.
  * @param transaction         The transaction
  */
final case class CommandExecutionResult(
    submitterInfo: SubmitterInfo,
    transactionMeta: TransactionMeta,
    transaction: Transaction.Transaction,
    dependsOnLedgerTime: Boolean,
)
