// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import com.daml.ledger.participant.state.v1.{SubmitterInfo, TransactionMeta}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.command.Commands
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.api.domain.{Commands => ApiCommands}
import com.digitalasset.platform.store.ErrorCause

import scala.concurrent.Future

/**
  * The result of command execution.
  *
  * @param submitterInfo       The submitter info
  * @param transactionMeta     The transaction meta-data
  * @param dependsOnLedgerTime True if the output of command execution depends in any way
  *                            on the ledger time, as specified through [[Commands.ledgerEffectiveTime]].
  *                            If this value is false, then the ledger time of the resulting transaction
  *                            ([[TransactionMeta.ledgerEffectiveTime]] can safely be changed after command
  *                            interpretation.
  * @param transaction         The transaction
  */
final case class CommandExecutionResult(
    submitterInfo: SubmitterInfo,
    transactionMeta: TransactionMeta,
    transaction: Transaction.Transaction,
    dependsOnLedgerTime: Boolean,
)

trait CommandExecutor {

  def execute(
      submitter: Party,
      submissionSeed: Option[crypto.Hash],
      submitted: ApiCommands,
      getContract: Value.AbsoluteContractId => Future[
        Option[Value.ContractInst[Transaction.Value[Value.AbsoluteContractId]]]],
      lookupKey: GlobalKey => Future[Option[AbsoluteContractId]],
      commands: Commands
  ): Future[Either[ErrorCause.DamlLf, CommandExecutionResult]]
}
