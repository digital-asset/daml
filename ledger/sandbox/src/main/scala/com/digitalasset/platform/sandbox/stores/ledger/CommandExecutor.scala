// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.digitalasset.daml.lf.command.Commands
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction.Transaction.{Value => TxValue}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.api.domain.{Commands => ApiCommands}
import com.digitalasset.ledger.backend.api.v1.TransactionSubmission

import scala.concurrent.Future

trait CommandExecutor {

  def execute(
      submitter: Party,
      submitted: ApiCommands,
      getContract: Value.AbsoluteContractId => Future[
        Option[Value.ContractInst[TxValue[Value.AbsoluteContractId]]]],
      lookupKey: GlobalKey => Future[Option[AbsoluteContractId]],
      commands: Commands
  ): Future[Either[ErrorCause, TransactionSubmission]]
}
