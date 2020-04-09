// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.ledger.api.domain.{Commands => ApiCommands}
import com.daml.lf.command.Commands
import com.daml.lf.crypto
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.Node.GlobalKey
import com.daml.lf.transaction.Transaction
import com.daml.lf.value.Value
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.platform.store.ErrorCause

import scala.concurrent.Future

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
