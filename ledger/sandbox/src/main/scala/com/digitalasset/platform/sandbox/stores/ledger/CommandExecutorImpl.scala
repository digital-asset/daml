// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.{Blinding, Engine}
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.transaction.Transaction.{Value => TxValue}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.api.domain.{Commands => ApiCommands}
import com.digitalasset.platform.sandbox.config.DamlPackageContainer
import com.digitalasset.platform.sandbox.damle.SandboxDamle
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

class CommandExecutorImpl(engine: Engine, packageContainer: DamlPackageContainer)(
    implicit ec: ExecutionContext)
    extends CommandExecutor {

  override def execute(
      submitter: Party,
      submitted: ApiCommands,
      getContract: Value.AbsoluteContractId => Future[
        Option[Value.ContractInst[TxValue[Value.AbsoluteContractId]]]],
      lookupKey: GlobalKey => Future[Option[AbsoluteContractId]],
      commands: Commands)
    : Future[Either[ErrorCause, (SubmitterInfo, TransactionMeta, Transaction.Transaction)]] = {
    SandboxDamle
      .consume(engine.submit(commands))(packageContainer, getContract, lookupKey)
      .map { submission =>
        (for {
          updateTx <- submission
          blindingInfo <- Blinding
            .checkAuthorizationAndBlind(updateTx, Set(submitter))
        } yield
          (
            SubmitterInfo(
              submitted.submitter,
              submitted.applicationId.unwrap,
              submitted.commandId.unwrap,
              Timestamp.assertFromInstant(submitted.maximumRecordTime)
            ),
            TransactionMeta(
              Timestamp.assertFromInstant(submitted.ledgerEffectiveTime),
              submitted.workflowId.map(_.unwrap)
            ),
            updateTx
          )).left.map(ErrorCause.DamlLf)
      }
  }
}
