// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.{Blinding, Engine, Commands => LfCommands}
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction.Transaction.{Value => TxValue}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.api.domain.{Commands, Party}
import com.digitalasset.platform.sandbox.config.DamlPackageContainer
import com.digitalasset.platform.sandbox.damle.SandboxDamle
import com.digitalasset.ledger.backend.api.v1.TransactionSubmission
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

class CommandExecutorImpl(engine: Engine, packageContainer: DamlPackageContainer)(
    implicit ec: ExecutionContext)
    extends CommandExecutor {

  override def execute(
      submitter: Party,
      submitted: Commands,
      getContract: Value.AbsoluteContractId => Future[
        Option[Value.ContractInst[TxValue[Value.AbsoluteContractId]]]],
      lookupKey: GlobalKey => Future[Option[AbsoluteContractId]],
      commands: LfCommands): Future[Either[ErrorCause, TransactionSubmission]] = {
    SandboxDamle.consume(engine.submit(commands))(packageContainer, getContract, lookupKey).map {
      case Left(err) =>
        Left(ErrorCause.DamlLf(err))
      case Right(updateTx) =>
        Blinding
          .checkAuthorizationAndBlind(updateTx, Set(Ref.Party.assertFromString(submitter.unwrap)))
          .fold(
            e => Left(ErrorCause.DamlLf(e)),
            blindingInfo =>
              Right(TransactionSubmission(
                submitted.commandId.unwrap,
                submitted.workflowId.fold("")(_.unwrap),
                submitted.submitter.unwrap,
                submitted.ledgerEffectiveTime,
                submitted.maximumRecordTime,
                submitted.applicationId.unwrap,
                blindingInfo,
                updateTx
              ))
          )
    }

  }
}
