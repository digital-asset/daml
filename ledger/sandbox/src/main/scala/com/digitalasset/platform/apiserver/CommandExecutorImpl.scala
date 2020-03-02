// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.util.concurrent.ConcurrentHashMap

import com.daml.ledger.participant.state.v1.{SubmitterInfo, TransactionMeta}
import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.engine.{
  Blinding,
  Engine,
  Result,
  ResultDone,
  ResultError,
  ResultNeedContract,
  ResultNeedKey,
  ResultNeedPackage,
  Error => DamlLfError
}
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.transaction.Transaction.{Value => TxValue}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.api.domain.{Commands => ApiCommands}
import com.digitalasset.platform.store.ErrorCause
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class CommandExecutorImpl(
    engine: Engine,
    getPackage: Ref.PackageId => Future[Option[Package]],
    participant: Ref.ParticipantId,
)(implicit ec: ExecutionContext)
    extends CommandExecutor {

  override def execute(
      submitter: Party,
      submissionSeed: Option[crypto.Hash],
      submitted: ApiCommands,
      getContract: Value.AbsoluteContractId => Future[
        Option[Value.ContractInst[TxValue[Value.AbsoluteContractId]]]],
      lookupKey: GlobalKey => Future[Option[AbsoluteContractId]],
      commands: Commands,
  ): Future[Either[ErrorCause, (SubmitterInfo, TransactionMeta, Transaction.Transaction)]] = {

    consume(engine.submit(commands, participant, submissionSeed))(
      getPackage,
      getContract,
      lookupKey)
      .map { submission =>
        (for {
          updateTx <- submission
          _ <- Blinding
            .checkAuthorizationAndBlind(updateTx, Set(submitter))
        } yield
          (
            SubmitterInfo(
              submitted.submitter,
              submitted.applicationId.unwrap,
              submitted.commandId.unwrap,
              Time.Timestamp.assertFromInstant(submitted.maximumRecordTime)
            ),
            TransactionMeta(
              Time.Timestamp.assertFromInstant(submitted.ledgerEffectiveTime),
              submitted.workflowId.map(_.unwrap),
              submissionSeed,
            ),
            updateTx,
          )).left.map(ErrorCause.DamlLf)
      }
  }

  // Concurrent map of promises to request each package only once.
  private val packagePromises: ConcurrentHashMap[Ref.PackageId, Promise[Option[Package]]] =
    new ConcurrentHashMap()

  def consume[A](result: Result[A])(
      getPackage: Ref.PackageId => Future[Option[Package]],
      getContract: Value.AbsoluteContractId => Future[
        Option[Value.ContractInst[TxValue[Value.AbsoluteContractId]]]],
      lookupKey: GlobalKey => Future[Option[AbsoluteContractId]])(
      implicit ec: ExecutionContext): Future[Either[DamlLfError, A]] = {

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def resolveStep(result: Result[A]): Future[Either[DamlLfError, A]] = {
      result match {
        case ResultNeedPackage(packageId, resume) =>
          var gettingPackage = false
          val promise = packagePromises
            .computeIfAbsent(packageId, { _ =>
              gettingPackage = true
              Promise[Option[Package]]()
            })

          if (gettingPackage) {
            val future = getPackage(packageId)
            future.onComplete {
              case Success(None) | Failure(_) =>
                // Did not find the package or got an error when looking for it. Remove the promise to allow later retries.
                packagePromises.remove(packageId)

              case Success(Some(_)) =>
              // we don't need to treat a successful package fetch here
            }
            promise.completeWith(future)
          }
          promise.future
            .flatMap { mbPkg =>
              resolveStep(resume(mbPkg))
            }

        case ResultDone(r) => Future.successful(Right(r))
        case ResultNeedKey(key, resume) =>
          lookupKey(key).flatMap(mbcoid => resolveStep(resume(mbcoid)))
        case ResultNeedContract(acoid, resume) =>
          getContract(acoid).flatMap(o => resolveStep(resume(o)))
        case ResultError(err) => Future.successful(Left(err))
      }
    }

    resolveStep(result)
  }

}
