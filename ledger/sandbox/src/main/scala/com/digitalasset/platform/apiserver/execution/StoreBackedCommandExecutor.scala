// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import java.util.concurrent.ConcurrentHashMap

import com.daml.ledger.api.domain.{Commands => ApiCommands}
import com.daml.ledger.participant.state.index.v2.{ContractStore, IndexPackagesService}
import com.daml.ledger.participant.state.v1.{SubmitterInfo, TransactionMeta}
import com.daml.lf.crypto
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.engine.{
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
import com.daml.lf.language.Ast.Package
import com.daml.logging.LoggingContext
import com.daml.platform.store.ErrorCause
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class StoreBackedCommandExecutor(
    engine: Engine,
    participant: Ref.ParticipantId,
    packagesService: IndexPackagesService,
    contractStore: ContractStore,
) extends CommandExecutor {

  override def execute(
      commands: ApiCommands,
      submissionSeed: Option[crypto.Hash],
  )(
      implicit ec: ExecutionContext,
      logCtx: LoggingContext,
  ): Future[Either[ErrorCause, CommandExecutionResult]] = {

    consume(commands.submitter, engine.submit(commands.commands, participant, submissionSeed))
      .map { submission =>
        (for {
          result <- submission
          (updateTx, meta) = result
          _ <- Blinding
            .checkAuthorizationAndBlind(updateTx, Set(commands.submitter))
        } yield
          CommandExecutionResult(
            submitterInfo = SubmitterInfo(
              commands.submitter,
              commands.applicationId.unwrap,
              commands.commandId.unwrap,
              commands.deduplicateUntil,
            ),
            transactionMeta = TransactionMeta(
              Time.Timestamp.assertFromInstant(commands.ledgerEffectiveTime),
              commands.workflowId.map(_.unwrap),
              meta.submissionTime,
              submissionSeed,
              Some(meta.usedPackages)
            ),
            transaction = updateTx,
            dependsOnLedgerTime = meta.dependsOnTime,
          )).left.map(ErrorCause.DamlLf)
      }
  }

  // Concurrent map of promises to request each package only once.
  private val packagePromises: ConcurrentHashMap[Ref.PackageId, Promise[Option[Package]]] =
    new ConcurrentHashMap()

  private def consume[A](submitter: Ref.Party, result: Result[A])(
      implicit ec: ExecutionContext
  ): Future[Either[DamlLfError, A]] = {

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def resolveStep(result: Result[A]): Future[Either[DamlLfError, A]] =
      result match {
        case ResultDone(r) => Future.successful(Right(r))

        case ResultError(err) => Future.successful(Left(err))

        case ResultNeedKey(key, resume) =>
          contractStore
            .lookupContractKey(submitter, key)
            .flatMap(contractId => resolveStep(resume(contractId)))

        case ResultNeedContract(acoid, resume) =>
          contractStore
            .lookupActiveContract(submitter, acoid)
            .flatMap(instance => resolveStep(resume(instance)))

        case ResultNeedPackage(packageId, resume) =>
          var gettingPackage = false
          val promise = packagePromises
            .computeIfAbsent(packageId, { _ =>
              gettingPackage = true
              Promise[Option[Package]]()
            })

          if (gettingPackage) {
            val future = packagesService.getLfPackage(packageId)
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
      }

    resolveStep(result)
  }

}
