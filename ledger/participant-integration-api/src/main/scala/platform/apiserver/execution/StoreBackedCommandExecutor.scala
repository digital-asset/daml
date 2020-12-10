// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import com.daml.ledger.api.domain.{Commands => ApiCommands}
import com.daml.ledger.participant.state.index.v2.{ContractStore, IndexPackagesService}
import com.daml.ledger.participant.state.v1.{SubmitterInfo, TransactionMeta}
import com.daml.lf.crypto
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.{
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
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.ErrorCause
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

private[apiserver] final class StoreBackedCommandExecutor(
    engine: Engine,
    participant: Ref.ParticipantId,
    packagesService: IndexPackagesService,
    contractStore: ContractStore,
    metrics: Metrics,
) extends CommandExecutor {

  override def execute(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
  )(
      implicit ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[ErrorCause, CommandExecutionResult]] = {
    val start = System.nanoTime()
    // The actAs and readAs parties are used for two kinds of checks by the ledger API server:
    // When looking up contracts during command interpretation, the engine should only see contracts
    // that are visible to at least one of the actAs or readAs parties. This visibility check is not part of the
    // DAML ledger model.
    // When checking DAML authorization rules, the engine verifies that the actAs parties are sufficient to
    // authorize the resulting transaction.
    val contractReaders = commands.actAs ++ commands.readAs
    val commitAuthorizers = commands.actAs
    val submissionResult = Timed.trackedValue(
      metrics.daml.execution.engineRunning,
      engine.submit(commitAuthorizers, commands.commands, participant, submissionSeed))
    consume(contractReaders, submissionResult)
      .map { submission =>
        (for {
          result <- submission
          (updateTx, meta) = result
        } yield {
          val interpretationTimeNanos = System.nanoTime() - start
          CommandExecutionResult(
            submitterInfo = SubmitterInfo(
              commands.actAs.toList,
              commands.applicationId.unwrap,
              commands.commandId.unwrap,
              commands.deduplicateUntil,
            ),
            transactionMeta = TransactionMeta(
              commands.commands.ledgerEffectiveTime,
              commands.workflowId.map(_.unwrap),
              meta.submissionTime,
              submissionSeed,
              Some(meta.usedPackages),
              Some(meta.nodeSeeds),
              Some(
                updateTx.nodes
                  .collect { case (nodeId, node) if node.byKey => nodeId }
                  .to[ImmArray]),
            ),
            transaction = updateTx,
            dependsOnLedgerTime = meta.dependsOnTime,
            interpretationTimeNanos = interpretationTimeNanos
          )
        }).left.map(ErrorCause.DamlLf)
      }
  }

  // Concurrent map of promises to request each package only once.
  private val packagePromises: ConcurrentHashMap[Ref.PackageId, Promise[Option[Package]]] =
    new ConcurrentHashMap()

  private def consume[A](readers: Set[Ref.Party], result: Result[A])(
      implicit ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[DamlLfError, A]] = {

    val lookupActiveContractTime = new AtomicLong(0L)
    val lookupActiveContractCount = new AtomicLong(0L)

    val lookupContractKeyTime = new AtomicLong(0L)
    val lookupContractKeyCount = new AtomicLong(0L)

    def resolveStep(result: Result[A]): Future[Either[DamlLfError, A]] =
      result match {
        case ResultDone(r) => Future.successful(Right(r))

        case ResultError(err) => Future.successful(Left(err))

        case ResultNeedContract(acoid, resume) =>
          val start = System.nanoTime
          Timed
            .future(
              metrics.daml.execution.lookupActiveContract,
              contractStore.lookupActiveContract(readers, acoid),
            )
            .flatMap { instance =>
              lookupActiveContractTime.addAndGet(System.nanoTime() - start)
              lookupActiveContractCount.incrementAndGet()
              resolveStep(
                Timed.trackedValue(metrics.daml.execution.engineRunning, resume(instance)))
            }

        case ResultNeedKey(key, resume) =>
          val start = System.nanoTime
          Timed
            .future(
              metrics.daml.execution.lookupContractKey,
              contractStore.lookupContractKey(readers, key.globalKey))
            .flatMap { contractId =>
              lookupContractKeyTime.addAndGet(System.nanoTime() - start)
              lookupContractKeyCount.incrementAndGet()
              resolveStep(
                Timed.trackedValue(metrics.daml.execution.engineRunning, resume(contractId)))
            }

        case ResultNeedPackage(packageId, resume) =>
          var gettingPackage = false
          val promise = packagePromises.computeIfAbsent(packageId, _ => {
            gettingPackage = true
            Promise[Option[Package]]()
          })

          if (gettingPackage) {
            val future =
              Timed.future(
                metrics.daml.execution.getLfPackage,
                packagesService.getLfPackage(packageId))
            future.onComplete {
              case Success(None) | Failure(_) =>
                // Did not find the package or got an error when looking for it. Remove the promise to allow later retries.
                packagePromises.remove(packageId)

              case Success(Some(_)) =>
              // we don't need to treat a successful package fetch here
            }
            promise.completeWith(future)
          }

          promise.future.flatMap { maybePackage =>
            resolveStep(
              Timed.trackedValue(metrics.daml.execution.engineRunning, resume(maybePackage)))
          }
      }

    resolveStep(result).andThen {
      case _ =>
        metrics.daml.execution.lookupActiveContractPerExecution
          .update(lookupActiveContractTime.get(), TimeUnit.NANOSECONDS)
        metrics.daml.execution.lookupActiveContractCountPerExecution
          .update(lookupActiveContractCount.get)
        metrics.daml.execution.lookupContractKeyPerExecution
          .update(lookupContractKeyTime.get(), TimeUnit.NANOSECONDS)
        metrics.daml.execution.lookupContractKeyCountPerExecution
          .update(lookupContractKeyCount.get())
    }
  }
}
