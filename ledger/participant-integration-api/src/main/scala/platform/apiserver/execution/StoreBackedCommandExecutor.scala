// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.daml.ledger.api.domain.{Commands => ApiCommands}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.index.v2.{ContractStore, IndexPackagesService}
import com.daml.ledger.participant.state.{v2 => state}
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
  Error => DamlLfError,
}
import com.daml.lf.transaction.Node
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.packages.DeduplicatingPackageLoader
import com.daml.platform.store.ErrorCause
import scalaz.syntax.tag._

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class StoreBackedCommandExecutor(
    engine: Engine,
    participant: Ref.ParticipantId,
    packagesService: IndexPackagesService,
    contractStore: ContractStore,
    metrics: Metrics,
) extends CommandExecutor {

  private[this] val packageLoader = new DeduplicatingPackageLoader()

  override def execute(
      commands: ApiCommands,
      submissionSeed: crypto.Hash,
      ledgerConfiguration: Configuration,
  )(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[ErrorCause, CommandExecutionResult]] = {
    val start = System.nanoTime()
    // The actAs and readAs parties are used for two kinds of checks by the ledger API server:
    // When looking up contracts during command interpretation, the engine should only see contracts
    // that are visible to at least one of the actAs or readAs parties. This visibility check is not part of the
    // Daml ledger model.
    // When checking Daml authorization rules, the engine verifies that the actAs parties are sufficient to
    // authorize the resulting transaction.
    val commitAuthorizers = commands.actAs
    val submissionResult = Timed.trackedValue(
      metrics.daml.execution.engineRunning,
      engine.submit(
        commitAuthorizers,
        commands.readAs,
        commands.commands,
        participant,
        submissionSeed,
      ),
    )
    consume(commands.actAs, commands.readAs, submissionResult)
      .map { submission =>
        (for {
          result <- submission
          (updateTx, meta) = result
        } yield {
          val interpretationTimeNanos = System.nanoTime() - start
          CommandExecutionResult(
            submitterInfo = state.SubmitterInfo(
              commands.actAs.toList,
              commands.applicationId.unwrap,
              commands.commandId.unwrap,
              commands.deduplicationPeriod,
              commands.submissionId.unwrap,
              ledgerConfiguration,
            ),
            transactionMeta = state.TransactionMeta(
              commands.commands.ledgerEffectiveTime,
              commands.workflowId.map(_.unwrap),
              meta.submissionTime,
              submissionSeed,
              Some(meta.usedPackages),
              Some(meta.nodeSeeds),
              Some(
                updateTx.nodes
                  .collect { case (nodeId, node: Node.GenActionNode[_]) if node.byKey => nodeId }
                  .to(ImmArray)
              ),
            ),
            transaction = updateTx,
            dependsOnLedgerTime = meta.dependsOnTime,
            interpretationTimeNanos = interpretationTimeNanos,
          )
        }).left.map(ErrorCause.DamlLf)
      }
  }

  private def consume[A](actAs: Set[Ref.Party], readAs: Set[Ref.Party], result: Result[A])(implicit
      ec: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Either[DamlLfError, A]] = {
    val readers = actAs ++ readAs

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
                Timed.trackedValue(metrics.daml.execution.engineRunning, resume(instance))
              )
            }

        case ResultNeedKey(key, resume) =>
          val start = System.nanoTime
          Timed
            .future(
              metrics.daml.execution.lookupContractKey,
              contractStore.lookupContractKey(readers, key.globalKey),
            )
            .flatMap { contractId =>
              lookupContractKeyTime.addAndGet(System.nanoTime() - start)
              lookupContractKeyCount.incrementAndGet()
              resolveStep(
                Timed.trackedValue(metrics.daml.execution.engineRunning, resume(contractId))
              )
            }

        case ResultNeedPackage(packageId, resume) =>
          packageLoader
            .loadPackage(
              packageId = packageId,
              delegate = packageId => packagesService.getLfArchive(packageId)(loggingContext),
              metric = metrics.daml.execution.getLfPackage,
            )
            .flatMap { maybePackage =>
              resolveStep(
                Timed.trackedValue(metrics.daml.execution.engineRunning, resume(maybePackage))
              )
            }
      }

    resolveStep(result).andThen { case _ =>
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
