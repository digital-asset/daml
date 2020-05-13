// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import java.time.Instant

import com.daml.lf.{CompiledPackages, crypto}
import com.daml.lf.data._
import com.daml.lf.language.Ast.{DDataType, DTypeSyn, DValue, Definition}
import com.daml.lf.language.Ast
import com.daml.lf.speedy.{ScenarioRunner, Speedy}
import com.daml.lf.transaction.GenTransaction
import com.daml.lf.types.Ledger.ScenarioTransactionId
import com.daml.lf.types.{Ledger => L}
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.stores.InMemoryActiveLedgerState
import com.daml.platform.store.entries.LedgerEntry
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.breakOut
import scala.collection.mutable.ArrayBuffer

object ScenarioLoader {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /** When loading from the scenario, we also specify by how much to bump the
    * ledger end after each entry. This is because in the scenario transaction
    * ids there might be "gaps" due to passTime instructions (and possibly
    * others in the future).
    *
    * Note that this matters because our ledger implementation typically derive
    * the transaction ids form the ledger end. So, the ledger end must be
    * greater than the latest transaction id produced by the scenario runner,
    * otherwise we'll get duplicates. See
    * <https://github.com/digital-asset/daml/issues/1079>.
    */
  sealed abstract class LedgerEntryOrBump extends Serializable with Product

  object LedgerEntryOrBump {
    final case class Entry(ledgerEntry: LedgerEntry) extends LedgerEntryOrBump
    final case class Bump(bump: Int) extends LedgerEntryOrBump
  }

  /**
    * @param packages All the packages where we're going to look for the scenario definition.
    * @param compiledPackages The above packages, compiled. Note that we require _all_
    *                         packages to be compiled -- this is just for ease of implementation
    *                         and might be revised in the future.
    * @param scenario The scenario to run. The scenario will be looked in all the packages above
    *                 trying both with the old and new identifier syntax (`Foo.Bar.Baz` vs `Foo.Bar:Baz`).
    *                 This function will crash if the scenario is not found or if there are multiple
    *                 matching scenarios.
    */
  def fromScenario(
      packages: InMemoryPackageStore,
      compiledPackages: CompiledPackages,
      scenario: String,
      submissionSeed: crypto.Hash,
  ): (InMemoryActiveLedgerState, ImmArray[LedgerEntryOrBump], Instant) = {
    val (scenarioLedger, scenarioRef) =
      buildScenarioLedger(packages, compiledPackages, scenario, submissionSeed)
    // we store the tx id since later we need to recover how much to bump the
    // ledger end by, and here the transaction id _is_ the ledger end.
    val ledgerEntries =
      new ArrayBuffer[(ScenarioTransactionId, LedgerEntry)](scenarioLedger.scenarioSteps.size)
    type Acc = (InMemoryActiveLedgerState, Time.Timestamp, Option[ScenarioTransactionId])
    val (acs, time, txId) =
      scenarioLedger.scenarioSteps.iterator
        .foldLeft[Acc]((InMemoryActiveLedgerState.empty, Time.Timestamp.Epoch, None)) {
          case ((acs, time, mbOldTxId), (stepId @ _, step)) =>
            executeScenarioStep(ledgerEntries, scenarioRef, acs, time, mbOldTxId, stepId, step)
        }
    // now decorate the entries with what the next increment is
    @tailrec
    def decorateWithIncrement(
        processed: BackStack[LedgerEntryOrBump],
        toProcess: ImmArray[(ScenarioTransactionId, LedgerEntry)]): ImmArray[LedgerEntryOrBump] = {

      def bumps(entryTxId: ScenarioTransactionId, nextTxId: ScenarioTransactionId) =
        if ((nextTxId.index - entryTxId.index) == 1)
          ImmArray.empty
        else
          ImmArray(LedgerEntryOrBump.Bump((nextTxId.index - entryTxId.index - 1)))

      toProcess match {
        case ImmArray() => processed.toImmArray
        // we have to bump the offsets when the first one is not zero (passTimes),
        case ImmArrayCons((entryTxId, entry), entries @ ImmArrayCons((nextTxId, next), _))
            if (processed.isEmpty && entryTxId.index > 0) =>
          val newProcessed = (processed :++ ImmArray(
            LedgerEntryOrBump.Bump(entryTxId.index),
            LedgerEntryOrBump.Entry(entry))) :++ bumps(entryTxId, nextTxId)

          decorateWithIncrement(newProcessed, entries)
        // the last one just bumps by 1 -- it does not matter as long as it's
        // positive
        case ImmArrayCons((_, entry), ImmArray()) =>
          (processed :+ LedgerEntryOrBump.Entry(entry)).toImmArray

        case ImmArrayCons((entryTxId, entry), entries @ ImmArrayCons((nextTxId, next), _)) =>
          val newProcessed = processed :+ LedgerEntryOrBump.Entry(entry) :++ bumps(
            entryTxId,
            nextTxId)

          decorateWithIncrement(newProcessed, entries)
      }
    }
    (acs, decorateWithIncrement(BackStack.empty, ImmArray(ledgerEntries)), time.toInstant)
  }

  private def buildScenarioLedger(
      packages: InMemoryPackageStore,
      compiledPackages: CompiledPackages,
      scenario: String,
      submissionSeed: crypto.Hash,
  ): (L.Ledger, Ref.DefinitionRef) = {
    val scenarioQualName = getScenarioQualifiedName(packages, scenario)
    val candidateScenarios = getCandidateScenarios(packages, scenarioQualName)
    val (scenarioRef, scenarioDef) = identifyScenario(packages, scenario, candidateScenarios)
    val scenarioExpr = getScenarioExpr(scenarioRef, scenarioDef)
    val speedyMachine = getSpeedyMachine(scenarioExpr, compiledPackages, submissionSeed)
    val scenarioLedger = getScenarioLedger(scenarioRef, speedyMachine)
    (scenarioLedger, scenarioRef)
  }

  private def getScenarioLedger(
      scenarioRef: Ref.DefinitionRef,
      speedyMachine: Speedy.Machine,
  ): L.Ledger =
    ScenarioRunner(speedyMachine).run() match {
      case Left(e) =>
        throw new RuntimeException(s"error running scenario $scenarioRef in scenario $e")
      case Right((_, _, l, _)) => l
    }

  private def getSpeedyMachine(
      scenarioExpr: Ast.Expr,
      compiledPackages: CompiledPackages,
      submissionSeed: crypto.Hash,
  ): Speedy.Machine =
    Speedy.Machine.newBuilder(compiledPackages, Time.Timestamp.now(), submissionSeed) match {
      case Left(err) => throw new RuntimeException(s"Could not build speedy machine: $err")
      case Right(build) => build(scenarioExpr)
    }

  private def getScenarioExpr(scenarioRef: Ref.DefinitionRef, scenarioDef: Definition): Ast.Expr = {
    scenarioDef match {
      case DValue(_, _, body, _) => body
      case _: DTypeSyn =>
        throw new RuntimeException(
          s"Requested scenario $scenarioRef is a type synonym, not a definition")
      case _: DDataType =>
        throw new RuntimeException(
          s"Requested scenario $scenarioRef is a data type, not a definition")
    }
  }

  private def identifyScenario(
      packages: InMemoryPackageStore,
      scenario: String,
      candidateScenarios: List[(Ref.DefinitionRef, Definition)])
    : (Ref.DefinitionRef, Definition) = {
    candidateScenarios match {
      case Nil =>
        throw new RuntimeException(
          s"Couldn't find scenario $scenario in packages ${packages.listLfPackagesSync().keys.toList}")
      case candidate :: Nil => candidate
      case candidates =>
        throw new RuntimeException(
          s"Requested scenario $scenario is present in multiple packages: ${candidates.map(_._1.packageId).toString}")
    }
  }

  private def getCandidateScenarios(
      packages: InMemoryPackageStore,
      scenarioQualName: Ref.QualifiedName
  ): List[(Ref.Identifier, Ast.Definition)] = {
    packages
      .listLfPackagesSync()
      .flatMap {
        case (packageId, _) =>
          val pkg = packages
            .getLfPackageSync(packageId)
            .getOrElse(sys.error(s"Listed package $packageId not found"))
          pkg.lookupIdentifier(scenarioQualName) match {
            case Right(x) =>
              List((Ref.Identifier(packageId, scenarioQualName) -> x))
            case Left(_) => List()
          }
      }(breakOut)
  }

  private def getScenarioQualifiedName(
      packages: InMemoryPackageStore,
      scenario: String
  ): Ref.QualifiedName = {
    Ref.QualifiedName.fromString(scenario) match {
      case Left(_) =>
        throw new RuntimeException(
          s"Cannot find scenario $scenario in packages ${packages.listLfPackagesSync().keys.mkString("[", ", ", "]")}.")
      case Right(x) => x
    }
  }

  private val transactionIdPrefix = Ref.LedgerString.assertFromString(s"scenario-transaction-")
  private val workflowIdPrefix = Ref.LedgerString.assertFromString(s"scenario-workflow-")
  private val scenarioLoader = Ref.LedgerString.assertFromString("scenario-loader")

  private def executeScenarioStep(
      ledger: ArrayBuffer[(ScenarioTransactionId, LedgerEntry)],
      scenarioRef: Ref.DefinitionRef,
      acs: InMemoryActiveLedgerState,
      time: Time.Timestamp,
      mbOldTxId: Option[ScenarioTransactionId],
      stepId: Int,
      step: L.ScenarioStep
  ): (InMemoryActiveLedgerState, Time.Timestamp, Option[ScenarioTransactionId]) = {
    step match {
      case L.Commit(txId: ScenarioTransactionId, richTransaction: L.RichTransaction, _) =>
        mbOldTxId match {
          case None => ()
          case Some(oldTxId) =>
            if (oldTxId >= txId) {
              throw new RuntimeException(
                s"Non-monotonic transaction ids in ledger results: got $oldTxId first and then $txId")
            }
        }

        val transactionId = txId.id
        val workflowId =
          Some(Ref.LedgerString.assertConcat(workflowIdPrefix, Ref.LedgerString.fromInt(stepId)))
        val tx = GenTransaction(richTransaction.nodes, richTransaction.roots)
        val mappedExplicitDisclosure = richTransaction.explicitDisclosure
        val mappedLocalImplicitDisclosure = richTransaction.localImplicitDisclosure
        val mappedGlobalImplicitDisclosure = richTransaction.globalImplicitDisclosure
        // copies non-absolute-able node IDs, but IDs that don't match
        // get intersected away later
        acs.addTransaction(
          time.toInstant,
          transactionId,
          workflowId,
          Some(richTransaction.committer),
          tx,
          mappedExplicitDisclosure,
          mappedGlobalImplicitDisclosure,
          List.empty
        ) match {
          case Right(newAcs) =>
            ledger +=
              (
                (
                  txId,
                  LedgerEntry.Transaction(
                    Some(transactionId),
                    transactionId,
                    Some(scenarioLoader),
                    Some(richTransaction.committer),
                    workflowId,
                    time.toInstant,
                    time.toInstant,
                    tx,
                    mappedExplicitDisclosure
                  )))
            (newAcs, time, Some(txId))
          case Left(err) =>
            throw new RuntimeException(s"Error when augmenting acs at step $stepId: $err")
        }
      case _: L.AssertMustFail =>
        (acs, time, mbOldTxId)
      case L.PassTime(dtMicros) =>
        (acs, time.addMicros(dtMicros), mbOldTxId)
    }
  }

}
