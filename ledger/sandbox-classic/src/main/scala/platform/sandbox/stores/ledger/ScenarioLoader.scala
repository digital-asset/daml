// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import java.time.Instant

import com.daml.lf.crypto
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.DefinitionRef
import com.daml.lf.data.{Relation => _, _}
import com.daml.lf.engine.Engine
import com.daml.lf.language.Ast
import com.daml.lf.scenario.{ScenarioLedger, ScenarioRunner}
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.stores.InMemoryActiveLedgerState
import com.daml.platform.store.entries.LedgerEntry

import scala.annotation.{nowarn, tailrec}
import scala.collection.mutable.ArrayBuffer

private[sandbox] object ScenarioLoader {

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

  /** @param packages All the packages where we're going to look for the scenario definition.
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
      engine: Engine,
      scenario: String,
      transactionSeed: crypto.Hash,
  ): (InMemoryActiveLedgerState, ImmArray[LedgerEntryOrBump], Instant) = {
    val (scenarioLedger, _) =
      buildScenarioLedger(packages, engine, scenario, transactionSeed)
    // we store the tx id since later we need to recover how much to bump the
    // ledger end by, and here the transaction id _is_ the ledger end.
    val ledgerEntries =
      new ArrayBuffer[(ScenarioLedger.TransactionId, LedgerEntry)](
        scenarioLedger.scenarioSteps.size
      )
    type Acc = (InMemoryActiveLedgerState, Time.Timestamp, Option[ScenarioLedger.TransactionId])
    val (acs, time, _) =
      scenarioLedger.scenarioSteps.iterator
        .foldLeft[Acc]((InMemoryActiveLedgerState.empty, Time.Timestamp.Epoch, None)) {
          case ((acs, time, mbOldTxId), (stepId @ _, step)) =>
            executeScenarioStep(ledgerEntries, acs, time, mbOldTxId, stepId, step)
        }
    // now decorate the entries with what the next increment is
    @tailrec
    def decorateWithIncrement(
        processed: BackStack[LedgerEntryOrBump],
        toProcess: ImmArray[(ScenarioLedger.TransactionId, LedgerEntry)],
    ): ImmArray[LedgerEntryOrBump] = {

      def bumps(entryTxId: ScenarioLedger.TransactionId, nextTxId: ScenarioLedger.TransactionId) =
        if ((nextTxId.index - entryTxId.index) == 1)
          ImmArray.empty
        else
          ImmArray(LedgerEntryOrBump.Bump((nextTxId.index - entryTxId.index - 1)))

      toProcess match {
        case ImmArray() => processed.toImmArray
        // we have to bump the offsets when the first one is not zero (passTimes),
        case ImmArrayCons((entryTxId, entry), entries @ ImmArrayCons((nextTxId, _), _))
            if (processed.isEmpty && entryTxId.index > 0) =>
          val newProcessed = (processed :++ ImmArray(
            LedgerEntryOrBump.Bump(entryTxId.index),
            LedgerEntryOrBump.Entry(entry),
          )) :++ bumps(entryTxId, nextTxId)

          decorateWithIncrement(newProcessed, entries)
        // the last one just bumps by 1 -- it does not matter as long as it's
        // positive
        case ImmArrayCons((_, entry), ImmArray()) =>
          (processed :+ LedgerEntryOrBump.Entry(entry)).toImmArray

        case ImmArrayCons((entryTxId, entry), entries @ ImmArrayCons((nextTxId, _), _)) =>
          val newProcessed =
            processed :+ LedgerEntryOrBump.Entry(entry) :++ bumps(entryTxId, nextTxId)

          decorateWithIncrement(newProcessed, entries)
      }
    }
    (acs, decorateWithIncrement(BackStack.empty, ledgerEntries.to(ImmArray)), time.toInstant)
  }

  private[this] def buildScenarioLedger(
      packages: InMemoryPackageStore,
      engine: Engine,
      scenario: String,
      transactionSeed: crypto.Hash,
  ): (ScenarioLedger, Ref.DefinitionRef) = {
    val scenarioQualName = getScenarioQualifiedName(packages, scenario)
    val candidateScenarios = getCandidateScenarios(packages, scenarioQualName)
    val (scenarioRef, scenarioDef) = identifyScenario(packages, scenario, candidateScenarios)
    val scenarioLedger = getScenarioLedger(engine, transactionSeed, scenarioRef, scenarioDef)
    (scenarioLedger, scenarioRef)
  }

  @nowarn("cat=deprecation&origin=com\\.daml\\.lf\\.scenario\\.ScenarioRunner\\.getScenarioLedger")
  private def getScenarioLedger(
      engine: Engine,
      transactionSeed: Hash,
      scenarioRef: DefinitionRef,
      scenarioDef: Ast.Definition,
  ) = ScenarioRunner.getScenarioLedger(engine, scenarioRef, scenarioDef, transactionSeed)

  private def identifyScenario(
      packages: InMemoryPackageStore,
      scenario: String,
      candidateScenarios: List[(Ref.DefinitionRef, Ast.Definition)],
  ): (Ref.DefinitionRef, Ast.Definition) = {
    candidateScenarios match {
      case Nil =>
        throw new RuntimeException(
          s"Couldn't find scenario $scenario in packages ${packages.listLfPackagesSync().keys.toList}"
        )
      case candidate :: Nil => candidate
      case candidates =>
        throw new RuntimeException(
          s"Requested scenario $scenario is present in multiple packages: ${candidates.map(_._1.packageId).toString}"
        )
    }
  }

  private def getCandidateScenarios(
      packages: InMemoryPackageStore,
      scenarioQualName: Ref.QualifiedName,
  ): List[(Ref.Identifier, Ast.Definition)] = {
    packages
      .listLfPackagesSync()
      .view
      .flatMap { case (packageId, _) =>
        val pkg = packages
          .getLfPackageSync(packageId)
          .getOrElse(sys.error(s"Listed package $packageId not found"))
        pkg.modules
          .get(scenarioQualName.module)
          .flatMap(_.definitions.get(scenarioQualName.name)) match {
          case Some(x) => List(Ref.Identifier(packageId, scenarioQualName) -> x)
          case None => List()
        }
      }
      .toList
  }

  private def getScenarioQualifiedName(
      packages: InMemoryPackageStore,
      scenario: String,
  ): Ref.QualifiedName = {
    Ref.QualifiedName.fromString(scenario) match {
      case Left(_) =>
        throw new RuntimeException(
          s"Cannot find scenario $scenario in packages ${packages.listLfPackagesSync().keys.mkString("[", ", ", "]")}."
        )
      case Right(x) => x
    }
  }

  private val submissionIdPrefix = Ref.LedgerString.assertFromString("scenario-submission-")
  private val workflowIdPrefix = Ref.LedgerString.assertFromString("scenario-workflow-")
  private val scenarioLoader = Ref.LedgerString.assertFromString("scenario-loader")

  private def executeScenarioStep(
      ledger: ArrayBuffer[(ScenarioLedger.TransactionId, LedgerEntry)],
      acs: InMemoryActiveLedgerState,
      time: Time.Timestamp,
      mbOldTxId: Option[ScenarioLedger.TransactionId],
      stepId: Int,
      step: ScenarioLedger.ScenarioStep,
  ): (InMemoryActiveLedgerState, Time.Timestamp, Option[ScenarioLedger.TransactionId]) = {
    step match {
      case ScenarioLedger.Commit(
            txId: ScenarioLedger.TransactionId,
            richTransaction: ScenarioLedger.RichTransaction,
            _,
          ) =>
        mbOldTxId match {
          case None => ()
          case Some(oldTxId) =>
            if (oldTxId >= txId) {
              throw new RuntimeException(
                s"Non-monotonic transaction ids in ledger results: got $oldTxId first and then $txId"
              )
            }
        }

        val transactionId = txId.id
        val stepIdString = Ref.LedgerString.fromInt(stepId)
        val submissionId = Some(Ref.SubmissionId.assertConcat(submissionIdPrefix, stepIdString))
        val workflowId = Some(Ref.WorkflowId.assertConcat(workflowIdPrefix, stepIdString))
        val tx = richTransaction.transaction
        // copies non-absolute-able node IDs, but IDs that don't match
        // get intersected away later
        acs.addTransaction(
          time.toInstant,
          transactionId,
          workflowId,
          richTransaction.actAs.toList,
          tx,
          richTransaction.blindingInfo.disclosure,
          richTransaction.blindingInfo.divulgence,
          List.empty,
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
                    submissionId,
                    richTransaction.actAs.toList,
                    workflowId,
                    time.toInstant,
                    time.toInstant,
                    tx,
                    richTransaction.blindingInfo.disclosure,
                  ),
                ),
              )
            (newAcs, time, Some(txId))
          case Left(err) =>
            throw new RuntimeException(s"Error when augmenting acs at step $stepId: $err")
        }
      case _: ScenarioLedger.AssertMustFail =>
        (acs, time, mbOldTxId)
      case ScenarioLedger.PassTime(dtMicros) =>
        (acs, time.addMicros(dtMicros), mbOldTxId)
    }
  }

}
