// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Ast.{DDataType, DValue, Definition}
import com.digitalasset.daml.lf.speedy.{ScenarioRunner, Speedy}
import com.digitalasset.daml.lf.types.{Ledger => L}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.platform.sandbox.stores.{InMemoryActiveLedgerState, InMemoryPackageStore}
import org.slf4j.LoggerFactory
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.types.Ledger.ScenarioTransactionId
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry.Transaction
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.VersionTimeline

import scala.collection.breakOut
import scala.collection.mutable.ArrayBuffer
import scalaz.syntax.std.map._

import scala.annotation.tailrec

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
      scenario: String): (InMemoryActiveLedgerState, ImmArray[LedgerEntryOrBump], Instant) = {
    val (scenarioLedger, scenarioRef) = buildScenarioLedger(packages, compiledPackages, scenario)
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
      scenario: String): (L.Ledger, Ref.DefinitionRef) = {
    val scenarioQualName = getScenarioQualifiedName(packages, scenario)
    val candidateScenarios: List[(Ref.DefinitionRef, LanguageVersion, Definition)] =
      getCandidateScenarios(packages, scenarioQualName)
    val (scenarioRef, scenarioLfVers, scenarioDef) =
      identifyScenario(packages, scenario, candidateScenarios)
    val scenarioExpr = getScenarioExpr(scenarioRef, scenarioDef)
    val speedyMachine = getSpeedyMachine(scenarioLfVers, scenarioExpr, compiledPackages)
    val scenarioLedger = getScenarioLedger(scenarioRef, speedyMachine)
    (scenarioLedger, scenarioRef)
  }

  private def getScenarioLedger(
      scenarioRef: Ref.DefinitionRef,
      speedyMachine: Speedy.Machine): L.Ledger = {
    ScenarioRunner(speedyMachine).run match {
      case Left(e) =>
        throw new RuntimeException(s"error running scenario $scenarioRef in scenario $e")
      case Right((_, _, l)) => l
    }
  }

  private def getSpeedyMachine(
      submissionVersion: LanguageVersion,
      scenarioExpr: Ast.Expr,
      compiledPackages: CompiledPackages): Speedy.Machine = {
    Speedy.Machine.newBuilder(compiledPackages) match {
      case Left(err) => throw new RuntimeException(s"Could not build speedy machine: $err")
      case Right(build) =>
        build(VersionTimeline.checkSubmitterInMaintainers(submissionVersion), scenarioExpr)
    }
  }

  private def getScenarioExpr(scenarioRef: Ref.DefinitionRef, scenarioDef: Definition): Ast.Expr = {
    scenarioDef match {
      case DValue(_, _, body, _) => body
      case _: DDataType =>
        throw new RuntimeException(
          s"Requested scenario $scenarioRef is a data type, not a definition")
    }
  }

  private def identifyScenario(
      packages: InMemoryPackageStore,
      scenario: String,
      candidateScenarios: List[(Ref.DefinitionRef, LanguageVersion, Definition)])
    : (Ref.DefinitionRef, LanguageVersion, Definition) = {
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
  ): List[(Ref.Identifier, LanguageVersion, Definition)] = {
    packages
      .listLfPackagesSync()
      .flatMap {
        case (packageId, _) =>
          val pkg = packages
            .getLfPackageSync(packageId)
            .getOrElse(sys.error(s"Listed package $packageId not found"))
          pkg.lookupIdentifier(scenarioQualName) match {
            case Right(x) =>
              List(
                (
                  Ref.Identifier(packageId, scenarioQualName),
                  pkg.modules(scenarioQualName.module).languageVersion,
                  x))
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

  private val transactionIdPrefix =
    Ref.TransactionIdString.assertFromString(s"scenario-transaction-")
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

        val transactionId = Ref.LedgerString.concat(transactionIdPrefix, txId.id)
        val workflowId =
          Some(Ref.LedgerString.concat(workflowIdPrefix, Ref.LedgerString.fromInt(stepId)))
        // note that it's important that we keep the event ids in line with the contract ids, since
        // the sandbox code assumes that in TransactionConversion.
        val txNoHash = GenTransaction(richTransaction.nodes, richTransaction.roots, Set.empty)
        val tx = txNoHash.mapContractIdAndValue(absCidWithHash, _.mapContractId(absCidWithHash))
        import richTransaction.{explicitDisclosure, implicitDisclosure}
        // copies non-absolute-able node IDs, but IDs that don't match
        // get intersected away later
        val globalizedImplicitDisclosure = richTransaction.implicitDisclosure mapKeys { nid =>
          absCidWithHash(AbsoluteContractId(nid))
        }
        acs.addTransaction[L.ScenarioNodeId](
          time.toInstant,
          transactionId,
          workflowId,
          tx,
          explicitDisclosure,
          implicitDisclosure,
          globalizedImplicitDisclosure) match {
          case Right(newAcs) =>
            val recordTx = tx.mapNodeId(nodeIdWithHash)
            val recordDisclosure = explicitDisclosure.map {
              case (nid, parties) => (nodeIdWithHash(nid), parties)
            }

            ledger +=
              (
                (
                  txId,
                  Transaction(
                    Some(transactionId),
                    transactionId,
                    Some(scenarioLoader),
                    Some(richTransaction.committer),
                    workflowId,
                    time.toInstant,
                    time.toInstant,
                    recordTx,
                    recordDisclosure
                  )))
            (newAcs, time, Some(txId))
          case Left(err) =>
            throw new RuntimeException(s"Error when augmenting acs at step $stepId: $err")
        }
      case _: L.AssertMustFail =>
        throw new RuntimeException(
          s"Scenario $scenarioRef contains a must fail -- you cannot use it to initialize the sandbox.")
      case L.PassTime(dtMicros) =>
        (acs, time.addMicros(dtMicros), mbOldTxId)
    }
  }

  private val `#` = Ref.ContractIdString.assertFromString("#")
  // currently the scenario interpreter produces the contract ids with no hash prefix,
  // but the sandbox does. add them here too for consistency
  private def absCidWithHash(a: AbsoluteContractId): AbsoluteContractId =
    AbsoluteContractId(Ref.ContractIdString.concat(`#`, a.coid))

  private def nodeIdWithHash(nid: L.ScenarioNodeId): com.digitalasset.ledger.EventId =
    Ref.ContractIdString.concat(`#`, nid)

}
