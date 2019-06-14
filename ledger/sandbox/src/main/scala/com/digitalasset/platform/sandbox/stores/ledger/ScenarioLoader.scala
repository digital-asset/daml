// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import com.digitalasset.daml.lf.CompiledPackages
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.engine.DeprecatedIdentifier
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Ast.{DDataType, DValue, Definition}
import com.digitalasset.daml.lf.speedy.{ScenarioRunner, Speedy}
import com.digitalasset.daml.lf.types.{Ledger => L}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.platform.sandbox.stores.{InMemoryActiveContracts, InMemoryPackageStore}
import org.slf4j.LoggerFactory
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.types.Ledger.ScenarioTransactionId
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry.Transaction

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
  case class LedgerEntryWithLedgerEndIncrement(entry: LedgerEntry, increment: Long)

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
      scenario: String)
    : (InMemoryActiveContracts, ImmArray[LedgerEntryWithLedgerEndIncrement], Instant) = {
    val (scenarioLedger, scenarioRef) = buildScenarioLedger(packages, compiledPackages, scenario)
    // we store the tx id since later we need to recover how much to bump the
    // ledger end by, and here the transaction id _is_ the ledger end.
    val ledgerEntries =
      new ArrayBuffer[(ScenarioTransactionId, LedgerEntry)](scenarioLedger.scenarioSteps.size)
    type Acc = (InMemoryActiveContracts, Time.Timestamp, Option[ScenarioTransactionId])
    val (acs, time, txId) =
      scenarioLedger.scenarioSteps.iterator
        .foldLeft[Acc]((InMemoryActiveContracts.empty, Time.Timestamp.Epoch, None)) {
          case ((acs, time, mbOldTxId), (stepId @ _, step)) =>
            executeScenarioStep(ledgerEntries, scenarioRef, acs, time, mbOldTxId, stepId, step)
        }
    // now decorate the entries with what the next increment is
    @tailrec
    def decorateWithIncrement(
        processed: BackStack[LedgerEntryWithLedgerEndIncrement],
        toProcess: ImmArray[(ScenarioTransactionId, LedgerEntry)])
      : ImmArray[LedgerEntryWithLedgerEndIncrement] =
      toProcess match {
        case ImmArray() => processed.toImmArray
        // the last one just bumps by 1 -- it does not matter as long as it's
        // positive
        case ImmArrayCons((_, entry), ImmArray()) =>
          (processed :+ LedgerEntryWithLedgerEndIncrement(entry, 1)).toImmArray
        case ImmArrayCons((entryTxId, entry), entries @ ImmArrayCons((nextTxId, next), _)) =>
          decorateWithIncrement(
            processed :+ LedgerEntryWithLedgerEndIncrement(
              entry,
              (nextTxId.index - entryTxId.index).toLong),
            entries)
      }
    (acs, decorateWithIncrement(BackStack.empty, ImmArray(ledgerEntries)), time.toInstant)
  }

  private def buildScenarioLedger(
      packages: InMemoryPackageStore,
      compiledPackages: CompiledPackages,
      scenario: String): (L.Ledger, Ref.DefinitionRef) = {
    val scenarioQualName = getScenarioQualifiedName(packages, scenario)
    val candidateScenarios: List[(Ref.DefinitionRef, Definition)] =
      getCandidateScenarios(packages, scenarioQualName)
    val (scenarioRef, scenarioDef) = identifyScenario(packages, scenario, candidateScenarios)
    val scenarioExpr = getScenarioExpr(scenarioRef, scenarioDef)
    val speedyMachine = getSpeedyMachine(scenarioExpr, compiledPackages)
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
      scenarioExpr: Ast.Expr,
      compiledPackages: CompiledPackages): Speedy.Machine = {
    Speedy.Machine.newBuilder(compiledPackages) match {
      case Left(err) => throw new RuntimeException(s"Could not build speedy machine: $err")
      case Right(build) => build(scenarioExpr)
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
  ): List[(Ref.Identifier, Definition)] = {
    packages
      .listLfPackagesSync()
      .flatMap {
        case (packageId, _) =>
          val pkg = packages.getLfPackageSync(packageId).get
          pkg.lookupIdentifier(scenarioQualName) match {
            case Right(x) => List((Ref.Identifier(packageId, scenarioQualName), x))
            case Left(_) => List()
          }
      }(breakOut)
  }

  private def getScenarioQualifiedName(
      packages: InMemoryPackageStore,
      scenario: String
  ): Ref.QualifiedName = {
    Ref.QualifiedName.fromString(scenario) match {
      case Left(err) =>
        logger.warn(
          "Dot-separated scenario specification is deprecated. Names are Module.Name:Inner.Name, with a colon between module name and the name of the definition. Falling back to deprecated name resolution.")
        packages
          .listLfPackagesSync()
          .iterator
          .map {
            case (pkgId, _) =>
              DeprecatedIdentifier.lookup(packages.getLfPackageSync(pkgId).get, scenario)
          }
          .collectFirst {
            case Right(qualifiedName) => qualifiedName
          }
          .getOrElse {
            throw new RuntimeException(
              s"Cannot find scenario $scenario in packages ${packages.listLfPackagesSync().keys.mkString("[", ", ", "]")}. Try using Module.Name:Inner.Name style scenario name specification.")
          }
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
      acs: InMemoryActiveContracts,
      time: Time.Timestamp,
      mbOldTxId: Option[ScenarioTransactionId],
      stepId: Int,
      step: L.ScenarioStep
  ): (InMemoryActiveContracts, Time.Timestamp, Option[ScenarioTransactionId]) = {
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
                    transactionId,
                    transactionId,
                    scenarioLoader,
                    richTransaction.committer,
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
