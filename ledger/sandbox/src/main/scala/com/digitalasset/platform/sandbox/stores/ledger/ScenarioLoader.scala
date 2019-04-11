// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.data.Ref.{
  DefinitionRef,
  QualifiedName,
  Identifier => LfIdentifier,
  PackageId => LfPackageId
}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.engine.DeprecatedIdentifier
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.daml.lf.lfpackage.Ast.{DDataType, DValue, Definition}
import com.digitalasset.daml.lf.speedy.{ScenarioRunner, Speedy}
import com.digitalasset.daml.lf.types.{Ledger => L}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.platform.sandbox.config.DamlPackageContainer
import com.digitalasset.platform.sandbox.stores.ActiveContractsInMemory
import org.slf4j.LoggerFactory
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.types.Ledger.TransactionId
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry.Transaction

import scala.collection.breakOut
import scala.collection.mutable.ArrayBuffer
import scalaz.syntax.std.map._

object ScenarioLoader {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def fromScenario(
      packages: DamlPackageContainer,
      scenario: String): (ActiveContractsInMemory, Seq[LedgerEntry], Instant) = {
    val (scenarioLedger, scenarioRef) = buildScenarioLedger(packages, scenario)
    val ledgerEntries = new ArrayBuffer[LedgerEntry](scenarioLedger.scenarioSteps.size)
    type Acc = (ActiveContractsInMemory, Time.Timestamp, Option[TransactionId])
    val (acs, time, txId) =
      scenarioLedger.scenarioSteps.iterator
        .foldLeft[Acc]((ActiveContractsInMemory.empty, Time.Timestamp.Epoch, None)) {
          case ((acs, time, mbOldTxId), (stepId @ _, step)) =>
            executeScenarioStep(ledgerEntries, scenarioRef, acs, time, mbOldTxId, stepId, step)
        }
    // increment the last transaction id returned by the ledger, since we start from there
    (acs, ledgerEntries, time.toInstant)
  }

  private def buildScenarioLedger(
      packages: DamlPackageContainer,
      scenario: String): (L.Ledger, DefinitionRef[LfPackageId]) = {
    val scenarioQualName: QualifiedName = getScenarioQualifiedName(packages, scenario)
    val candidateScenarios: List[(DefinitionRef[LfPackageId], Definition)] =
      getCandidateScenarios(packages, scenarioQualName)
    val (scenarioRef, scenarioDef) = identifyScenario(packages, scenario, candidateScenarios)
    val scenarioExpr = getScenarioExpr(scenarioRef, scenarioDef)
    val compiledPackages = getCompiledPackages(packages)
    val speedyMachine = getSpeedyMachine(scenarioExpr, compiledPackages)
    val scenarioLedger = getScenarioLedger(scenarioRef, speedyMachine)
    (scenarioLedger, scenarioRef)
  }

  private def getScenarioLedger(
      scenarioRef: DefinitionRef[LfPackageId],
      speedyMachine: Speedy.Machine): L.Ledger = {
    ScenarioRunner(speedyMachine).run match {
      case Left(e) =>
        throw new RuntimeException(s"error running scenario $scenarioRef in scenario $e")
      case Right((_, _, l)) => l
    }
  }

  private def getSpeedyMachine(
      scenarioExpr: Ast.Expr,
      compiledPackages: PureCompiledPackages): Speedy.Machine = {
    Speedy.Machine.newBuilder(compiledPackages) match {
      case Left(err) => throw new RuntimeException(s"Could not build speedy machine: $err")
      case Right(build) => build(scenarioExpr)
    }
  }

  private def getCompiledPackages(packages: DamlPackageContainer): PureCompiledPackages = {
    PureCompiledPackages(packages.packages) match {
      case Left(err) => throw new RuntimeException(s"Could not compile packages: $err")
      case Right(x) => x
    }
  }

  private def getScenarioExpr(
      scenarioRef: DefinitionRef[LfPackageId],
      scenarioDef: Definition): Ast.Expr = {
    scenarioDef match {
      case DValue(_, _, body, _) => body
      case _: DDataType =>
        throw new RuntimeException(
          s"Requested scenario $scenarioRef is a data type, not a definition")
    }
  }

  private def identifyScenario(
      packages: DamlPackageContainer,
      scenario: String,
      candidateScenarios: List[(DefinitionRef[LfPackageId], Definition)])
    : (DefinitionRef[LfPackageId], Definition) = {
    candidateScenarios match {
      case Nil =>
        throw new RuntimeException(
          s"Couldn't find scenario $scenario in packages ${packages.packages.keys.toList}")
      case candidate :: Nil => candidate
      case candidates =>
        throw new RuntimeException(
          s"Requested scenario $scenario is present in multiple packages: ${candidates.map(_._1.packageId).toString}")
    }
  }

  private def getCandidateScenarios(
      packages: DamlPackageContainer,
      scenarioQualName: QualifiedName): List[(LfIdentifier, Definition)] = {
    packages.packages.flatMap {
      case (packageId, pkg) =>
        pkg.lookupIdentifier(scenarioQualName) match {
          case Right(x) => List((LfIdentifier(packageId, scenarioQualName), x))
          case Left(_) => List()
        }
    }(breakOut)
  }

  private def getScenarioQualifiedName(
      packages: DamlPackageContainer,
      scenario: String): QualifiedName = {
    QualifiedName.fromString(scenario) match {
      case Left(err) =>
        logger.warn(
          "Dot-separated scenario specification is deprecated. Names are Module.Name:Inner.Name, with a colon between module name and the name of the definition. Falling back to deprecated name resolution.")
        packages.packages.iterator
          .map {
            case (_, pkg) => DeprecatedIdentifier.lookup(pkg, scenario)
          }
          .collectFirst {
            case Right(qualifiedName) => qualifiedName
          }
          .getOrElse {
            throw new RuntimeException(
              s"Cannot find scenario $scenario in packages ${packages.packages.keys.mkString("[", ", ", "]")}. Try using Module.Name:Inner.Name style scenario name specification.")
          }
      case Right(x) => x
    }
  }

  private def executeScenarioStep(
      ledger: ArrayBuffer[LedgerEntry],
      scenarioRef: DefinitionRef[LfPackageId],
      acs: ActiveContractsInMemory,
      time: Time.Timestamp,
      mbOldTxId: Option[TransactionId],
      stepId: Int,
      step: L.ScenarioStep): (ActiveContractsInMemory, Time.Timestamp, Option[TransactionId]) = {
    step match {
      case L.Commit(txId: TransactionId, richTransaction: L.RichTransaction, _) =>
        mbOldTxId match {
          case None => ()
          case Some(oldTxId) =>
            if (oldTxId >= txId) {
              throw new RuntimeException(
                s"Non-monotonic transaction ids in ledger results: got $oldTxId first and then $txId")
            }
        }
        val transactionId = s"scenario-transaction-$txId"
        val workflowId = s"scenario-workflow-$stepId"
        // note that it's important that we keep the event ids in line with the contract ids, since
        // the sandbox code assumes that in TransactionConversion.
        val txNoHash = GenTransaction(richTransaction.nodes, richTransaction.roots)
        val tx = txNoHash.mapContractIdAndValue(absCidWithHash, _.mapContractId(absCidWithHash))
        val explicitDisclosure = richTransaction.explicitDisclosure
        val implicitDisclosure = richTransaction.implicitDisclosure mapKeys { nid =>
          AbsoluteContractId(nid.id)
        }
        acs.addTransaction[L.NodeId](
          time.toInstant,
          transactionId,
          workflowId,
          tx,
          explicitDisclosure,
          implicitDisclosure) match {
          case Right(newAcs) =>
            val recordTx = tx.mapNodeId(nodeIdWithHash)
            val recordDisclosure = explicitDisclosure.map {
              case (nid, parties) => (nodeIdWithHash(nid), parties)
            }
            ledger +=
              Transaction(
                transactionId,
                transactionId,
                "scenario-loader",
                richTransaction.committer.underlyingString,
                workflowId,
                time.toInstant,
                time.toInstant,
                recordTx,
                recordDisclosure.mapValues(_.map(_.underlyingString))
              )
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

  // currently the scenario interpreter produces the contract ids with no hash prefix,
  // but the sandbox does. add them here too for consistency
  private def absCidWithHash(a: AbsoluteContractId): AbsoluteContractId =
    AbsoluteContractId("#" + a.coid)

  private def nodeIdWithHash(nid: L.NodeId): String = "#" + nid.id

}
