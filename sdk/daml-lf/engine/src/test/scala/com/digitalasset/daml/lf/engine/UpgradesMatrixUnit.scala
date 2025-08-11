// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.engine.{Error => EE}
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.speedy.SExpr.SEImportValue
import com.digitalasset.daml.lf.speedy.Speedy.Machine
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import org.scalatest.Inside.inside
import org.scalatest.{Assertion, ParallelTestExecution}
import com.digitalasset.daml.lf.transaction.CreationTime
import com.digitalasset.daml.lf.engine.UpgradesMatrix

import scala.collection.immutable
import scala.concurrent.Future

// Split the Upgrade unit tests over two suites, which seems to be the sweet
// spot (~25s instead of ~35s runtime)
class UpgradesMatrixUnit0 extends UpgradesMatrixUnit(2, 0)
class UpgradesMatrixUnit1 extends UpgradesMatrixUnit(2, 1)

/** A test suite to run the UpgradesMatrix matrix directly in the engine
  *
  * This runs a lot more quickly (~35s on a single suite) than UpgradesMatrixIT
  * (~5000s) because it does not need to spin up Canton, so we can use this for
  * sanity checking before running UpgradesMatrixIT.
  */
abstract class UpgradesMatrixUnit(n: Int, k: Int)
    extends UpgradesMatrix[Error, (SubmittedTransaction, Transaction.Metadata)](
      UpgradesMatrixCasesV2MaxStable,
      Some((n, k)),
    )
    with ParallelTestExecution {
  def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))

  override def setup(testHelper: cases.TestHelper): Future[UpgradesMatrixCases.SetupData] =
    Future.successful(
      UpgradesMatrixCases.SetupData(
        alice = Party.assertFromString("Alice"),
        bob = Party.assertFromString("Bob"),
        clientContractId = toContractId("client"),
        globalContractId = toContractId("1"),
      )
    )

  def normalize(value: Value, typ: Ast.Type): Value = {
    Machine.fromPureSExpr(cases.compiledPackages, SEImportValue(typ, value)).runPure() match {
      case Left(err) => throw new RuntimeException(s"Normalization failed: $err")
      case Right(sValue) => sValue.toNormalizedValue(cases.langVersion)
    }
  }

  def newEngine() = new Engine(cases.engineConfig)

  override def execute(
      setupData: UpgradesMatrixCases.SetupData,
      testHelper: cases.TestHelper,
      apiCommands: ImmArray[ApiCommand],
      contractOrigin: UpgradesMatrixCases.ContractOrigin,
  ): Future[Either[Error, (SubmittedTransaction, Transaction.Metadata)]] = Future {
    val clientContract: FatContractInstance =
      FatContractInstance.fromThinInstance(
        version = cases.langVersion,
        packageName = cases.clientPkg.pkgName,
        template = testHelper.clientTplId,
        arg = testHelper.clientContractArg(setupData.alice, setupData.bob),
      )

    val globalContract: FatContractInstance =
      FatContractInstance.fromThinInstance(
        version = cases.langVersion,
        packageName = cases.clientPkg.pkgName,
        template = testHelper.v1TplId,
        arg = testHelper.globalContractArg(setupData.alice, setupData.bob),
      )

    val globalContractDisclosure: FatContractInstance = FatContractInstanceImpl(
      version = cases.langVersion,
      contractId = setupData.globalContractId,
      packageName = cases.templateDefsPkgName,
      templateId = testHelper.v1TplId,
      createArg = normalize(
        testHelper.globalContractArg(setupData.alice, setupData.bob),
        Ast.TTyCon(testHelper.v1TplId),
      ),
      signatories = immutable.TreeSet(setupData.alice),
      stakeholders = immutable.TreeSet(setupData.alice),
      contractKeyWithMaintainers = Some(testHelper.globalContractKeyWithMaintainers(setupData)),
      createdAt = CreationTime.CreatedAt(Time.Timestamp.Epoch),
      authenticationData = Bytes.assertFromString("00"),
    )

    val participant = ParticipantId.assertFromString("participant")
    val submissionSeed = crypto.Hash.hashPrivateKey("command")
    val submitters = Set(setupData.alice)
    val readAs = Set.empty[Party]

    val disclosures = contractOrigin match {
      case UpgradesMatrixCases.Disclosed => ImmArray(globalContractDisclosure)
      case _ => ImmArray.empty
    }
    val lookupContractById = contractOrigin match {
      case UpgradesMatrixCases.Global =>
        Map(
          setupData.clientContractId -> clientContract,
          setupData.globalContractId -> globalContract,
        )
      case _ => Map(setupData.clientContractId -> clientContract)
    }
    val lookupContractByKey = contractOrigin match {
      case UpgradesMatrixCases.Global =>
        val keyMap = Map(
          testHelper
            .globalContractKeyWithMaintainers(setupData)
            .globalKey -> setupData.globalContractId
        )
        ((kwm: GlobalKeyWithMaintainers) => keyMap.get(kwm.globalKey)).unlift
      case _ => PartialFunction.empty
    }

    newEngine()
      .submit(
        packageMap = cases.packageMap,
        packagePreference =
          Set(cases.commonDefsPkgId, cases.templateDefsV2PkgId, cases.clientPkgId),
        submitters = submitters,
        readAs = readAs,
        cmds = ApiCommands(apiCommands, Time.Timestamp.Epoch, "test"),
        disclosures = disclosures,
        participantId = participant,
        submissionSeed = submissionSeed,
        prefetchKeys = Seq.empty,
      )
      .consume(
        pcs = lookupContractById,
        pkgs = cases.lookupPackage,
        keys = lookupContractByKey,
      )
  }

  override def assertResultMatchesExpectedOutcome(
      result: Either[Error, (SubmittedTransaction, Transaction.Metadata)],
      expectedOutcome: UpgradesMatrixCases.ExpectedOutcome,
  ): Assertion = {
    expectedOutcome match {
      case UpgradesMatrixCases.ExpectSuccess =>
        result shouldBe a[Right[_, _]]
      case UpgradesMatrixCases.ExpectUpgradeError =>
        inside(result) { case Left(EE.Interpretation(EE.Interpretation.DamlException(error), _)) =>
          error shouldBe a[IE.Upgrade]
        }
      case UpgradesMatrixCases.ExpectPreprocessingError =>
        inside(result) { case Left(error) =>
          error shouldBe a[EE.Preprocessing]
        }
      case UpgradesMatrixCases.ExpectPreconditionViolated =>
        inside(result) { case Left(EE.Interpretation(EE.Interpretation.DamlException(error), _)) =>
          error shouldBe a[IE.TemplatePreconditionViolated]
        }
      case UpgradesMatrixCases.ExpectUnhandledException =>
        inside(result) { case Left(EE.Interpretation(EE.Interpretation.DamlException(error), _)) =>
          error shouldBe a[IE.FailureStatus]
        }
      case UpgradesMatrixCases.ExpectInternalInterpretationError =>
        inside(result) { case Left(EE.Interpretation(error, _)) =>
          error shouldBe a[EE.Interpretation.Internal]
        }
    }
  }
}
