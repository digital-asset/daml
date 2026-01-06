// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.engine.{Error => EE}
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import org.scalatest.Inside.inside
import org.scalatest.{Assertion, ParallelTestExecution}
import com.digitalasset.daml.lf.transaction.CreationTime
import com.digitalasset.daml.lf.speedy.ValueTranslator
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder

import scala.collection.immutable
import scala.concurrent.Future

// Split the Upgrade unit tests over four suites, which seems to be the sweet
// spot (~95s instead of ~185s runtime)
class UpgradesMatrixUnit0 extends UpgradesMatrixUnit(UpgradesMatrixCasesV2MaxStable, 2, 0)
class UpgradesMatrixUnit1 extends UpgradesMatrixUnit(UpgradesMatrixCasesV2MaxStable, 2, 1)
class UpgradesMatrixUnit2 extends UpgradesMatrixUnit(UpgradesMatrixCasesV2Dev, 2, 0)
class UpgradesMatrixUnit3 extends UpgradesMatrixUnit(UpgradesMatrixCasesV2Dev, 2, 1)

/** A test suite to run the UpgradesMatrix matrix directly in the engine
  *
  * This runs a lot more quickly (~35s on a single suite) than UpgradesMatrixIT
  * (~5000s) because it does not need to spin up Canton, so we can use this for
  * sanity checking before running UpgradesMatrixIT.
  */
abstract class UpgradesMatrixUnit(upgradesMatrixCases: UpgradesMatrixCases, n: Int, k: Int)
    extends UpgradesMatrix[Error, (SubmittedTransaction, Transaction.Metadata)](
      upgradesMatrixCases,
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
        clientLocalContractId = toContractId("client-local"),
        clientGlobalContractId = toContractId("client-global"),
        globalContractId = toContractId("1"),
      )
    )

  def normalize(value: Value, typ: Ast.Type): Value = {
    new ValueTranslator(
      cases.compiledPackages.pkgInterface,
      forbidLocalContractIds = true,
      forbidTrailingNones = false,
    )
      .translateValue(typ, value) match {
      case Left(err) => throw new RuntimeException(s"Normalization failed: $err")
      case Right(sValue) => sValue.toNormalizedValue
    }
  }

  def newEngine() = new Engine(cases.engineConfig)

  override def execute(
      setupData: UpgradesMatrixCases.SetupData,
      testHelper: cases.TestHelper,
      apiCommands: ImmArray[ApiCommand],
      contractOrigin: UpgradesMatrixCases.ContractOrigin,
      creationPackageStatus: UpgradesMatrixCases.CreationPackageStatus,
  ): Future[Either[Error, (SubmittedTransaction, Transaction.Metadata)]] = Future {
    val clientLocalContract: FatContractInstance =
      TransactionBuilder.fatContractInstanceWithDummyDefaults(
        version = cases.serializationVersion,
        packageName = cases.clientLocalPkg.pkgName,
        template = testHelper.clientLocalTplId,
        arg = testHelper.clientContractArg(setupData.alice, setupData.bob),
        signatories = immutable.Set(setupData.alice),
      )

    val clientGlobalContract: FatContractInstance =
      TransactionBuilder.fatContractInstanceWithDummyDefaults(
        version = cases.serializationVersion,
        packageName = cases.clientGlobalPkg.pkgName,
        template = testHelper.clientGlobalTplId,
        arg = testHelper.clientContractArg(setupData.alice, setupData.bob),
        signatories = immutable.Set(setupData.alice),
      )

    val globalContract: FatContractInstance = FatContractInstanceImpl(
      version = cases.serializationVersion,
      contractId = setupData.globalContractId,
      packageName = cases.templateDefsPkgName,
      templateId = testHelper.v1TplId,
      createArg = normalize(
        testHelper.globalContractArg(setupData.alice, setupData.bob),
        Ast.TTyCon(testHelper.v1TplId),
      ),
      signatories = immutable.TreeSet(setupData.alice),
      stakeholders = immutable.TreeSet(setupData.alice),
      contractKeyWithMaintainers = testHelper.globalContractKeyWithMaintainers(setupData),
      createdAt = CreationTime.CreatedAt(Time.Timestamp.Epoch),
      authenticationData = Bytes.assertFromString("00"),
    )

    val participant = ParticipantId.assertFromString("participant")
    val submissionSeed = crypto.Hash.hashPrivateKey("command")
    val submitters = Set(setupData.alice)
    val readAs = Set.empty[Party]

    val lookupContractById = contractOrigin match {
      case UpgradesMatrixCases.Global | UpgradesMatrixCases.Disclosed =>
        Map(
          setupData.clientLocalContractId -> clientLocalContract,
          setupData.clientGlobalContractId -> clientGlobalContract,
          setupData.globalContractId -> globalContract,
        )
      case _ =>
        Map(
          setupData.clientLocalContractId -> clientLocalContract,
          setupData.clientGlobalContractId -> clientGlobalContract,
        )
    }
    val lookupContractByKey = contractOrigin match {
      case UpgradesMatrixCases.Global | UpgradesMatrixCases.Disclosed =>
        (
            (kwm: GlobalKeyWithMaintainers) =>
              testHelper
                .globalContractKeyWithMaintainers(setupData)
                .flatMap(helperKey =>
                  Option.when(helperKey.globalKey == kwm.globalKey)(setupData.globalContractId)
                )
        ).unlift
      case _ => PartialFunction.empty
    }

    def hash(fci: FatContractInstance): crypto.Hash =
      newEngine()
        .hashCreateNode(fci.toCreateNode, identity, crypto.Hash.HashingMethod.TypedNormalForm)
        .consume(pkgs = cases.allPackages)
        .fold(e => throw new IllegalArgumentException(s"hashing $fci failed: $e"), identity)

    val hashes = Map(
      setupData.clientLocalContractId -> hash(clientLocalContract),
      setupData.clientGlobalContractId -> hash(clientGlobalContract),
      setupData.globalContractId -> hash(globalContract),
    )

    newEngine()
      .submit(
        packageMap = cases.packageMap,
        packagePreference = Set(
          cases.commonDefsPkgId,
          cases.templateDefsV2PkgId,
          cases.clientLocalPkgId,
          cases.clientGlobalPkgId,
        ),
        submitters = submitters,
        readAs = readAs,
        cmds = ApiCommands(apiCommands, Time.Timestamp.Epoch, "test"),
        participantId = participant,
        submissionSeed = submissionSeed,
        contractIdVersion = cases.contractIdVersion,
        prefetchKeys = Seq.empty,
      )
      .consume(
        pcs = lookupContractById,
        pkgs = creationPackageStatus match {
          case UpgradesMatrixCases.CreationPackageVetted => cases.allPackages
          case UpgradesMatrixCases.CreationPackageUnvetted => cases.allNonCreationPackages
        },
        keys = lookupContractByKey,
        idValidator = (cid, hash) =>
          hashes.get(cid) match {
            case Some(expectedHash) => hash == expectedHash
            case None => false
          },
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
      case UpgradesMatrixCases.ExpectAuthenticationError =>
        inside(result) {
          case Left(EE.Interpretation(EE.Interpretation.DamlException(IE.Upgrade(error)), _)) =>
            error shouldBe a[IE.Upgrade.AuthenticationFailed]
        }
      case UpgradesMatrixCases.ExpectRuntimeTypeMismatchError =>
        inside(result) {
          case Left(EE.Interpretation(EE.Interpretation.DamlException(IE.Upgrade(error)), _)) =>
            error shouldBe a[IE.Upgrade.TranslationFailed]
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
