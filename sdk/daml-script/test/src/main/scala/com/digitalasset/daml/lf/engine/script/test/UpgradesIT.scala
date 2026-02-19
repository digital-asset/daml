// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.digitalasset.daml.lf.UpgradeTestUtil
import com.digitalasset.daml.lf.UpgradeTestUtil.TestCase
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray}
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient
import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.engine.ScriptEngine.{
  defaultCompilerConfig,
  newTraceLog,
  newWarningLog,
}
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.GrpcLedgerClient
import com.digitalasset.daml.lf.value.Value
import com.google.protobuf.ByteString
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.io.FileInputStream
import java.nio.file.{Path, Paths}
import scala.concurrent.Future

class UpgradesIT(
    runCantonInDevMode: Boolean,
    languageVersion: LanguageVersion,
    upgradeTestLibDarPath: String,
    testFilesDirPath: String,
    numParticipants: Int,
    testCaseFilter: TestCase => Boolean,
) extends AsyncWordSpec
    with AbstractScriptTest
    with Inside
    with Matchers {

  final override protected lazy val nParticipants = numParticipants
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

  final override protected lazy val devMode = runCantonInDevMode
  final override protected val disableUpgradeValidation = true

  override val majorLanguageVersion: LanguageVersion.Major = languageVersion.major

  override protected lazy val darFiles = List()

  lazy val upgradeTestLibDar: Path = rlocation(Paths.get(upgradeTestLibDarPath))

  val testUtil = new UpgradeTestUtil(upgradeTestLibDar)

  val testFileDir: Path = rlocation(Paths.get(testFilesDirPath))
  val testCases: Seq[TestCase] = UpgradeTestUtil.getTestCases(languageVersion, testFileDir)

  private def traverseSequential[A, B](elems: Seq[A])(f: A => Future[B]): Future[Seq[B]] =
    elems.foldLeft(Future.successful(Seq.empty[B])) { case (comp, elem) =>
      comp.flatMap { elems => f(elem).map(elems :+ _) }
    }

  // Maybe provide our own tracer that doesn't tag, it makes the logs very long
  "Multi-participant Daml Script Upgrades" should {
    testCases.filter(testCaseFilter).foreach { testCase =>
      (testCase.name + " on IDE Ledger") in {
        for {
          // Build dars
          (testDarPath, _) <- testUtil.buildTestCaseDarMemoized(languageVersion, testCase)

          // Create ide ledger client
          testDar = CompiledDar.read(testDarPath, defaultCompilerConfig)
          participants <- Runner.ideLedgerClient(
            testDar.compiledPackages,
            newTraceLog,
            newWarningLog,
          )

          // Run tests
          _ <- run(
            participants,
            QualifiedName.assertFromString(s"${testCase.name}:main"),
            inputValue = Some(
              mkInitialTestState(
                testCaseName = testCase.name,
                runMode = RunMode.IdeLedger,
                utilPackageId = testUtil.upgradeTestLibPackageId,
              )
            ),
            dar = testDar,
          ).recoverWith {
            case com.digitalasset.daml.lf.interpretation.Error.Upgrade.TranslationFailed
                  .InvalidExtendedValue(msg) =>
              Future.failed(new RuntimeException(msg))
          }
        } yield succeed
      }
      (testCase.name + " on Canton") in {
        for {
          // Build dars
          (testDarPath, deps) <- testUtil.buildTestCaseDarMemoized(languageVersion, testCase)

          // Upload dars
          client <- defaultLedgerClient()
          ledgerClient = new GrpcLedgerClient(
            client,
            None,
            PureCompiledPackages.Empty(defaultCompilerConfig),
          )
          defaultParticipantUid <- ledgerClient.getParticipantUid()
          scriptClients <- scriptClients()
          _ <- traverseSequential(scriptClients.participants.toSeq) { case (participant, client) =>
            Future.traverse(deps.reverse) { dep =>
              Thread.sleep(500)
              println(
                s"Uploading ${dep.versionedName} (${dep.mainPackageId}) to participant ${participant.participant}"
              )

              client.grpcClient.packageManagementClient
                .uploadDarFile(ByteString.readFrom(new FileInputStream(dep.path.toFile)))
            }
          }

          // Vet dars
          pkgs = deps
            .map(dep => ScriptLedgerClient.ReadablePackageId.assertFromString(dep.versionedName))
            .toList
          _ <- ledgerClient.vetPackages(pkgs)
          _ <- ledgerClient.waitUntilVettingVisible(pkgs, defaultParticipantUid)
          _ = println("All packages vetted on all participants")

          // Run tests
          testDar = CompiledDar.read(testDarPath, defaultCompilerConfig)
          _ <- run(
            scriptClients,
            QualifiedName.assertFromString(s"${testCase.name}:main"),
            inputValue = Some(
              mkInitialTestState(
                testCaseName = testCase.name,
                runMode = RunMode.Canton,
                utilPackageId = testUtil.upgradeTestLibPackageId,
              )
            ),
            dar = testDar,
          ).recoverWith {
            case com.digitalasset.daml.lf.interpretation.Error.Upgrade.TranslationFailed
                  .LookupError(err) =>
              Future.failed(new RuntimeException(err.toString))
            case com.digitalasset.daml.lf.interpretation.Error.Upgrade.TranslationFailed
                  .InvalidExtendedValue(err) =>
              Future.failed(new RuntimeException(err))
          }
        } yield succeed
      }
    }
  }

  sealed class RunMode(nameString: String) {
    val name: Name = Name.assertFromString(nameString)
  }
  object RunMode {
    case object Canton extends RunMode("Canton")
    case object IdeLedger extends RunMode("IdeLedger")
  }

  private def mkInitialTestState(
      testCaseName: String,
      runMode: RunMode,
      utilPackageId: PackageId,
  ): Value = {
    Value.ValueRecord(
      Some(Identifier(utilPackageId, QualifiedName.assertFromString("UpgradeTestLib:TestState"))),
      ImmArray(
        (
          Some(Name.assertFromString("runMode")),
          Value.ValueEnum(
            Some(
              Identifier(utilPackageId, QualifiedName.assertFromString("UpgradeTestLib:RunMode"))
            ),
            runMode.name,
          ),
        ),
        (
          Some(Name.assertFromString("testPath")),
          Value.ValueList(FrontStack(Value.ValueText(testCaseName))),
        ),
      ),
    )
  }
}

// Because the test cases are listed even before the canton fixture is initialized,
// `bazel run --sandbox_debug -- -z SubViews` will not wastefully spawn canton for UpgradesITSmallStable. It is thus
// safe to break down tests into many classes like this.
class UpgradesITSmallStable
    extends UpgradesIT(
      runCantonInDevMode = false,
      languageVersion = LanguageVersion.latestStableLfVersion,
      upgradeTestLibDarPath = "daml-script/test/upgrade-test-lib.dar",
      testFilesDirPath = "daml-script/test/daml/upgrades/stable",
      numParticipants = 2,
      testCaseFilter = _.name != "SubViews",
    )

class UpgradesITLargeStable
    extends UpgradesIT(
      runCantonInDevMode = false,
      languageVersion = LanguageVersion.latestStableLfVersion,
      upgradeTestLibDarPath = "daml-script/test/upgrade-test-lib.dar",
      testFilesDirPath = "daml-script/test/daml/upgrades/stable",
      numParticipants = 5,
      testCaseFilter = _.name == "SubViews",
    )

class UpgradesITSmallDev
    extends UpgradesIT(
      runCantonInDevMode = true,
      languageVersion = LanguageVersion.devLfVersion,
      upgradeTestLibDarPath = "daml-script/test/upgrade-test-lib-dev.dar",
      testFilesDirPath = "daml-script/test/daml/upgrades/dev",
      numParticipants = 2,
      testCaseFilter = _.name != "SubViews",
    )
