// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.timer.RetryStrategy
import com.digitalasset.canton.ledger.client.configuration.LedgerClientChannelConfiguration
import com.digitalasset.daml.lf.UpgradeTestUtil
import com.digitalasset.daml.lf.UpgradeTestUtil.TestCase
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray}
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.engine.script.test.DarUtil.Dar
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.AdminLedgerClient
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.speedy.Speedy.Machine.{newTraceLog, newWarningLog}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.{Path, Paths}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

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

  override val majorLanguageVersion: LanguageMajorVersion = languageVersion.major

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
          testDar = CompiledDar.read(testDarPath, Runner.compilerConfig(LanguageMajorVersion.V2))
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
                runMode = runModeIdeLedger,
              )
            ),
            dar = testDar,
          )
        } yield succeed
      }
      (testCase.name + " on Canton") in {
        for {
          // Build dars
          (testDarPath, deps) <- testUtil.buildTestCaseDarMemoized(languageVersion, testCase)

          // Connection
          clients <- scriptClients(provideAdminPorts = true)
          adminClients <- traverseSequential(ledgerPorts) { portInfo =>
            AdminLedgerClient
              .singleHostWithUnknownParticipantId(
                "localhost",
                portInfo.adminPort.value,
                None,
                LedgerClientChannelConfiguration.InsecureDefaults,
              )
              .map(portInfo.ledgerPort.value -> _)
          }
          _ <- traverseSequential(adminClients) { case (ledgerPort, adminClient) =>
            Future.traverse(deps.reverse) { dep =>
              Thread.sleep(500)
              println(
                s"Uploading ${dep.versionedName} (${dep.mainPackageId}) to participant on port ${ledgerPort}"
              )
              adminClient
                .uploadDar(dep.path.toFile)
                .map(_.left.map(msg => throw new Exception(msg)))
            }
          }

          // Wait for upload
          _ <- RetryStrategy.constant(attempts = 20, waitTime = 1.seconds) { (_, _) =>
            assertDepsVetted(adminClients.head._2, deps)
          }
          _ = println("All packages vetted on all participants")

          // Run tests
          testDar = CompiledDar.read(testDarPath, Runner.compilerConfig(LanguageMajorVersion.V2))
          _ <- run(
            clients,
            QualifiedName.assertFromString(s"${testCase.name}:main"),
            inputValue = Some(
              mkInitialTestState(
                testCaseName = testCase.name,
                runMode = runModeCanton,
              )
            ),
            dar = testDar,
          )
        } yield succeed
      }
    }
  }

  private def mkInitialTestState(
      testCaseName: String,
      runMode: Value,
  ): Value = {
    Value.ValueRecord(
      None,
      ImmArray(
        (None, runMode),
        (None, Value.ValueList(FrontStack(Value.ValueText(testCaseName)))),
      ),
    )
  }

  private val runModeCanton: Value =
    Value.ValueEnum(None, Name.assertFromString("Canton"))

  private val runModeIdeLedger: Value =
    Value.ValueEnum(None, Name.assertFromString("IdeLedger"))

  private def assertDepsVetted(
      client: AdminLedgerClient,
      deps: Seq[Dar],
  ): Future[Unit] = {
    client
      .listVettedPackages()
      .map(_.foreach {
        case (participantId, packages) => {
          val packageIds = packages.view.map(_.packageId).toSet
          deps.foreach { dep =>
            if (!packageIds.contains(dep.mainPackageId))
              throw new Exception(
                s"Couldn't find package ${dep.versionedName} on participant $participantId"
              )
          }
        }
      })
  }
}

// Because the test cases are listed even before the canton fixture is initialized,
// `bazel run --sandbox_debug -- -z SubViews` will not wastefully spawn canton for UpgradesITSmallStable. It is thus
// safe to break down tests into many classes like this.
class UpgradesITSmallStable
    extends UpgradesIT(
      runCantonInDevMode = false,
      languageVersion = LanguageVersion.Major.V2.maxStableVersion,
      upgradeTestLibDarPath = "daml-script/test/upgrade-test-lib.dar",
      testFilesDirPath = "daml-script/test/daml/upgrades/stable",
      numParticipants = 2,
      testCaseFilter = _.name != "SubViews",
    )

class UpgradesITSLargeStable
    extends UpgradesIT(
      runCantonInDevMode = false,
      languageVersion = LanguageVersion.Major.V2.maxStableVersion,
      upgradeTestLibDarPath = "daml-script/test/upgrade-test-lib.dar",
      testFilesDirPath = "daml-script/test/daml/upgrades/stable",
      numParticipants = 5,
      testCaseFilter = _.name == "SubViews",
    )

class UpgradesITSmallDev
    extends UpgradesIT(
      runCantonInDevMode = true,
      languageVersion = LanguageVersion.Major.V2.dev,
      upgradeTestLibDarPath = "daml-script/test/upgrade-test-lib-dev.dar",
      testFilesDirPath = "daml-script/test/daml/upgrades/dev",
      numParticipants = 2,
      testCaseFilter = _.name != "SubViews",
    )
