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
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.test.TestingAdminLedgerClient
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.speedy.Speedy.Machine.{newTraceLog, newWarningLog}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.{Path, Paths}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class UpgradesIT extends AsyncWordSpec with AbstractScriptTest with Inside with Matchers {

  final override protected lazy val nParticipants = 2
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

  final override protected lazy val devMode = true
  final override protected val disableUpgradeValidation = true

  // TODO(https://github.com/digital-asset/daml/issues/18457): split the test into one with contract
  //  keys and one without, and revert to the default version. Here and below, in the loaded dars.
  val languageVersion: LanguageVersion = LanguageVersion.v2_dev
  override val majorLanguageVersion: LanguageMajorVersion = languageVersion.major

  override protected lazy val darFiles = List()

  lazy val upgradeTestLibDar: Path = rlocation(Paths.get("daml-script/test/upgrade-test-lib.dar"))

  val testUtil = new UpgradeTestUtil(upgradeTestLibDar)

  val testFileDir: Path = rlocation(Paths.get("daml-script/test/daml/upgrades/"))
  val testCases: Seq[TestCase] = UpgradeTestUtil.getTestCases(testFileDir)

  private def traverseSequential[A, B](elems: Seq[A])(f: A => Future[B]): Future[Seq[B]] =
    elems.foldLeft(Future.successful(Seq.empty[B])) { case (comp, elem) =>
      comp.flatMap { elems => f(elem).map(elems :+ _) }
    }

  // Maybe provide our own tracer that doesn't tag, it makes the logs very long
  "Multi-participant Daml Script Upgrades" should {
    testCases.foreach { testCase =>
      (testCase.name + " on IDE Ledger") in {
        for {
          // Build dars
          (testDarPath, _) <- testUtil.buildTestCaseDarMemoized(testCase)

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
          (testDarPath, deps) <- testUtil.buildTestCaseDarMemoized(testCase)

          // Connection
          clients <- scriptClients(provideAdminPorts = true)
          adminClients = ledgerPorts.map { portInfo =>
            (
              portInfo.ledgerPort.value,
              TestingAdminLedgerClient.singleHost(
                "localhost",
                portInfo.adminPort.value,
                None,
                LedgerClientChannelConfiguration.InsecureDefaults,
              ),
            )
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
      client: TestingAdminLedgerClient,
      deps: Seq[Dar],
  ): Future[Unit] = {
    client
      .listVettedPackages()
      .map(_.foreach { case (participantId, packageIds) =>
        deps.foreach { dep =>
          if (!packageIds.contains(dep.mainPackageId))
            throw new Exception(
              s"Couldn't find package ${dep.versionedName} on participant $participantId"
            )
        }
      })
  }
}
