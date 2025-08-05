// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.daml.integrationtest.CantonConfig
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.engine.script.test.AbstractScriptTest
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, FileSystems, Path}

class GenerateSpliceSnapshotsTest0 extends GenerateSpliceSnapshots(
  LanguageMajorVersion.V2,
  snapshotBaseDir = Path.of("/tmp/canton/snapshot"),
  testParam = GenerateSpliceSnapshots.testParams(0),
)

class GenerateSpliceSnapshotsTest1 extends GenerateSpliceSnapshots(
  LanguageMajorVersion.V2,
  snapshotBaseDir = Path.of("/tmp/canton/snapshot"),
  testParam = GenerateSpliceSnapshots.testParams(1),
)

class GenerateSpliceSnapshotsTest2 extends GenerateSpliceSnapshots(
  LanguageMajorVersion.V2,
  snapshotBaseDir = Path.of("/tmp/canton/snapshot"),
  testParam = GenerateSpliceSnapshots.testParams(2),
)

class GenerateSpliceSnapshotsTest3 extends GenerateSpliceSnapshots(
  LanguageMajorVersion.V2,
  snapshotBaseDir = Path.of("/tmp/canton/snapshot"),
  testParam = GenerateSpliceSnapshots.testParams(3),
)

class GenerateSpliceSnapshots(override val majorLanguageVersion: LanguageMajorVersion, snapshotBaseDir: Path, testParam: GenerateSpliceSnapshots.TestParam)
  extends AsyncWordSpec
    with AbstractScriptTest
    with Matchers {

  val GenerateSpliceSnapshots.TestParam(darFile, scriptEntryPoint, choiceEntryPoint) = testParam
  val snapshotDir = snapshotBaseDir.resolve(s"${darFile.getFileName}/${scriptEntryPoint.name}")
  val participantId = Ref.ParticipantId.assertFromString("participant0")
  val snapshotFileMatcher =
    FileSystems
      .getDefault()
      .getPathMatcher(s"glob:$snapshotDir/snapshot-$participantId*.bin")

  override protected def cantonConfig(): CantonConfig =
    super.cantonConfig().copy(snapshotDir = Some(snapshotDir.toFile.getAbsolutePath))

  override protected lazy val timeMode = ScriptTimeMode.Static

  override lazy val darPath = darFile

  s"Generate snapshot data for ${darFile.getFileName}/${scriptEntryPoint.name}" in {
    val existingSnapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList

    if (existingSnapshotFiles.size() >= 1) {
      val snapshotFile = existingSnapshotFiles.get(0)

      validateSnapshotFile(darFile, snapshotFile, choiceEntryPoint)
    } else {
      for {
        clients <- scriptClients()
        _ <- run(clients, scriptEntryPoint, dar = dar)
      } yield {
        val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
        snapshotFiles.size() should be(1)

        val snapshotFile = snapshotFiles.get(0)

        validateSnapshotFile(darFile, snapshotFile, choiceEntryPoint)
      }
    }
  }

  private def validateSnapshotFile(darFile: Path, snapshotFile: Path, choiceEntryPoint: String): Assertion = {
    Files.exists(snapshotFile) should be(true)
    Files.size(snapshotFile) should be > 0L

    // Replay and validate the snapshot file
    val benchmark = new ReplayBenchmark
    benchmark.darFile = darFile.toFile.getAbsolutePath
    benchmark.choiceName = choiceEntryPoint
    benchmark.entriesFile = snapshotFile.toFile.getAbsolutePath

    noException should be thrownBy benchmark.init()
  }
}

object GenerateSpliceSnapshots {
  final case class TestParam(darFile: Path, scriptEntryPoint: Ref.QualifiedName, choiceEntryPoint: String)

  private val spliceDsoGovernanceTestDar = Path.of("/Users/carlpulley/Projects/splice/daml/splice-dso-governance-test/.daml/dist/splice-dso-governance-test-current.dar")

  val testParams = Seq(
    TestParam(
      darFile = spliceDsoGovernanceTestDar,
      scriptEntryPoint = Ref.QualifiedName.assertFromString("Splice.Scripts.TestDecentralizedAutomation:testDsoDelegateElection"),
      choiceEntryPoint = "Splice.DsoRules:DsoRules:DsoRules_RequestElection",
    ),
    TestParam(
      darFile = spliceDsoGovernanceTestDar,
      scriptEntryPoint = Ref.QualifiedName.assertFromString("Splice.Scripts.TestDecentralizedAutomation:testDsoDelegateElection"),
      choiceEntryPoint = "Splice.DsoRules:DsoRules:DsoRules_ElectDsoDelegate",
    ),
    TestParam(
      darFile = spliceDsoGovernanceTestDar,
      scriptEntryPoint = Ref.QualifiedName.assertFromString("Splice.Scripts.TestDecentralizedAutomation:testUnclaimedRewardsMerging"),
      choiceEntryPoint = "Splice.DsoRules:DsoRules:DsoRules_MergeUnclaimedRewards",
    ),
    TestParam(
      darFile = spliceDsoGovernanceTestDar,
      scriptEntryPoint = Ref.QualifiedName.assertFromString("Splice.Scripts.TestDecentralizedAutomation:testConfirmationExpiry"),
      choiceEntryPoint = "Splice.DsoRules:DsoRules:DsoRules_ExpireStaleConfirmation",
    ),
  )
}