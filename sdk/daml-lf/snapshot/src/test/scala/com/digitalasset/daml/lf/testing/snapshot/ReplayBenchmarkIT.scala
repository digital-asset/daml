// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.integrationtest.CantonConfig
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.engine.script.test.AbstractScriptTest
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, FileSystems, Path}

class ReplayBenchmarkITV2 extends ReplayBenchmarkIT(LanguageMajorVersion.V2)

class ReplayBenchmarkIT(override val majorLanguageVersion: LanguageMajorVersion)
    extends AsyncWordSpec
    with AbstractScriptTest
    with Matchers
    with BeforeAndAfterEach {

  val participantId = Ref.ParticipantId.assertFromString("participant0")
  val snapshotDir = Files.createTempDirectory("ReplayBenchmarkTest")
  val snapshotFileMatcher =
    FileSystems.getDefault().getPathMatcher(s"glob:$snapshotDir/snapshot-$participantId*.bin")

  override protected val cantonFixtureDebugMode = CantonFixtureDebugKeepTmpFiles

  override protected def cantonConfig(): CantonConfig =
    super.cantonConfig().copy(snapshotDir = Some(snapshotDir.toFile.getAbsolutePath), debug = true)

  override lazy val darPath: Path =
    Path.of(rlocation(s"daml-lf/snapshot/ReplayBenchmark-v${majorLanguageVersion.pretty}.dar"))
  override protected lazy val timeMode = ScriptTimeMode.WallClock

  override def afterEach(): Unit = {
    Files.newDirectoryStream(snapshotDir).forEach(Files.delete)
  }

  "Ledger submission" should {
    "on a happy path" should {
      "generate a replayable snapshot file" in {
        for {
          clients <- scriptClients()
          _ <- run(
            clients,
            Ref.QualifiedName.assertFromString("GenerateSnapshot:generateSnapshot"),
            dar = dar,
          )
        } yield {
          val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
          snapshotFiles.size() should be(1)

          val snapshotFile = snapshotFiles.get(0)
          Files.exists(snapshotFile) should be(true)
          Files.size(snapshotFile) should be > 0L

          // Replay and validate the snapshot file
          val benchmark = new ReplayBenchmark
          benchmark.darFile = darPath.toFile.getAbsolutePath
          benchmark.choiceName = "ReplayBenchmark:T:Add"
          benchmark.entriesFile = snapshotFile.toFile.getAbsolutePath

          noException should be thrownBy benchmark.init()
        }
      }
    }

    "with Daml exceptions" should {
      "not generate a snapshot file" in {
        for {
          clients <- scriptClients()
          _ <- run(
            clients,
            Ref.QualifiedName.assertFromString("GenerateSnapshot:explode"),
            dar = dar,
          )
        } yield {
          val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
          snapshotFiles.size() should be(0)
        }
      }

      "not update or corrupt a snapshot file" in {
        for {
          clients <- scriptClients()
          _ <- run(
            clients,
            Ref.QualifiedName.assertFromString("GenerateSnapshot:generateAndExplode"),
            dar = dar,
          )
        } yield {
          val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
          snapshotFiles.size() should be(1)

          val snapshotFile = snapshotFiles.get(0)
          Files.exists(snapshotFile) should be(true)
          Files.size(snapshotFile) should be > 0L

          // Replay and validate the snapshot file
          val benchmark = new ReplayBenchmark
          benchmark.darFile = darPath.toFile.getAbsolutePath
          benchmark.choiceName = "ReplayBenchmark:T:Add"
          benchmark.entriesFile = snapshotFile.toFile.getAbsolutePath

          noException should be thrownBy benchmark.init()
        }
      }
    }
  }

}
