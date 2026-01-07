// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.integrationtest.CantonConfig
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.engine.script.test.AbstractScriptTest
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.value.ContractIdVersion
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{FileSystems, Files, Path}

// TODO(#23971) This will likely break when Canton starts to produce V2 contract IDs.
// Ping Andreas Lochbihler on the #team-daml-language slack channel when this happens.
class ReplayBenchmarkITV2_V1
    extends ReplayBenchmarkIT(LanguageVersion.Major.V2, ContractIdVersion.V1)

class ReplayBenchmarkIT(
    override val majorLanguageVersion: LanguageVersion.Major,
    contractIdVersion: ContractIdVersion,
) extends AsyncWordSpec
    with AbstractScriptTest
    with Matchers
    with BeforeAndAfterEach {

  val participantId = Ref.ParticipantId.assertFromString("participant0")
  val snapshotDir = Files.createTempDirectory("ReplayBenchmarkTest")
  val snapshotFileMatcher =
    FileSystems
      .getDefault()
      .getPathMatcher(s"glob:$snapshotDir/snapshot-$participantId*.bin")

  override protected def cantonConfig(): CantonConfig =
    super.cantonConfig().copy(snapshotDir = Some(snapshotDir.toFile.getAbsolutePath))

  override lazy val darPath: Path =
    Path.of(rlocation(s"daml-lf/snapshot/ReplayBenchmark.dar"))
  override protected lazy val timeMode = ScriptTimeMode.WallClock

  override def afterEach(): Unit = {
    Files.newDirectoryStream(snapshotDir).forEach(Files.delete)
  }

  "Ledger submission" should {
    "with a create" should {
      "generate a snapshot file" in {
        for {
          clients <- scriptClients()
          _ <- run(
            clients,
            Ref.QualifiedName.assertFromString("GenerateSnapshot:createOnly"),
            dar = dar,
          )
        } yield {
          val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
          snapshotFiles.size() should be(1)

          val snapshotFile = snapshotFiles.get(0)
          Files.exists(snapshotFile) should be(true)
          Files.size(snapshotFile) should be > 0L

          // Replay and attempt to validate the snapshot file
          val benchmark = new ReplayBenchmark
          benchmark.darFile = darPath.toFile.getAbsolutePath
          benchmark.choiceName = "ReplayBenchmark:T:Add"
          benchmark.entriesFile = snapshotFile.toFile.getAbsolutePath
          benchmark.contractIdVersion = contractIdVersion.toString

          val exn = intercept[RuntimeException] {
            benchmark.init()
          }
          exn.getMessage should be("choice ReplayBenchmark:T:Add not found")
        }
      }
    }

    "with a create and exercise" should {
      "generate a replayable snapshot file when contract is global" in {
        for {
          clients <- scriptClients()
          _ <- run(
            clients,
            Ref.QualifiedName.assertFromString("GenerateSnapshot:globalCreateAndExercise"),
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
          benchmark.contractIdVersion = contractIdVersion.toString

          noException should be thrownBy benchmark.init()
        }
      }

      "generate a replayable snapshot file when contract is local" in {
        for {
          clients <- scriptClients()
          _ <- run(
            clients,
            Ref.QualifiedName.assertFromString("GenerateSnapshot:localCreateAndExercise"),
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
          benchmark.contractIdVersion = contractIdVersion.toString

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
            Ref.QualifiedName.assertFromString("GenerateSnapshot:createExerciseAndExplode"),
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
          benchmark.contractIdVersion = contractIdVersion.toString

          noException should be thrownBy benchmark.init()
        }
      }
    }
  }
}
