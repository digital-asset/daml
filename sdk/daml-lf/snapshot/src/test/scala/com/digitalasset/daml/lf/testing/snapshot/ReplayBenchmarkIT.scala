// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.integrationtest.CantonFixture
import com.daml.ledger.javaapi.data.CreateAndExerciseCommand
import com.digitalasset.daml.lf.archive.UniversalArchiveDecoder
import com.digitalasset.daml.lf.language.LanguageMajorVersion

import java.io.File
import java.nio.file.{Files, Path}

class ReplayBenchmarkITV2 extends ReplayBenchmarkIT(LanguageMajorVersion.V2)

class ReplayBenchmarkIT(majorLanguageVersion: LanguageMajorVersion)
  extends AnyWordSpec
    with CantonFixture
    with Matchers {

  private def getMainPkgIdAndDarPath(resource: String): (Ref.PackageId, Path) = {
    val darFile = new File(rlocation(resource))
    val packages = UniversalArchiveDecoder.assertReadFile(darFile)
    val (mainPkgId, _) = packages.main

    (mainPkgId, darFile.toPath)
  }

  val participantId = Ref.ParticipantId.assertFromString("participant")
  val snapshotDir = Files.createTempDirectory("ReplayBenchmarkTest")
  val snapshotFile = snapshotDir.resolve(s"snapshot-$participantId.bin")
  val (pkgId, darFile) = getMainPkgIdAndDarPath(
    s"daml-lf/tests/ReplayBenchmark-v${majorLanguageVersion.pretty}.dar"
  )

  // FIXME: spin Canton up with participantId, snapshotDir defined and nonStandardConfig set

  "Ledger submission" should {
    "generate a replayable snapshot file" in withClient { client =>
      // FIXME: run GenerateSnapshot.daml#generateSnapshot and generate a snapshot file

      Files.exists(snapshotFile) should be(true)
      Files.size(snapshotFile) should be > 0L

      // Replay and validate the snapshot file
      val benchmark = new ReplayBenchmark
      benchmark.darFile = darFile.toFile.getAbsolutePath
      benchmark.choiceName = "ReplayBenchmark:T:Add"
      benchmark.entriesFile = snapshotFile.toFile.getAbsolutePath

      noException should be thrownBy benchmark.init()
    }

    "survive Daml execeptions" in {
      // FIXME: run GenerateSnapshot.daml#explode

      Files.exists(snapshotFile) should be(true)
      Files.size(snapshotFile) should be > 0L

      // Replay and validate the existing snapshot file
      val benchmark = new ReplayBenchmark
      benchmark.darFile = darFile.toFile.getAbsolutePath
      benchmark.choiceName = "ReplayBenchmark:T:Add"
      benchmark.entriesFile = snapshotFile.toFile.getAbsolutePath

      noException should be thrownBy benchmark.init()
    }
  }

}
