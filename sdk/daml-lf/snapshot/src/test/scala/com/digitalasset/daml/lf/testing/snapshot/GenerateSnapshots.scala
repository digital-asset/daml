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

class GenerateSnapshotsV2 extends GenerateSnapshots(LanguageMajorVersion.V2)

/** Generate snapshot data by running Daml script code and then validate the generated snapshot data by replaying
  * it using a choice name (that the Daml script code may have exercised).
  *
  * The following environment variables provide test arguments:
  * - DAR_FILE:     defines the actual Dar file containing the script code
  * - CHOICE_NAME:  defines the choice that will be exercised during snapshot replay validation - e.g. Module:Template:Choice
  * - SCRIPT_NAME:  defines the script function that should be ran to generate transaction entries for the snapshot file - e.g. Module:Script
  * - SNAPSHOT_DIR: defines the (base) directory used for storing snapshot data. Snapshot files are saved in the file with
  *   path $SNAPSHOT_DIR/$(basename DAR_FILE)/Choice/snapshot-participant0*.bin (where Choice is the exercise choice name defined by $CHOICE_NAME)
  */
class GenerateSnapshots(override val majorLanguageVersion: LanguageMajorVersion)
    extends AsyncWordSpec
    with AbstractScriptTest
    with Matchers {

  if (
    Seq("DAR_FILE", "CHOICE_NAME", "SCRIPT_NAME", "SNAPSHOT_DIR")
      .exists(envVar => sys.env.get(envVar).isEmpty)
  ) {
    throw new AssertionError(
      "The environment variables DAR_FILE, CHOICE_NAME, SCRIPT_NAME and SNAPSHOT_DIR all need to be set"
    )
  }

  val snapshotBaseDir = Path.of(sys.env("SNAPSHOT_DIR"))
  val darFile = Path.of(sys.env("DAR_FILE"))
  val scriptEntryPoint = Ref.QualifiedName.assertFromString(sys.env("SCRIPT_NAME"))
  val choiceEntryPoint = sys.env("CHOICE_NAME")
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

  private def validateSnapshotFile(
      darFile: Path,
      snapshotFile: Path,
      choiceEntryPoint: String,
  ): Assertion = {
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
