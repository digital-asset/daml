// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.data.Ref
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, FileSystems, Path}

class ExtractSnapshotChoices
  extends AsyncWordSpec
    with Matchers {

  if (
    Seq("DAR_FILE", "SCRIPT_NAME", "SNAPSHOT_DIR")
      .exists(envVar => sys.env.get(envVar).isEmpty)
  ) {
    throw new AssertionError(
      "The environment variables DAR_FILE, SCRIPT_NAME and SNAPSHOT_DIR all need to be set"
    )
  }

  val snapshotBaseDir = Path.of(sys.env("SNAPSHOT_DIR"))
  val darFile = Path.of(sys.env("DAR_FILE"))
  val scriptEntryPoint = Ref.QualifiedName.assertFromString(sys.env("SCRIPT_NAME"))
  val snapshotDir = snapshotBaseDir.resolve(s"${darFile.getFileName}/${scriptEntryPoint.name}")
  val participantId = Ref.ParticipantId.assertFromString("participant0")
  val snapshotFileMatcher =
    FileSystems
      .getDefault()
      .getPathMatcher(s"glob:$snapshotDir/snapshot-$participantId*.bin")

  s"Extract choices from snapshot data ${darFile.getFileName}/${scriptEntryPoint.name}" in {
    val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
    snapshotFiles.size() should be(1)

    val snapshotFile = snapshotFiles.get(0)
    val choiceNames = TransactionSnapshot.getAllChoiceNames(snapshotFile)

    noException should be thrownBy {
      Files.writeString(snapshotDir.resolve(s"${snapshotFile.getFileName}.choices"), choiceNames.mkString("\n"))
    }
  }
}
