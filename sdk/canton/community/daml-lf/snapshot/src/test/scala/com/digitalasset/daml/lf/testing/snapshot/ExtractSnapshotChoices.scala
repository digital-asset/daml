// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.data.Ref
import org.scalatest.{Assertion, BeforeAndAfterAll}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, FileSystems, Path}

class ExtractSnapshotChoices extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private var snapshotBaseDir: Path = _
  private var darFile: Path = _
  private var scriptEntryPoint: Ref.QualifiedName = _

  override protected def beforeAll(): Unit = {
    assume(
      Seq("DAR_FILE", "SCRIPT_NAME", "SNAPSHOT_DIR")
        .forall(envVar => sys.env.contains(envVar)),
      "The environment variables DAR_FILE, SCRIPT_NAME and SNAPSHOT_DIR all need to be set"
    )

    snapshotBaseDir = Path.of(sys.env("SNAPSHOT_DIR"))
    darFile = Path.of(sys.env("DAR_FILE"))
    scriptEntryPoint = Ref.QualifiedName.assertFromString(sys.env("SCRIPT_NAME"))
  }

  lazy val snapshotDir = snapshotBaseDir.resolve(s"${darFile.getFileName}/${scriptEntryPoint.name}")
  lazy val participantId = Ref.ParticipantId.assertFromString("participant1")
  lazy val snapshotFileMatcher =
    FileSystems
      .getDefault()
      .getPathMatcher(s"glob:$snapshotDir/snapshot-$participantId*.bin")

  private def runWhenEnvVarSet(name: String)(testFun: => Assertion): Unit = {
    if (sys.env.get("STANDALONE").nonEmpty) {
      name.in(testFun)
    } else {
      name.ignore(testFun)
    }
  }

  runWhenEnvVarSet("Extract choices from snapshot data") {
    println(s"Using snapshot data ${darFile.getFileName}/${scriptEntryPoint.name}")

    val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
    snapshotFiles.size() should be(1)

    val snapshotFile = snapshotFiles.get(0)
    val choiceNames = TransactionSnapshot.getAllTopLevelChoiceNames(snapshotFile)

    noException should be thrownBy {
      Files.writeString(
        snapshotDir.resolve(s"${snapshotFile.getFileName}.choices"),
        choiceNames.mkString("\n"),
      )
    }
  }
}
