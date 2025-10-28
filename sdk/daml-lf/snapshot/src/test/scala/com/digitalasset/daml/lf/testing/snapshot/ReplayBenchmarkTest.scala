// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.value.ContractIdVersion
import com.digitalasset.daml.lf.value.Value._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.File
import java.nio.file.{Files, Path}

class ReplayBenchmarkTestV1
    extends ReplayBenchmarkTest(ContractIdVersion.V1)

class ReplayBenchmarkTest(contractIdVersion: ContractIdVersion) extends AnyWordSpec with Matchers {

  implicit val logContext: LoggingContext = LoggingContext.ForTesting

  private def getMainPkgIdAndDarPath(resource: String): (Ref.PackageId, Path) = {
    val darFile = new File(rlocation(resource))
    val packages = DarDecoder.assertReadArchiveFromFile(darFile)
    val (mainPkgId, _) = packages.main

    (mainPkgId, darFile.toPath)
  }

  val participantId = Ref.ParticipantId.assertFromString("participant")
  val snapshotDir = Files.createTempDirectory("ReplayBenchmarkTest")
  val snapshotFile = snapshotDir.resolve(s"snapshot-$participantId.bin")
  val alice = Ref.Party.assertFromString("Alice")
  val submissionSeed = crypto.Hash.hashPrivateKey("replay snapshot test")
  val (pkgId, darFile) = getMainPkgIdAndDarPath(
    s"daml-lf/snapshot/ReplayBenchmark.dar"
  )

  "Generating a snapshot" should {
    "be valid on replay" in {
      // Generate a snapshot file
      val templateId = Ref.Identifier.assertFromString(s"$pkgId:ReplayBenchmark:T")
      val cmd =
        ApiCommand.CreateAndExercise(
          templateId.toRef,
          ValueRecord(
            Some(templateId),
            ImmArray(None -> ValueParty(alice), None -> ValueInt64(42)),
          ),
          Ref.ChoiceName.assertFromString("Add"),
          ValueRecord(None, ImmArray(None -> ValueInt64(3))),
        )
      val pkgs = TransactionSnapshot.loadDar(darFile)
      val engine = TransactionSnapshot.compile(pkgs, snapshotDir = Some(snapshotDir))
      engine.submit(
        submitters = Set(alice),
        readAs = Set.empty,
        cmds = ApiCommands(ImmArray(cmd), Time.Timestamp.now(), "replay-snapshot-test"),
        participantId = participantId,
        submissionSeed = submissionSeed,
        contractIdVersion = contractIdVersion,
        prefetchKeys = Seq.empty,
      )

      Files.exists(snapshotFile) should be(true)
      Files.size(snapshotFile) should be > 0L

      // Replay and validate the snapshot file
      val benchmark = new ReplayBenchmark
      benchmark.darFile = darFile.toFile.getAbsolutePath
      benchmark.choiceName = "ReplayBenchmark:T:Add"
      benchmark.entriesFile = snapshotFile.toFile.getAbsolutePath
      benchmark.contractIdVersion = contractIdVersion.toString

      noException should be thrownBy benchmark.init()
    }
  }

}
