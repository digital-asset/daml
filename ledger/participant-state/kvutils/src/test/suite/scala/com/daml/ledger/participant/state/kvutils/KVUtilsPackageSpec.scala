// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.File

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry
import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf_dev.DamlLf
import org.scalatest.{Matchers, WordSpec}

import scala.util.Try

class KVUtilsPackageSpec extends WordSpec with Matchers with BazelRunfiles {
  import KVTest._
  import TestHelpers._

  val darReader = DarReader { case (_, is) => Try(DamlLf.Archive.parseFrom(is)) }

  val testStablePackages =
    darReader.readArchiveFromFile(new File(rlocation("ledger/test-common/Test-stable.dar"))).get

  "packages" should {
    "be able to submit simple package" in KVTest.runTest {
      for {
        // NOTE(JM): 'runTest' always uploads 'simpleArchive' by default.
        logEntry <- submitArchives("simple-archive-submission-1", simpleArchive).map(_._2)
        archiveState <- getDamlState(Conversions.packageStateKey(simplePackageId))

        // Submit again and verify that the uploaded archive didn't appear again.
        logEntry2 <- submitArchives("simple-archive-submission-2", simpleArchive)
          .map(_._2)

      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY
        logEntry.getPackageUploadEntry.getArchivesCount shouldEqual 1

        archiveState.isDefined shouldBe true
        archiveState.get.hasArchive shouldBe true
        archiveState.get.getArchive shouldEqual simpleArchive

        logEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY
        logEntry2.getPackageUploadEntry.getArchivesCount shouldEqual 0
      }
    }

    "be able to submit Test-stable.dar" in KVTest.runTest {
      for {
        // NOTE(JM): 'runTest' always uploads 'simpleArchive' by default.
        logEntry <- submitArchives("test-stable-submission", testStablePackages.all: _*).map(_._2)
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY
        logEntry.getPackageUploadEntry.getArchivesCount shouldEqual 3
      }
    }

    "reject invalid packages" in KVTest.runTest {
      for {
        logEntry <- submitArchives("bad-archive-submission", badArchive).map(_._2)
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_REJECTION_ENTRY
      }
    }

  }
}
