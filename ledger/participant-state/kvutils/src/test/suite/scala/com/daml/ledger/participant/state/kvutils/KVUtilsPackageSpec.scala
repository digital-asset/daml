// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry
import org.scalatest.{Matchers, WordSpec}

class KVUtilsPackageSpec extends WordSpec with Matchers {
  import KVTest._
  import TestHelpers._

  "packages" should {
    "be able to submit package" in KVTest.runTest {
      for {
        // NOTE(JM): 'runTest' always uploads 'simpleArchive' by default.
        logEntry <- submitArchives(emptyArchive).map(_._2)
        archiveState <- getDamlState(Conversions.packageStateKey(emptyPackageId))

        // Submit again and verify that the uploaded archive didn't appear again.
        logEntry2 <- submitArchives(emptyArchive, simpleArchive).map(_._2)

      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY
        logEntry.getPackageUploadEntry.getArchivesCount shouldEqual 1

        archiveState.isDefined shouldBe true
        archiveState.get.hasArchive shouldBe true
        archiveState.get.getArchive shouldEqual emptyArchive

        logEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY
        logEntry2.getPackageUploadEntry.getArchivesCount shouldEqual 0
      }
    }

    "reject invalid packages" in KVTest.runTest {
      for {
        logEntry <- submitArchives(badArchive).map(_._2)
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_REJECTION_ENTRY
      }

    }

  }
}
