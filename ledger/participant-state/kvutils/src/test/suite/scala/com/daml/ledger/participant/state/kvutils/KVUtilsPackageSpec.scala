// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.File

import com.daml.bazeltools.BazelRunfiles
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlPackageUploadRejectionEntry
}
import com.daml.lf.archive.DarReader
import org.scalatest.{Matchers, WordSpec}

import scala.util.Try

class KVUtilsPackageSpec extends WordSpec with Matchers with BazelRunfiles {

  import KVTest._
  import TestHelpers._

  private val darReader = DarReader { case (_, is) => Try(DamlLf.Archive.parseFrom(is)) }

  private val testStablePackages =
    darReader.readArchiveFromFile(new File(rlocation("ledger/test-common/Test-stable.dar"))).get

  private val simpleArchive = archiveWithContractData("Party")

  "packages" should {
    "be able to submit simple package" in KVTest.runTest {
      for {
        // NOTE(JM): 'runTest' always uploads 'simpleArchive' by default.
        logEntry <- submitArchives("simple-archive-submission-1", simpleArchive).map(_._2)
        archiveState <- getDamlState(
          Conversions.packageStateKey(packageIdWithContractData("Party")))

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
        logEntry.getPackageUploadEntry.getArchivesCount shouldEqual testStablePackages.all.length
      }
    }

    "reject invalid packages" in KVTest.runTest {
      for {
        logEntry <- submitArchives("bad-archive-submission", badArchive).map(_._2)
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_REJECTION_ENTRY
      }
    }

    "reject duplicate" in KVTest.runTest {
      for {
        logEntry0 <- submitArchives("simple-archive-submission-1", simpleArchive).map(_._2)
        logEntry1 <- submitArchives("simple-archive-submission-1", simpleArchive).map(_._2)
      } yield {
        logEntry0.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY
        logEntry1.getPayloadCase shouldEqual
          DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_REJECTION_ENTRY
        logEntry1.getPackageUploadRejectionEntry.getReasonCase shouldEqual
          DamlPackageUploadRejectionEntry.ReasonCase.DUPLICATE_SUBMISSION

      }
    }

    "metrics get updated" in KVTest.runTestWithSimplePackage() {
      for {
        //Submit archive twice to force one acceptance and one rejection on duplicate
        _ <- submitArchives("simple-archive-submission-1", simpleArchive).map(_._2)
        _ <- submitArchives("simple-archive-submission-1", simpleArchive).map(_._2)
      } yield {
        // Check that we're updating the metrics (assuming this test at least has been run)
        metrics.daml.kvutils.committer.packageUpload.accepts.getCount should be >= 1L
        metrics.daml.kvutils.committer.packageUpload.rejections.getCount should be >= 1L
        metrics.daml.kvutils.committer.runTimer("package_upload").getCount should be >= 1L
      }
    }

  }
}
