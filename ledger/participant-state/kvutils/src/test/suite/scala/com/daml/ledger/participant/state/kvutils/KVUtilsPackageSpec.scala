// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.lf.data.Ref
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class KVUtilsPackageSpec extends AnyWordSpec with Matchers with BazelRunfiles {

  import KVTest._
  import TestHelpers._

  private val darReader = DarReader { case (_, is) => Try(DamlLf.Archive.parseFrom(is)) }

  private def darFile = new File(rlocation("ledger/test-common/model-tests.dar"))

  private val testStablePackages = darReader.readArchiveFromFile(darFile).get

  private val simplePackage = new SimplePackage("Party")
  private val simpleArchive = simplePackage.archive

  "packages" should {
    "be able to submit simple package" in KVTest.runTest {
      for {
        // NOTE(JM): 'runTest' always uploads 'simpleArchive' by default.
        logEntry <- submitArchives("simple-archive-submission-1", simpleArchive).map(_._2)
        archiveState <- getDamlState(
          Conversions.packageStateKey(Ref.PackageId.assertFromString(simplePackage.archiveHash)))

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

    "be able to submit model-test.dar" in KVTest.runTest {
      for {
        logEntry <- submitArchives("model-test-submission", testStablePackages.all: _*).map(_._2)
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY
        logEntry.getPackageUploadEntry.getArchivesCount shouldEqual testStablePackages.all.length
      }
    }

    "be able to pre-execute model-test.dar" in KVTest.runTest {
      for {
        preExecutionResult <- preExecuteArchives(
          "model-test-submission",
          testStablePackages.all: _*)
          .map(_._2)
        actualLogEntry = preExecutionResult.successfulLogEntry
      } yield {
        actualLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY
        actualLogEntry.getPackageUploadEntry.getArchivesCount shouldEqual testStablePackages.all.length
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

    "update metrics" in KVTest.runTest {
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
