// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client

import com.daml.integrationtest.CantonFixture
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{Future}
import com.google.protobuf.ByteString
import com.daml.bazeltools.BazelRunfiles._
import java.io.File
import java.io.FileInputStream
import org.scalatest.Suite

import scala.io.Source
import com.daml.lf.data.Ref.PackageId

import com.daml.lf.archive.{DarReader}

final class UpgradesIT extends AsyncWordSpec with Matchers with Inside with CantonFixture {
  self: Suite =>
  override lazy val devMode = true;
  override val cantonFixtureDebugMode = true;

  private def loadTestPackageBS(version: Int): Future[ByteString] = {
    val path = s"test-common/upgrades-example-v$version.dar"
    val testPackage = Future {
      val in = new FileInputStream(new File(rlocation(path)))
      assert(in != null, s"Unable to load test package resource '$path'")
      in
    }
    val bytes = testPackage.map(ByteString.readFrom)
    bytes.onComplete(_ => testPackage.map(_.close()))
    bytes
  }

  private def loadTestPackageId(version: Int): PackageId = {
    val path = s"test-common/upgrades-example-v$version.dar"
    val dar = DarReader.assertReadArchiveFromFile(new File(rlocation(path)))
    assert(dar != null, s"Unable to load test package resource '$path'")
    dar.main.pkgId
  }

  "Upload-time Upgradeability Checks" should {
    "report error when record fields change" in {
      for {
        client <- defaultLedgerClient()
        testPackageV1BS <- loadTestPackageBS(1)
        testPackageV2BS <- loadTestPackageBS(2)
        _ <- client.packageManagementClient.uploadDarFile(testPackageV1BS)
        _ <- client.packageManagementClient.uploadDarFile(testPackageV2BS)
      } yield {
        val cantonLog = Source.fromFile(s"$cantonTmpDir/canton.log").mkString
        val testPackageV1Id = loadTestPackageId(1)
        val testPackageV2Id = loadTestPackageId(2)
        cantonLog should include regex s"Package $testPackageV1Id has no upgraded package"
        cantonLog should include regex s"Package $testPackageV2Id upgrades package id $testPackageV1Id"
        cantonLog should include regex s"Typechecking upgrades for $testPackageV2Id failed with following message: RecordFieldsExistingChanged"
      }
    }
  }
}
