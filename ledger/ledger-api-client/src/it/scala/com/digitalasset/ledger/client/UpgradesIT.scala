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
import org.scalatest.compatible.Assertion

import scala.io.Source
import com.daml.lf.data.Ref.PackageId

import com.daml.lf.archive.{DarReader}
import scala.util.{Success, Failure}

final class UpgradesIT extends AsyncWordSpec with Matchers with Inside with CantonFixture {
  self: Suite =>
  override lazy val devMode = true;
  override val cantonFixtureDebugMode = true;

  private def loadPackageIdAndBS(path: String): Future[(PackageId, ByteString)] = {
    val dar = DarReader.assertReadArchiveFromFile(new File(rlocation(path)))
    assert(dar != null, s"Unable to load test package resource '$path'")

    val testPackage = Future {
      val in = new FileInputStream(new File(rlocation(path)))
      assert(in != null, s"Unable to load test package resource '$path'")
      in
    }
    val bytes = testPackage.map(ByteString.readFrom)
    bytes.onComplete(_ => testPackage.map(_.close()))
    bytes.map((dar.main.pkgId, _))
  }

  private def testPackagePair(
      upgraded: String,
      upgrading: String,
      failureMessage: Option[String],
  ): Future[Assertion] = {
    for {
      client <- defaultLedgerClient()
      (testPackageV1Id, testPackageV1BS) <- loadPackageIdAndBS(upgraded)
      (testPackageV2Id, testPackageV2BS) <- loadPackageIdAndBS(upgrading)
      uploadV1Result <- client.packageManagementClient
        .uploadDarFile(testPackageV1BS)
        .transform({
          case Failure(err) => Success(Some(err));
          case Success(_) => Success(None);
        })
      uploadV2Result <- client.packageManagementClient
        .uploadDarFile(testPackageV2BS)
        .transform({
          case Failure(err) => Success(Some(err));
          case Success(_) => Success(None);
        })
    } yield {
      val cantonLog = Source.fromFile(s"$cantonTmpDir/canton.log").mkString
      cantonLog should include regex s"Package $testPackageV1Id does not upgrade anything"
      cantonLog should include regex s"Package $testPackageV2Id upgrades package id $testPackageV1Id"
      uploadV1Result match {
        case Some(err) =>
          fail(s"Uploading first package $testPackageV1Id failed with message: $err");
        case _ => {}
      }

      failureMessage match {
        // If a failure message is expected, look for it in the canton logs
        case Some(failureMessage) => {
          cantonLog should include regex s"Typechecking upgrades for $testPackageV2Id failed with following message: $failureMessage"
          uploadV2Result match {
            case None => fail(s"Uploading second package $testPackageV2Id should fail but didn't.");
            case Some(err) => {
              val msg = err.toString
              msg should include regex "INVALID_ARGUMENT: DAR_NOT_VALID_UPGRADE"
              msg should include regex "The DAR contains a package which claims to upgrade another package, but basic checks indicate the package is not a valid upgrade"
            }
          }
        }

        // If a failure is not expected, look for a success message
        case None => {
          cantonLog should include regex s"Typechecking upgrades for $testPackageV2Id succeeded."
          uploadV2Result match {
            case None => succeed;
            case Some(err) => {
              fail(
                s"Uploading second package $testPackageV2Id shouldn't fail but did, with message: $err"
              );
            }
          }
        }
      }
    }
  }

  "Upload-time Upgradeability Checks" should {
    "report no upgrade errors for valid upgrade" in {
      testPackagePair(
        "test-common/upgrades-ValidUpgrade-v1.dar",
        "test-common/upgrades-ValidUpgrade-v2.dar",
        None,
      )
    }
    "report error when module is missing in upgrading package" in {
      testPackagePair(
        "test-common/upgrades-MissingModule-v1.dar",
        "test-common/upgrades-MissingModule-v2.dar",
        Some("MissingModule"),
      )
    }
    "report error when template is missing in upgrading package" in {
      testPackagePair(
        "test-common/upgrades-MissingTemplate-v1.dar",
        "test-common/upgrades-MissingTemplate-v2.dar",
        Some("MissingTemplate"),
      )
    }
    "report error when datatype is missing in upgrading package" in {
      testPackagePair(
        "test-common/upgrades-MissingDataCon-v1.dar",
        "test-common/upgrades-MissingDataCon-v2.dar",
        Some("MissingDataCon"),
      )
    }
    "report error when choice is missing in upgrading package" in {
      testPackagePair(
        "test-common/upgrades-MissingChoice-v1.dar",
        "test-common/upgrades-MissingChoice-v2.dar",
        Some("MissingChoice"),
      )
    }
    "report error when key type changes" in {
      testPackagePair(
        "test-common/upgrades-TemplateChangedKeyType-v1.dar",
        "test-common/upgrades-TemplateChangedKeyType-v2.dar",
        Some("TemplateChangedKeyType"),
      )
    }
    "report error when record fields change" in {
      testPackagePair(
        "test-common/upgrades-RecordFieldsNewNonOptional-v1.dar",
        "test-common/upgrades-RecordFieldsNewNonOptional-v2.dar",
        Some("RecordFieldsNewNonOptional"),
      )
    }
  }
}
