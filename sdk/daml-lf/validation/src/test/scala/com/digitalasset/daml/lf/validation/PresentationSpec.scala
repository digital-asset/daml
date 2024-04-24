// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.integrationtest.CantonFixture
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import com.google.protobuf.ByteString
import com.daml.bazeltools.BazelRunfiles

import java.io.File
import java.io.FileInputStream
import org.scalatest.compatible.Assertion

import com.daml.lf.data.Ref.PackageId
import com.daml.lf.archive.DarReader

class UpgradesSpecAdminAPIWithValidation
    extends UpgradesSpecAdminAPI("Admin API with validation")
    with LongTests

abstract class UpgradesSpecAdminAPI(override val suffix: String) extends UpgradesSpec(suffix) {
  override def uploadPackage(
      path: String
  ): Future[(PackageId, Unit)] = {
    val client = AdminLedgerClient.singleHost(
      ledgerPorts(0).adminPort,
      config,
    )
    for {
      (testPackageId, testPackageBS) <- loadPackageIdAndBS(path)
      uploadResult <- client
        .uploadDar(testPackageBS, path)
    } yield (testPackageId, uploadResult)
  }
}

trait LongTests { this: UpgradesSpec =>
  s"Upload-time Upgradeability Checks ($suffix)" should {
    s"report error when module is missing in upgrading package ($suffix)" in {
      for {
        _ <- uploadPackage("test-common/upgrades-MissingModule-v1.dar")
        _ <- uploadPackage("test-common/upgrades-MissingModule-v2.dar")
      } yield succeed
    }
    /*
    s"Succeeds when new field with optional type is added to template ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v1.dar",
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    */
  }
}

abstract class UpgradesSpec(val suffix: String)
    extends AsyncWordSpec
    with Matchers
    with Inside
    with CantonFixture {
  override lazy val devMode = true;
  override val cantonFixtureDebugMode = CantonFixtureDebugRemoveTmpFiles;

  protected def loadPackageIdAndBS(path: String): Future[(PackageId, ByteString)] = {
    val dar = DarReader.assertReadArchiveFromFile(new File(BazelRunfiles.rlocation(path)))
    assert(dar != null, s"Unable to load test package resource '$path'")

    val testPackage = Future {
      val in = new FileInputStream(new File(BazelRunfiles.rlocation(path)))
      assert(in != null, s"Unable to load test package resource '$path'")
      in
    }
    val bytes = testPackage.map(ByteString.readFrom)
    bytes.onComplete(_ => testPackage.map(_.close()))
    bytes.map((dar.main.pkgId, _))
  }

  def uploadPackage(
      path: String
  ): Future[(PackageId, Unit)]

  def assertPackageUpgradeCheck(failureMessage: Option[String])(
      v1: (PackageId, Option[Throwable]),
      v2: (PackageId, Option[Throwable]),
  )(cantonLogSrc: String): Assertion =
    assertPackageUpgradeCheckGeneral(failureMessage)(v1, v2, true)(cantonLogSrc)

  def assertPackageUpgradeCheckGeneral(failureMessage: Option[String])(
      v1: (PackageId, Option[Throwable]),
      v2: (PackageId, Option[Throwable]),
      validateV1Checked: Boolean = true,
  )(cantonLogSrc: String): Assertion = {
    val (testPackageV1Id, uploadV1Result) = v1
    val (testPackageV2Id, uploadV2Result) = v2
    if (disableUpgradeValidation) {
      cantonLogSrc should include(s"Skipping upgrade validation for package $testPackageV1Id")
      cantonLogSrc should include(s"Skipping upgrade validation for package $testPackageV2Id")
    } else {
      uploadV1Result match {
        case Some(err) if validateV1Checked =>
          fail(s"Uploading first package $testPackageV1Id failed with message: $err");
        case _ => {}
      }

      if (validateV1Checked) {
        cantonLogSrc should include(
          s"Package $testPackageV2Id claims to upgrade package id $testPackageV1Id"
        )
      }

      failureMessage match {
        // If a failure message is expected, look for it in the canton logs
        case Some(additionalInfo) => {
          cantonLogSrc should include(
            s"The DAR contains a package which claims to upgrade another package, but basic checks indicate the package is not a valid upgrade err-context:{additionalInfo=$additionalInfo"
          )
          uploadV2Result match {
            case None =>
              fail(s"Uploading second package $testPackageV2Id should fail but didn't.");
            case Some(err) => {
              val msg = err.toString
              msg should include("INVALID_ARGUMENT: DAR_NOT_VALID_UPGRADE")
              msg should include(
                "The DAR contains a package which claims to upgrade another package, but basic checks indicate the package is not a valid upgrade"
              )
            }
          }
        }

        // If a failure is not expected, look for a success message
        case None => {
          cantonLogSrc should include(s"Typechecking upgrades for $testPackageV2Id succeeded.")
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
}
