// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.integrationtest.CantonFixture
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{Future}
import com.google.protobuf.ByteString
import com.daml.bazeltools.BazelRunfiles
import java.io.File
import java.io.FileInputStream
import org.scalatest.compatible.Assertion

import scala.io.Source
import com.daml.lf.data.Ref.PackageId

import com.daml.lf.archive.{DarReader}
import scala.util.{Success, Failure}

class UpgradesSpec extends AsyncWordSpec with Matchers with Inside with CantonFixture {
  override lazy val devMode = true;
  override val cantonFixtureDebugMode = CantonFixtureDebugRemoveTmpFiles;

  private def loadPackageIdAndBS(path: String): Future[(PackageId, ByteString)] = {
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

  private def assertPackageUpgradeCheck(failureMessage: Option[String])(
      testPackageV1Id: PackageId,
      uploadV1Result: Option[Throwable],
      testPackageV2Id: PackageId,
      uploadV2Result: Option[Throwable],
  )(cantonLogSrc: String): Assertion = {
    cantonLogSrc should include(s"Package $testPackageV1Id does not upgrade anything")
    cantonLogSrc should include(
      s"Package $testPackageV2Id claims to upgrade package id $testPackageV1Id"
    )
    uploadV1Result match {
      case Some(err) =>
        fail(s"Uploading first package $testPackageV1Id failed with message: $err");
      case _ => {}
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

  private def assertDuplicatePackageUpload()(
      testPackageV1Id: PackageId,
      uploadV1Result: Option[Throwable],
      testPackageV2Id: PackageId,
      uploadV2Result: Option[Throwable],
  )(cantonLogSrc: String): Assertion = {
    cantonLogSrc should include(s"Package $testPackageV1Id does not upgrade anything")
    uploadV1Result should be(empty)
    cantonLogSrc should include(
      s"Ignoring upload of package $testPackageV2Id as it has been previously uploaded"
    )
    uploadV2Result should be(empty)
  }

  private def assertPackageUploadVersionFailure(failureMessage: String, packageVersion: String)(
      testPackageV1Id: PackageId,
      uploadV1Result: Option[Throwable],
      testPackageV2Id: PackageId,
      uploadV2Result: Option[Throwable],
  )(cantonLogSrc: String): Assertion = {
    cantonLogSrc should include(s"Package $testPackageV1Id does not upgrade anything")
    uploadV1Result match {
      case Some(err) =>
        fail(s"Uploading first package $testPackageV1Id failed with message: $err");
      case _ => {}
    }
    cantonLogSrc should include(
      s"Package $testPackageV2Id can not be uploaded - $testPackageV1Id is already uploaded and at version $packageVersion"
    )
    uploadV2Result match {
      case None =>
        fail(s"Uploading second package $testPackageV2Id should fail but didn't.");
      case Some(err) => {
        val msg = err.toString
        msg should include("INVALID_ARGUMENT: KNOWN_DAR_VERSION")
        msg should include(failureMessage)
      }
    }
  }

  private def testPackagePair(
      upgraded: String,
      upgrading: String,
      uploadAssertion: (
          PackageId,
          Option[Throwable],
          PackageId,
          Option[Throwable],
      ) => String => Assertion,
  ): Future[Assertion] = {
    if (sys.props("os.name").startsWith("Windows")) {
      Future {
        info("Tests are disabled on Windows")
        succeed
      }
    } else {
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
        val cantonLog = Source.fromFile(s"$cantonTmpDir/canton.log")
        try {
          uploadAssertion(testPackageV1Id, uploadV1Result, testPackageV2Id, uploadV2Result)(
            cantonLog.mkString
          )
        } finally {
          cantonLog.close()
        }
      }
    }
  }

  "Upload-time Upgradeability Checks" should {
    "uploading the same package multiple times succeeds" in {
      testPackagePair(
        "test-common/upgrades-ValidUpgrade-v1.dar",
        "test-common/upgrades-ValidUpgrade-v1.dar",
        assertDuplicatePackageUpload(),
      )
    }
    "uploads against the same package name must be version unique" in {
      testPackagePair(
        "test-common/upgrades-CommonVersionFailure-v1a.dar",
        "test-common/upgrades-CommonVersionFailure-v1b.dar",
        assertPackageUploadVersionFailure(
          "A DAR with the same version number has previously been uploaded.",
          "1.0.0",
        ),
      )
    }
    "report no upgrade errors for valid upgrade" in {
      testPackagePair(
        "test-common/upgrades-ValidUpgrade-v1.dar",
        "test-common/upgrades-ValidUpgrade-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    "report error when module is missing in upgrading package" in {
      testPackagePair(
        "test-common/upgrades-MissingModule-v1.dar",
        "test-common/upgrades-MissingModule-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "Module Other appears in package that is being upgraded, but does not appear in the upgrading package."
          )
        ),
      )
    }
    "report error when template is missing in upgrading package" in {
      testPackagePair(
        "test-common/upgrades-MissingTemplate-v1.dar",
        "test-common/upgrades-MissingTemplate-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "Template U appears in package that is being upgraded, but does not appear in the upgrading package."
          )
        ),
      )
    }
    "report error when datatype is missing in upgrading package" in {
      testPackagePair(
        "test-common/upgrades-MissingDataCon-v1.dar",
        "test-common/upgrades-MissingDataCon-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "Datatype U appears in package that is being upgraded, but does not appear in the upgrading package."
          )
        ),
      )
    }
    "report error when choice is missing in upgrading package" in {
      testPackagePair(
        "test-common/upgrades-MissingChoice-v1.dar",
        "test-common/upgrades-MissingChoice-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "Choice C2 appears in package that is being upgraded, but does not appear in the upgrading package."
          )
        ),
      )
    }
    "report error when key type changes" in {
      testPackagePair(
        "test-common/upgrades-TemplateChangedKeyType-v1.dar",
        "test-common/upgrades-TemplateChangedKeyType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded template T cannot change its key type.")),
      )
    }
    "report error when record fields change" in {
      testPackagePair(
        "test-common/upgrades-RecordFieldsNewNonOptional-v1.dar",
        "test-common/upgrades-RecordFieldsNewNonOptional-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "The upgraded datatype Struct has added new fields, but those fields are not Optional."
          )
        ),
      )
    }

    // Ported from DamlcUpgrades.hs
    "Fails when template changes key type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTemplateChangesKeyType-v1.dar",
        "test-common/upgrades-FailsWhenTemplateChangesKeyType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded template A cannot change its key type.")),
      )
    }
    "Fails when template removes key type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v1.dar",
        "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded template A cannot remove its key.")),
      )
    }
    "Fails when template adds key type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTemplateAddsKeyType-v1.dar",
        "test-common/upgrades-FailsWhenTemplateAddsKeyType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded template A cannot add a key.")),
      )
    }
    "Fails when new field is added to template without Optional type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v1.dar",
        "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded template A has added new fields, but those fields are not Optional.")
        ),
      )
    }
    "Fails when old field is deleted from template" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v1.dar",
        "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded template A is missing some of its original fields.")
        ),
      )
    }
    "Fails when existing field in template is changed" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v1.dar",
        "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded template A has changed the types of some of its original fields.")
        ),
      )
    }
    "Succeeds when new field with optional type is added to template" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v1.dar",
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    "Fails when new field is added to template choice without Optional type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v1.dar",
        "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "The upgraded input type of choice C on template A has added new fields, but those fields are not Optional."
          )
        ),
      )
    }
    "Fails when old field is deleted from template choice" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v1.dar",
        "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "The upgraded input type of choice C on template A is missing some of its original fields."
          )
        ),
      )
    }
    "Fails when existing field in template choice is changed" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v1.dar",
        "test-common/upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "The upgraded input type of choice C on template A has changed the types of some of its original fields."
          )
        ),
      )
    }
    "Fails when template choice changes its return type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v1.dar",
        "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded choice C cannot change its return type.")),
      )
    }
    "Succeeds when template choice returns a template which has changed" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v1.dar",
        "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    "Succeeds when template choice input argument has changed" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentHasChanged-v1.dar",
        "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentHasChanged-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    "Succeeds when new field with optional type is added to template choice" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v1.dar",
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    "Succeeds when v1 upgrades to v2 and then v3" in {
      testPackagePair(
        "test-common/upgrades-SuccessUpgradingV2ThenV3-v1.dar",
        "test-common/upgrades-SuccessUpgradingV2ThenV3-v2.dar",
        assertPackageUpgradeCheck(None),
      )
      testPackagePair(
        "test-common/upgrades-SuccessUpgradingV2ThenV3-v1.dar",
        "test-common/upgrades-SuccessUpgradingV2ThenV3-v3.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    "Succeeds when v1 upgrades to v3 and then v2" in {
      testPackagePair(
        "test-common/upgrades-SuccessUpgradingV3ThenV2-v1.dar",
        "test-common/upgrades-SuccessUpgradingV3ThenV2-v3.dar",
        assertPackageUpgradeCheck(None),
      )
      testPackagePair(
        "test-common/upgrades-SuccessUpgradingV3ThenV2-v1.dar",
        "test-common/upgrades-SuccessUpgradingV3ThenV2-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    "Fails when v1 upgrades to v2, but v3 does not upgrade v2" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenUpgradingV2ThenV3-v1.dar",
        "test-common/upgrades-FailsWhenUpgradingV2ThenV3-v2.dar",
        assertPackageUpgradeCheck(None),
      )
      testPackagePair(
        "test-common/upgrades-FailsWhenUpgradingV2ThenV3-v1.dar",
        "test-common/upgrades-FailsWhenUpgradingV2ThenV3-v3.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded template T has added new fields, but those fields are not Optional.")
        ),
      )
    }

    "Fails when v1 upgrades to v3, but v3 does not upgrade v2" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenUpgradingV3ThenV2-v1.dar",
        "test-common/upgrades-FailsWhenUpgradingV3ThenV2-v2.dar",
        assertPackageUpgradeCheck(None),
      )
      testPackagePair(
        "test-common/upgrades-FailsWhenUpgradingV3ThenV2-v1.dar",
        "test-common/upgrades-FailsWhenUpgradingV3ThenV2-v3.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded template T has added new fields, but those fields are not Optional.")
        ),
      )
    }
  }
}
