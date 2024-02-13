// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.integrationtest.CantonFixture
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AsyncWordSpec}
import org.scalatest.Inspectors
import org.scalatest.Assertions

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

case class DeferredCantonLogCheck(description: String, check: String => Assertion) {
  def run(cantonLogSrc: String): Assertion =
    Assertions.withClue(description)(check(cantonLogSrc))
}

final class UpgradesIT extends AsyncWordSpec with Matchers with Inside with CantonFixture {
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

  private def testPackagePair(
      testDescription: String,
      upgraded: String,
      upgrading: String,
      failureMessage: Option[String],
  ): Future[DeferredCantonLogCheck] = {
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
    } yield DeferredCantonLogCheck(
      testDescription,
      (cantonLogSrc: String) => {
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
          case Some(failureMessage) => {
            cantonLogSrc should include(
              s"The DAR contains a package which claims to upgrade another package, but basic checks indicate the package is not a valid upgrade err-context:{additionalInfo=$failureMessage"
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
      },
    )
  }

  "Upload-time Upgradeability Checks" should {
    "Upload DARs and check messages match" in {
      for {
        checks: Seq[DeferredCantonLogCheck] <- Future.sequence(
          Seq(
            testPackagePair(
              "report no upgrade errors for valid upgrade",
              "test-common/upgrades-ValidUpgrade-v1.dar",
              "test-common/upgrades-ValidUpgrade-v2.dar",
              None,
            ),
            testPackagePair(
              "report error when module is missing in upgrading package",
              "test-common/upgrades-MissingModule-v1.dar",
              "test-common/upgrades-MissingModule-v2.dar",
              Some(
                "Module Other appears in package that is being upgraded, but does not appear in the upgrading package."
              ),
            ),
            testPackagePair(
              "report no upgrade errors for valid upgrade",
              "test-common/upgrades-ValidUpgrade-v1.dar",
              "test-common/upgrades-ValidUpgrade-v2.dar",
              None,
            ),
            testPackagePair(
              "report error when module is missing in upgrading package",
              "test-common/upgrades-MissingModule-v1.dar",
              "test-common/upgrades-MissingModule-v2.dar",
              Some(
                "Module Other appears in package that is being upgraded, but does not appear in the upgrading package."
              ),
            ),
            testPackagePair(
              "report error when template is missing in upgrading package",
              "test-common/upgrades-MissingTemplate-v1.dar",
              "test-common/upgrades-MissingTemplate-v2.dar",
              Some(
                "Template U appears in package that is being upgraded, but does not appear in the upgrading package."
              ),
            ),
            testPackagePair(
              "report error when datatype is missing in upgrading package",
              "test-common/upgrades-MissingDataCon-v1.dar",
              "test-common/upgrades-MissingDataCon-v2.dar",
              Some(
                "Datatype U appears in package that is being upgraded, but does not appear in the upgrading package."
              ),
            ),
            testPackagePair(
              "report error when choice is missing in upgrading package",
              "test-common/upgrades-MissingChoice-v1.dar",
              "test-common/upgrades-MissingChoice-v2.dar",
              Some(
                "Choice C2 appears in package that is being upgraded, but does not appear in the upgrading package."
              ),
            ),
            testPackagePair(
              "report error when key type changes",
              "test-common/upgrades-TemplateChangedKeyType-v1.dar",
              "test-common/upgrades-TemplateChangedKeyType-v2.dar",
              Some("The upgraded template T cannot change its key type."),
            ),
            testPackagePair(
              "report error when record fields change",
              "test-common/upgrades-RecordFieldsNewNonOptional-v1.dar",
              "test-common/upgrades-RecordFieldsNewNonOptional-v2.dar",
              Some(
                "The upgraded datatype Struct has added new fields, but those fields are not Optional."
              ),
            ),

            // Ported from DamlcUpgrades.hs
            testPackagePair(
              "Fails when template changes key type",
              "test-common/upgrades-FailsWhenTemplateChangesKeyType-v1.dar",
              "test-common/upgrades-FailsWhenTemplateChangesKeyType-v2.dar",
              Some("The upgraded template A cannot change its key type."),
            ),
            testPackagePair(
              "Fails when template removes key type",
              "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v1.dar",
              "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v2.dar",
              Some("The upgraded template A cannot remove its key."),
            ),
            testPackagePair(
              "Fails when template adds key type",
              "test-common/upgrades-FailsWhenTemplateAddsKeyType-v1.dar",
              "test-common/upgrades-FailsWhenTemplateAddsKeyType-v2.dar",
              Some("The upgraded template A cannot add a key."),
            ),
            testPackagePair(
              "Fails when new field is added to template without Optional type",
              "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v1.dar",
              "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v2.dar",
              Some(
                "The upgraded template A has added new fields, but those fields are not Optional."
              ),
            ),
            testPackagePair(
              "Fails when old field is deleted from template",
              "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v1.dar",
              "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v2.dar",
              Some("The upgraded template A is missing some of its original fields."),
            ),
            testPackagePair(
              "Fails when existing field in template is changed",
              "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v1.dar",
              "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v2.dar",
              Some("The upgraded template A has changed the types of some of its original fields."),
            ),
            testPackagePair(
              "Succeeds when new field with optional type is added to template",
              "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v1.dar",
              "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v2.dar",
              None,
            ),
            testPackagePair(
              "Fails when new field is added to template choice without Optional type",
              "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v1.dar",
              "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v2.dar",
              Some(
                "The upgraded input type of choice C on template A has added new fields, but those fields are not Optional."
              ),
            ),
            testPackagePair(
              "Fails when old field is deleted from template choice",
              "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v1.dar",
              "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v2.dar",
              Some(
                "The upgraded input type of choice C on template A is missing some of its original fields."
              ),
            ),
            testPackagePair(
              "Fails when existing field in template choice is changed",
              "test-common/upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v1.dar",
              "test-common/upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v2.dar",
              Some(
                "The upgraded input type of choice C on template A has changed the types of some of its original fields."
              ),
            ),
            testPackagePair(
              "Fails when template choice changes its return type",
              "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v1.dar",
              "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v2.dar",
              Some("The upgraded choice C cannot change its return type."),
            ),
            testPackagePair(
              "Succeeds when template choice returns a template which has changed",
              "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v1.dar",
              "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v2.dar",
              None,
            ),
            testPackagePair(
              "Succeeds when template choice input argument has changed",
              "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentHasChanged-v1.dar",
              "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentHasChanged-v2.dar",
              None,
            ),
            testPackagePair(
              "Succeeds when new field with optional type is added to template choice",
              "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v1.dar",
              "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v2.dar",
              None,
            ),
          )
        )
      } yield {
        // Close resource, so that canton relinquishes its log file
        suiteResource.close()

        // Read log file
        val cantonLog = Source.fromFile(s"$cantonTmpDir/canton.log")
        val cantonLogSrc = cantonLog.mkString
        cantonLog.close()

        Inspectors.forAll(checks)(_.run(cantonLogSrc))
      }
    }
  }
}
