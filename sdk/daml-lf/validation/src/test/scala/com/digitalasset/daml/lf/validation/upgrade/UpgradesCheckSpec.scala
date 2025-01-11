// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation
package upgrade

import com.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.{/*Dar,*/ DarReader /*, DarWriter*/}
import com.digitalasset.daml.lf.data.Ref.PackageId
import org.scalatest.{Assertion, Inside}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.{Path, Paths}
import java.io.File
import org.scalatest.Inspectors.forEvery

import com.digitalasset.daml.lf.validation.upgrade.{StringLoggerFactory}

import com.digitalasset.daml.lf.validation.UpgradeCheckMain

final class UpgradesCheckSpec extends AsyncWordSpec with Matchers with Inside {
  protected def loadPackageId(path: Path): PackageId = {
    val dar = DarReader.assertReadArchiveFromFile(new File(path.toString))
    assert(dar != null, s"Unable to load test package resource '$path'")
    dar.main.pkgId
  }

  def testPackages(
      rawPaths: Seq[String],
      uploadAssertions: Seq[(String, String, Option[String])],
  ): Assertion = {
    val paths: Seq[Path] = rawPaths.map((x: String) => BazelRunfiles.rlocation(Paths.get(x)))

    val builder = new StringBuilder()
    val loggerFactory = StringLoggerFactory("")
    val upgradeCheck = UpgradeCheckMain(loggerFactory)
    upgradeCheck.check(paths.toArray.map(_.toString))
    for { msg <- loggerFactory.msgs } {
      (builder append msg) append '\n'
    }
    val out = builder.toString

    forEvery(uploadAssertions) { uploadAssertion =>
      checkTwo(uploadAssertion)(out)
    }
  }

  def checkTwo(assertion: (String, String, Option[String]))(
      upgradeCheckToolLogs: String
  ): Assertion = {
    val (firstIdx: String, secondIdx: String, failureMessage: Option[String]) = assertion
    val testPackageFirstId: PackageId = loadPackageId(Paths.get(firstIdx))
    val testPackageSecondId: PackageId = loadPackageId(Paths.get(secondIdx))
    val header =
      s"Error while checking two DARs:\nUpgrade checks indicate that (existing|new) package ($testPackageFirstId|$testPackageSecondId) \\(.*\\) cannot be an upgrade of (existing|new) package ($testPackageFirstId|$testPackageSecondId) \\(.*\\)"
    failureMessage match {
      case None => upgradeCheckToolLogs should not include regex(header)
      case Some(msg) => upgradeCheckToolLogs should include regex (s"$header. Reason: $msg")
    }
  }

  s"Upgradeability Checks using `daml upgrade-check` tool" should {
    "report no upgrade errors when the upgrade use a newer version of LF" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenNewerPackagesUsesANewerLFVersion-v1.dar",
          "test-common/upgrades-SucceedsWhenNewerPackagesUsesANewerLFVersion-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenNewerPackagesUsesANewerLFVersion-v1.dar",
            "test-common/upgrades-SucceedsWhenNewerPackagesUsesANewerLFVersion-v2.dar",
            None,
          )
        ),
      )
    }

    "report upgrade errors when the upgrade use a older version of LF" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenNewerPackagesUsesAnOlderLFVersion-v1.dar",
          "test-common/upgrades-FailsWhenNewerPackagesUsesAnOlderLFVersion-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenNewerPackagesUsesAnOlderLFVersion-v1.dar",
            "test-common/upgrades-FailsWhenNewerPackagesUsesAnOlderLFVersion-v2.dar",
            Some("The upgraded package uses an older LF version"),
          )
        ),
      )
    }

    "Succeeds when v2 depends on v2dep which is a valid upgrade of v1dep" in {
      testPackages(
        Seq(
          "test-common/upgrades-UploadSucceedsWhenDepsAreValidUpgrades-v1.dar",
          "test-common/upgrades-UploadSucceedsWhenDepsAreValidUpgrades-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-UploadSucceedsWhenDepsAreValidUpgradesDep-v1.dar",
            "test-common/upgrades-UploadSucceedsWhenDepsAreValidUpgradesDep-v2.dar",
            None,
          )
        ),
      )
    }

    "report upgrade errors when v2 depends on v2dep which is an invalid upgrade of v1dep" in {
      testPackages(
        Seq(
          "test-common/upgrades-UploadFailsWhenDepsAreInvalidUpgrades-v1.dar",
          "test-common/upgrades-UploadFailsWhenDepsAreInvalidUpgrades-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v1.dar",
            "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v2.dar",
            Some("The upgraded template A has changed the types of some of its original fields."),
          )
        ),
      )
    }

    "Fails when a package embeds a previous version of itself it is not a valid upgrade of" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenDepIsInvalidPreviousVersionOfSelf-v2.dar"
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenDepIsInvalidPreviousVersionOfSelf-v1.dar",
            "test-common/upgrades-FailsWhenDepIsInvalidPreviousVersionOfSelf-v2.dar",
            Some(
              "The upgraded data type T has added new fields, but those fields are not Optional."
            ),
          )
        ),
      )
    }

    "Succeeds when a package embeds a previous version of itself it is a valid upgrade of" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenDepIsValidPreviousVersionOfSelf-v2.dar"),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenDepIsValidPreviousVersionOfSelf-v1.dar",
            "test-common/upgrades-SucceedsWhenDepIsValidPreviousVersionOfSelf-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds when upgrading a dependency" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenUpgradingADependency-v1.dar",
          "test-common/upgrades-SucceedsWhenUpgradingADependency-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenUpgradingADependency-dep-v1.dar",
            "test-common/upgrades-SucceedsWhenUpgradingADependency-dep-v2.dar",
            None,
          ),
          (
            "test-common/upgrades-SucceedsWhenUpgradingADependency-v1.dar",
            "test-common/upgrades-SucceedsWhenUpgradingADependency-v2.dar",
            None,
          ),
        ),
      )
    }

    "Succeeds when upgrading a dependency of a dependency" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-v1.dar",
          "test-common/upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-dep-dep-v1.dar",
            "test-common/upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-dep-dep-v2.dar",
            None,
          ),
          (
            "test-common/upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-dep-v1.dar",
            "test-common/upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-dep-v2.dar",
            None,
          ),
          (
            "test-common/upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-v1.dar",
            "test-common/upgrades-SucceedsWhenUpgradingADependencyOfAnUpgradedDependency-v2.dar",
            None,
          ),
        ),
      )
    }

    "Fails when upgrading an erroneous dependency of a dependency" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenUpgradingAnUnupgradeableDependencyOfAnUpgradedDependency-v1.dar",
          "test-common/upgrades-FailsWhenUpgradingAnUnupgradeableDependencyOfAnUpgradedDependency-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenUpgradingAnUnupgradeableDependencyOfAnUpgradedDependency-dep-dep-v1.dar",
            "test-common/upgrades-FailsWhenUpgradingAnUnupgradeableDependencyOfAnUpgradedDependency-dep-dep-v2.dar",
            Some(
              "The upgraded data type D has added new fields, but those fields are not Optional."
            ),
          )
        ),
      )
    }

    s"Succeeds when v1 upgrades to v2 and then v3" in {
      testPackages(
        Seq(
          "test-common/upgrades-SuccessUpgradingV2ThenV3-v1.dar",
          "test-common/upgrades-SuccessUpgradingV2ThenV3-v2.dar",
          "test-common/upgrades-SuccessUpgradingV2ThenV3-v3.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SuccessUpgradingV2ThenV3-v1.dar",
            "test-common/upgrades-SuccessUpgradingV2ThenV3-v2.dar",
            None,
          ),
          (
            "test-common/upgrades-SuccessUpgradingV2ThenV3-v2.dar",
            "test-common/upgrades-SuccessUpgradingV2ThenV3-v3.dar",
            None,
          ),
        ),
      )
    }

    s"Succeeds when v1 upgrades to v3 and then v2" in {
      testPackages(
        Seq(
          "test-common/upgrades-SuccessUpgradingV3ThenV2-v1.dar",
          "test-common/upgrades-SuccessUpgradingV3ThenV2-v3.dar",
          "test-common/upgrades-SuccessUpgradingV3ThenV2-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SuccessUpgradingV3ThenV2-v1.dar",
            "test-common/upgrades-SuccessUpgradingV3ThenV2-v2.dar",
            None,
          ),
          (
            "test-common/upgrades-SuccessUpgradingV3ThenV2-v1.dar",
            "test-common/upgrades-SuccessUpgradingV3ThenV2-v3.dar",
            None,
          ),
        ),
      )
    }

    s"Fails when v1 upgrades to v2, but v3 does not upgrade v2" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenUpgradingV2ThenV3-v1.dar",
          "test-common/upgrades-FailsWhenUpgradingV2ThenV3-v2.dar",
          "test-common/upgrades-FailsWhenUpgradingV2ThenV3-v3.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenUpgradingV2ThenV3-v1.dar",
            "test-common/upgrades-FailsWhenUpgradingV2ThenV3-v2.dar",
            None,
          ),
          (
            "test-common/upgrades-FailsWhenUpgradingV2ThenV3-v2.dar",
            "test-common/upgrades-FailsWhenUpgradingV2ThenV3-v3.dar",
            Some("The upgraded template T is missing some of its original fields."),
          ),
        ),
      )
    }

    s"Fails when v1 upgrades to v3, but v3 does not upgrade v2" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenUpgradingV3ThenV2-v1.dar",
          "test-common/upgrades-FailsWhenUpgradingV3ThenV2-v3.dar",
          "test-common/upgrades-FailsWhenUpgradingV3ThenV2-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenUpgradingV3ThenV2-v1.dar",
            "test-common/upgrades-FailsWhenUpgradingV3ThenV2-v2.dar",
            None,
          ),
          (
            "test-common/upgrades-FailsWhenUpgradingV3ThenV2-v3.dar",
            "test-common/upgrades-FailsWhenUpgradingV3ThenV2-v2.dar",
            Some("The upgraded template T is missing some of its original fields."),
          ),
        ),
      )
    }

    "Fails when an instance is dropped." in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenAnInstanceIsDropped-dep.dar",
          "test-common/upgrades-FailsWhenAnInstanceIsDropped-v1.dar",
          "test-common/upgrades-FailsWhenAnInstanceIsDropped-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenAnInstanceIsDropped-v1.dar",
            "test-common/upgrades-FailsWhenAnInstanceIsDropped-v2.dar",
            Some(
              "Implementation of interface .*:Dep:I by template T appears in package that is being upgraded, but does not appear in this package."
            ),
          )
        ),
      )
    }

    "Fails when an instance is added (separate dep)." in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenAnInstanceIsAddedSeparateDep-dep.dar",
          "test-common/upgrades-FailsWhenAnInstanceIsAddedSeparateDep-v1.dar",
          "test-common/upgrades-FailsWhenAnInstanceIsAddedSeparateDep-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenAnInstanceIsAddedSeparateDep-v1.dar",
            "test-common/upgrades-FailsWhenAnInstanceIsAddedSeparateDep-v2.dar",
            Some(
              "Implementation of interface .*:Dep:I by template T appears in this package, but does not appear in package that is being upgraded."
            ),
          )
        ),
      )
    }

    s"report no upgrade errors for valid upgrade" in {
      testPackages(
        Seq("test-common/upgrades-ValidUpgrade-v1.dar", "test-common/upgrades-ValidUpgrade-v2.dar"),
        Seq(
          (
            "test-common/upgrades-ValidUpgrade-v1.dar",
            "test-common/upgrades-ValidUpgrade-v2.dar",
            None,
          )
        ),
      )
    }
    s"report no upgrade errors for valid upgrades of parameterized data types" in {
      testPackages(
        Seq(
          "test-common/upgrades-ValidParameterizedTypesUpgrade-v1.dar",
          "test-common/upgrades-ValidParameterizedTypesUpgrade-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-ValidParameterizedTypesUpgrade-v1.dar",
            "test-common/upgrades-ValidParameterizedTypesUpgrade-v2.dar",
            None,
          )
        ),
      )
    }
    s"report no upgrade errors for alpha-equivalent complex key types" in {
      testPackages(
        Seq(
          "test-common/upgrades-ValidKeyTypeEquality-v1.dar",
          "test-common/upgrades-ValidKeyTypeEquality-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-ValidKeyTypeEquality-v1.dar",
            "test-common/upgrades-ValidKeyTypeEquality-v2.dar",
            None,
          )
        ),
      )
    }
    s"report error when module is missing in upgrading package" in {
      testPackages(
        Seq(
          "test-common/upgrades-MissingModule-v1.dar",
          "test-common/upgrades-MissingModule-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-MissingModule-v1.dar",
            "test-common/upgrades-MissingModule-v2.dar",
            Some(
              "Module Other appears in package that is being upgraded, but does not appear in the upgrading package."
            ),
          )
        ),
      )
    }
    s"report error when template is missing in upgrading package" in {
      testPackages(
        Seq(
          "test-common/upgrades-MissingTemplate-v1.dar",
          "test-common/upgrades-MissingTemplate-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-MissingTemplate-v1.dar",
            "test-common/upgrades-MissingTemplate-v2.dar",
            Some(
              "Template U appears in package that is being upgraded, but does not appear in the upgrading package."
            ),
          )
        ),
      )
    }
    s"allow uploading a package with a missing template but for a different package-name" in {
      testPackages(
        Seq(
          "test-common/upgrades-MissingTemplate-v1.dar",
          "test-common/upgrades-MissingTemplateDifferentPackageName.dar",
        ),
        Seq(
          (
            "test-common/upgrades-MissingTemplate-v1.dar",
            "test-common/upgrades-MissingTemplateDifferentPackageName.dar",
            None,
          )
        ),
      )
    }
    s"report error when datatype is missing in upgrading package" in {
      testPackages(
        Seq(
          "test-common/upgrades-MissingDataCon-v1.dar",
          "test-common/upgrades-MissingDataCon-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-MissingDataCon-v1.dar",
            "test-common/upgrades-MissingDataCon-v2.dar",
            Some(
              "Data type U appears in package that is being upgraded, but does not appear in the upgrading package."
            ),
          )
        ),
      )
    }
    s"report error when choice is missing in upgrading package" in {
      testPackages(
        Seq(
          "test-common/upgrades-MissingChoice-v1.dar",
          "test-common/upgrades-MissingChoice-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-MissingChoice-v1.dar",
            "test-common/upgrades-MissingChoice-v2.dar",
            Some(
              "Choice C2 appears in package that is being upgraded, but does not appear in the upgrading package."
            ),
          )
        ),
      )
    }
    s"succeed when adding a choice to a template in upgrading package" in {
      testPackages(
        Seq(
          "test-common/upgrades-TemplateAddedChoice-v1.dar",
          "test-common/upgrades-TemplateAddedChoice-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-TemplateAddedChoice-v1.dar",
            "test-common/upgrades-TemplateAddedChoice-v2.dar",
            None,
          )
        ),
      )
    }
    s"report error when key type changes" in {
      testPackages(
        Seq(
          "test-common/upgrades-TemplateChangedKeyType-v1.dar",
          "test-common/upgrades-TemplateChangedKeyType-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-TemplateChangedKeyType-v1.dar",
            "test-common/upgrades-TemplateChangedKeyType-v2.dar",
            Some("The upgraded template T cannot change its key type."),
          )
        ),
      )
    }
    s"report error when record fields change" in {
      testPackages(
        Seq(
          "test-common/upgrades-RecordFieldsNewNonOptional-v1.dar",
          "test-common/upgrades-RecordFieldsNewNonOptional-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-RecordFieldsNewNonOptional-v1.dar",
            "test-common/upgrades-RecordFieldsNewNonOptional-v2.dar",
            Some(
              "The upgraded data type Struct has added new fields, but those fields are not Optional."
            ),
          )
        ),
      )
    }

    // Ported from DamlcUpgrades.hs
    s"Fails when template changes key type" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenTemplateChangesKeyType-v1.dar",
          "test-common/upgrades-FailsWhenTemplateChangesKeyType-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenTemplateChangesKeyType-v1.dar",
            "test-common/upgrades-FailsWhenTemplateChangesKeyType-v2.dar",
            Some("The upgraded template A cannot change its key type."),
          )
        ),
      )
    }
    s"Fails when template removes key type" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v1.dar",
          "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v1.dar",
            "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v2.dar",
            Some("The upgraded template A cannot remove its key."),
          )
        ),
      )
    }
    s"Fails when template adds key type" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenTemplateAddsKeyType-v1.dar",
          "test-common/upgrades-FailsWhenTemplateAddsKeyType-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenTemplateAddsKeyType-v1.dar",
            "test-common/upgrades-FailsWhenTemplateAddsKeyType-v2.dar",
            Some("The upgraded template A cannot add a key."),
          )
        ),
      )
    }
    s"Fails when new field is added to template without Optional type" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v1.dar",
          "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v1.dar",
            "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v2.dar",
            Some("The upgraded template A has added new fields, but those fields are not Optional."),
          )
        ),
      )
    }
    s"Fails when old field is deleted from template" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v1.dar",
          "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v1.dar",
            "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v2.dar",
            Some("The upgraded template A is missing some of its original fields."),
          )
        ),
      )
    }
    s"Fails when existing field in template is changed" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v1.dar",
          "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v1.dar",
            "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v2.dar",
            Some("The upgraded template A has changed the types of some of its original fields."),
          )
        ),
      )
    }
    s"Succeeds when new field with optional type is added to template" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v1.dar",
          "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v1.dar",
            "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v2.dar",
            None,
          )
        ),
      )
    }
    s"Fails when new field is added to template choice without Optional type" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v1.dar",
          "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v1.dar",
            "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v2.dar",
            Some(
              "The upgraded input type of choice C on template A has added new fields, but those fields are not Optional."
            ),
          )
        ),
      )
    }
    s"Fails when old field is deleted from template choice" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v1.dar",
          "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v1.dar",
            "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v2.dar",
            Some(
              "The upgraded input type of choice C on template A is missing some of its original fields."
            ),
          )
        ),
      )
    }
    s"Fails when existing field in template choice is changed" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v1.dar",
          "test-common/upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v1.dar",
            "test-common/upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v2.dar",
            Some(
              "The upgraded input type of choice C on template A has changed the types of some of its original fields."
            ),
          )
        ),
      )
    }
    s"Fails when template choice changes its return type" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v1.dar",
          "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v1.dar",
            "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v2.dar",
            Some("The upgraded choice C cannot change its return type."),
          )
        ),
      )
    }
    s"Succeeds when template choice returns a template which has changed" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v1.dar",
          "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v1.dar",
            "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v2.dar",
            None,
          )
        ),
      )
    }
    s"Succeeds when template choice input argument template has changed" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentTemplateHasChanged-v1.dar",
          "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentTemplateHasChanged-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentTemplateHasChanged-v1.dar",
            "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentTemplateHasChanged-v2.dar",
            None,
          )
        ),
      )
    }
    s"Succeeds when template choice input argument enum has changed" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentEnumHasChanged-v1.dar",
          "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentEnumHasChanged-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentEnumHasChanged-v1.dar",
            "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentEnumHasChanged-v2.dar",
            None,
          )
        ),
      )
    }
    s"Succeeds when template choice input argument struct has changed" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentStructHasChanged-v1.dar",
          "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentStructHasChanged-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentStructHasChanged-v1.dar",
            "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentStructHasChanged-v2.dar",
            None,
          )
        ),
      )
    }
    s"Succeeds when template choice input argument variant has changed" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentVariantHasChanged-v1.dar",
          "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentVariantHasChanged-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentVariantHasChanged-v1.dar",
            "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentVariantHasChanged-v2.dar",
            None,
          )
        ),
      )
    }
    s"Succeeds when new field with optional type is added to template choice" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v1.dar",
          "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v1.dar",
            "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when a top-level record adds a non-optional field" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenATopLevelRecordAddsANonOptionalField-v1.dar",
          "test-common/upgrades-FailsWhenATopLevelRecordAddsANonOptionalField-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenATopLevelRecordAddsANonOptionalField-v1.dar",
            "test-common/upgrades-FailsWhenATopLevelRecordAddsANonOptionalField-v2.dar",
            Some(
              "The upgraded data type A has added new fields, but those fields are not Optional."
            ),
          )
        ),
      )
    }

    "Succeeds when a top-level record adds an optional field at the end" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v1.dar",
          "test-common/upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v1.dar",
            "test-common/upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when a top-level record adds an optional field before the end" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd-v1.dar",
          "test-common/upgrades-FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd-v1.dar",
            "test-common/upgrades-FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd-v2.dar",
            Some(
              "The upgraded data type A has changed the order of its fields - any new fields must be added at the end of the record."
            ),
          )
        ),
      )
    }

    "Succeeds when a top-level variant adds a variant" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAConstructor-v1.dar",
          "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAConstructor-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAConstructor-v1.dar",
            "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAConstructor-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when a top-level variant removes a variant" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenATopLevelVariantRemovesAConstructor-v1.dar",
          "test-common/upgrades-FailsWhenATopLevelVariantRemovesAConstructor-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenATopLevelVariantRemovesAConstructor-v1.dar",
            "test-common/upgrades-FailsWhenATopLevelVariantRemovesAConstructor-v2.dar",
            Some(
              "Data type A.Z appears in package that is being upgraded, but does not appear in the upgrading package."
            ),
          )
        ),
      )
    }

    "Fail when a top-level variant changes changes the order of its variants" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors-v1.dar",
          "test-common/upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors-v1.dar",
            "test-common/upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors-v2.dar",
            Some(
              "The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the variant."
            ),
          )
        ),
      )
    }

    "Fails when a top-level variant adds a field to a variant's type" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenATopLevelVariantAddsAFieldToAConstructorsType-v1.dar",
          "test-common/upgrades-FailsWhenATopLevelVariantAddsAFieldToAConstructorsType-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenATopLevelVariantAddsAFieldToAConstructorsType-v1.dar",
            "test-common/upgrades-FailsWhenATopLevelVariantAddsAFieldToAConstructorsType-v2.dar",
            Some("The upgraded variant constructor A.Y from variant A has added a field."),
          )
        ),
      )
    }

    "Succeeds when a top-level variant adds an optional field to a variant's type" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType-v1.dar",
          "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType-v1.dar",
            "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when a top-level enum drops a constructor" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenAnEnumDropsAConstructor-v1.dar",
          "test-common/upgrades-FailsWhenAnEnumDropsAConstructor-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenAnEnumDropsAConstructor-v1.dar",
            "test-common/upgrades-FailsWhenAnEnumDropsAConstructor-v2.dar",
            Some("The upgraded data type MyEnum has removed an existing variant."),
          )
        ),
      )
    }

    "Succeeds when a top-level enum changes" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenATopLevelEnumChanges-v1.dar",
          "test-common/upgrades-SucceedsWhenATopLevelEnumChanges-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenATopLevelEnumChanges-v1.dar",
            "test-common/upgrades-SucceedsWhenATopLevelEnumChanges-v2.dar",
            None,
          )
        ),
      )
    }

    "Fail when a top-level enum changes changes the order of its variants" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors-v1.dar",
          "test-common/upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors-v1.dar",
            "test-common/upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors-v2.dar",
            Some(
              "The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the enum."
            ),
          )
        ),
      )
    }

    "Succeeds when a top-level type synonym changes" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenATopLevelTypeSynonymChanges-v1.dar",
          "test-common/upgrades-SucceedsWhenATopLevelTypeSynonymChanges-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenATopLevelTypeSynonymChanges-v1.dar",
            "test-common/upgrades-SucceedsWhenATopLevelTypeSynonymChanges-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds when two deeply nested type synonyms resolve to the same datatypes" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes-v1.dar",
          "test-common/upgrades-SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes-v1.dar",
            "test-common/upgrades-SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when two deeply nested type synonyms resolve to different datatypes" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes-v1.dar",
          "test-common/upgrades-FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes-v1.dar",
            "test-common/upgrades-FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes-v2.dar",
            Some("The upgraded template A has changed the types of some of its original fields."),
          )
        ),
      )
    }

    "Fails when datatype changes variety" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenDatatypeChangesVariety-v1.dar",
          "test-common/upgrades-FailsWhenDatatypeChangesVariety-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenDatatypeChangesVariety-v1.dar",
            "test-common/upgrades-FailsWhenDatatypeChangesVariety-v2.dar",
            Some("The upgraded data type RecordToEnum has changed from a record to a enum."),
          )
        ),
      )
    }

    "Succeeds when adding non-optional fields to unserializable types" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes-v1.dar",
          "test-common/upgrades-SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes-v1.dar",
            "test-common/upgrades-SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds when changing variant of unserializable type" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenChangingConstructorOfUnserializableType-v1.dar",
          "test-common/upgrades-SucceedsWhenChangingConstructorOfUnserializableType-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenChangingConstructorOfUnserializableType-v1.dar",
            "test-common/upgrades-SucceedsWhenChangingConstructorOfUnserializableType-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds when deleting unserializable type" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenDeletingUnserializableType-v1.dar",
          "test-common/upgrades-SucceedsWhenDeletingUnserializableType-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenDeletingUnserializableType-v1.dar",
            "test-common/upgrades-SucceedsWhenDeletingUnserializableType-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when making type unserializable" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenMakingTypeUnserializable-v1.dar",
          "test-common/upgrades-FailsWhenMakingTypeUnserializable-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenMakingTypeUnserializable-v1.dar",
            "test-common/upgrades-FailsWhenMakingTypeUnserializable-v2.dar",
            Some(
              "The upgraded data type MyData was serializable and is now unserializable. Datatypes cannot change their serializability via upgrades."
            ),
          )
        ),
      )
    }

    // Copied interface tests
    "Succeeds when an interface is only defined in the initial package." in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage-v1.dar",
          "test-common/upgrades-SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage-v1.dar",
            "test-common/upgrades-SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when an interface is defined in an upgrading package when it was already in the prior package." in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v1.dar",
          "test-common/upgrades-FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v1.dar",
            "test-common/upgrades-FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v2.dar",
            Some(
              "Tried to upgrade interface I, but interfaces cannot be upgraded. They should be removed in any upgrading package."
            ),
          )
        ),
      )
    }

    "Fails when an instance is added (upgraded package)." in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenAnInstanceIsAddedUpgradedPackage-v1.dar",
          "test-common/upgrades-FailsWhenAnInstanceIsAddedUpgradedPackage-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenAnInstanceIsAddedUpgradedPackage-v1.dar",
            "test-common/upgrades-FailsWhenAnInstanceIsAddedUpgradedPackage-v2.dar",
            Some(
              "Implementation of interface .*:Main:I by template T appears in this package, but does not appear in package that is being upgraded."
            ),
          )
        ),
      )
    }

    "Fails when an instance is replaced with a different instance of an identically named interface." in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenAnInstanceIsReplacedWithADifferentInstanceOfAnIdenticallyNamedInterface-v1.dar",
          "test-common/upgrades-FailsWhenAnInstanceIsReplacedWithADifferentInstanceOfAnIdenticallyNamedInterface-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenAnInstanceIsReplacedWithADifferentInstanceOfAnIdenticallyNamedInterface-v1.dar",
            "test-common/upgrades-FailsWhenAnInstanceIsReplacedWithADifferentInstanceOfAnIdenticallyNamedInterface-v2.dar",
            Some(
              "Implementation of interface .*:Dep:I by template T appears in package that is being upgraded, but does not appear in this package."
            ),
          )
        ),
      )
    }

    "Succeeds when an instance is added to a new template (upgraded package)." in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage-v1.dar",
          "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage-v1.dar",
            "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds when an instance is added to a new template (separate dep)." in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep-v1.dar",
          "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep-v1.dar",
            "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds even when non-serializable types are incompatible" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenNonSerializableTypesAreIncompatible-v1.dar",
          "test-common/upgrades-SucceedsWhenNonSerializableTypesAreIncompatible-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenNonSerializableTypesAreIncompatible-v1.dar",
            "test-common/upgrades-SucceedsWhenNonSerializableTypesAreIncompatible-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when comparing types from packages with different names" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-v1.dar",
          "test-common/upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-v1.dar",
            "test-common/upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-v2.dar",
            Some("The upgraded data type A has changed the types of some of its original fields."),
          )
        ),
      )
    }

    "Fails when comparing type constructors from other packages that resolve to incompatible types" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenUpgradedFieldPackagesAreNotUpgradable-v1.dar",
          "test-common/upgrades-FailsWhenUpgradedFieldPackagesAreNotUpgradable-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenUpgradedFieldPackagesAreNotUpgradable-v1.dar",
            "test-common/upgrades-FailsWhenUpgradedFieldPackagesAreNotUpgradable-v2.dar",
            Some("The upgraded data type T has changed the types of some of its original fields."),
          )
        ),
      )
    }

    "FailWhenParamCountChanges" in {
      testPackages(
        Seq(
          "test-common/upgrades-FailWhenParamCountChanges-v1.dar",
          "test-common/upgrades-FailWhenParamCountChanges-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailWhenParamCountChanges-v1.dar",
            "test-common/upgrades-FailWhenParamCountChanges-v2.dar",
            Some(
              "The upgraded data type MyStruct has changed the number of type variables it has."
            ),
          )
        ),
      )
    }

    "SucceedWhenParamNameChanges" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedWhenParamNameChanges-v1.dar",
          "test-common/upgrades-SucceedWhenParamNameChanges-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedWhenParamNameChanges-v1.dar",
            "test-common/upgrades-SucceedWhenParamNameChanges-v2.dar",
            None,
          )
        ),
      )
    }

    "SucceedWhenPhantomParamBecomesUsed" in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedWhenPhantomParamBecomesUsed-v1.dar",
          "test-common/upgrades-SucceedWhenPhantomParamBecomesUsed-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedWhenPhantomParamBecomesUsed-v1.dar",
            "test-common/upgrades-SucceedWhenPhantomParamBecomesUsed-v2.dar",
            None,
          )
        ),
      )
    }

    "Succeeds when an exception is only defined in the initial package." in {
      testPackages(
        Seq(
          "test-common/upgrades-SucceedsWhenAnExceptionIsOnlyDefinedInTheInitialPackage-v1.dar",
          "test-common/upgrades-SucceedsWhenAnExceptionIsOnlyDefinedInTheInitialPackage-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-SucceedsWhenAnExceptionIsOnlyDefinedInTheInitialPackage-v1.dar",
            "test-common/upgrades-SucceedsWhenAnExceptionIsOnlyDefinedInTheInitialPackage-v2.dar",
            None,
          )
        ),
      )
    }

    "Fails when an exception is defined in an upgrading package when it was already in the prior package." in {
      testPackages(
        Seq(
          "test-common/upgrades-FailsWhenAnExceptionIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v1.dar",
          "test-common/upgrades-FailsWhenAnExceptionIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v2.dar",
        ),
        Seq(
          (
            "test-common/upgrades-FailsWhenAnExceptionIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v1.dar",
            "test-common/upgrades-FailsWhenAnExceptionIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v2.dar",
            Some(
              "Tried to upgrade exception E, but exceptions cannot be upgraded. They should be removed in any upgrading package."
            ),
          )
        ),
      )
    }
  }
}
