// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation
package upgrade

import com.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.{/*Dar,*/ DarReader/*, DarWriter*/}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.google.protobuf.ByteString
import org.scalatest.{Assertion, Inside}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.{Path, Paths}
import java.io.File
import java.io.FileInputStream
import scala.concurrent.Future
import scala.sys.process._
import com.daml.scalautil.Statement.discard
import org.scalatest.{ParallelTestExecution}

final class UpgradesCheckSpec
    extends AsyncWordSpec
    with Matchers
    with Inside
    with ParallelTestExecution {
  protected def loadPackageIdAndBS(path: String): (PackageId, ByteString) = {
    val dar = DarReader.assertReadArchiveFromFile(new File(BazelRunfiles.rlocation(path)))
    assert(dar != null, s"Unable to load test package resource '$path'")

    val testPackage = {
      val in = new FileInputStream(new File(BazelRunfiles.rlocation(path)))
      assert(in != null, s"Unable to load test package resource '$path'")
      in
    }
    val bytes =
      try {
        ByteString.readFrom(testPackage)
      } finally {
        testPackage.close()
      }
    dar.main.pkgId -> bytes
  }

  def testPackagePair(
      upgraded: String,
      upgrading: String,
      uploadAssertion: (
          PackageId,
          PackageId,
      ) => String => Assertion,
  ): Future[Assertion] = {
    val (v1PackageId, _) = loadPackageIdAndBS(upgraded)
    val (v2PackageId, _) = loadPackageIdAndBS(upgrading)
    val v1Path = BazelRunfiles.rlocation(upgraded).toString
    val v2Path = BazelRunfiles.rlocation(upgrading).toString

    val exe = if (sys.props("os.name").toLowerCase.contains("windows")) ".exe" else ""
    val damlSdk = BazelRunfiles.rlocation(Paths.get(s"daml-assistant/daml-sdk/sdk$exe"))

    // Runs process with args, returns status and stdout <> stderr
    def runProc(exe: Path, args: Seq[String]): Future[Either[String, String]] =
      Future {
        val out = new StringBuilder()
        val cmd = exe.toString +: args
        cmd !< ProcessLogger(line => discard(out append line append '\n')) match {
          case 0 => Right(out.toString)
          case _ => Left(out.toString)
        }
      }

    for {
      result <- runProc(damlSdk, Seq("upgrade-check", v1Path, v2Path))
      assertion <- result match {
        case Left(out) =>
          uploadAssertion(v1PackageId, v2PackageId)(out)
        case Right(out) =>
          uploadAssertion(v1PackageId, v2PackageId)(out)
      }
    } yield assertion
  }

  def assertPackageUpgradeCheck(failureMessage: Option[String])(
      testPackageFirstId: PackageId,
      testPackageSecondId: PackageId,
  )(upgradeCheckToolLogs: String): Assertion = {
    failureMessage match {
      case None => upgradeCheckToolLogs should not include regex(s"Error while checking two DARs:\nThe uploaded DAR contains a package $testPackageSecondId \\(.*\\), but upgrade checks indicate that (existing package $testPackageFirstId|new package $testPackageSecondId) \\(.*\\) cannot be an upgrade of (existing package $testPackageFirstId|new package $testPackageSecondId)")
      case Some(msg) => upgradeCheckToolLogs should include regex(msg)
    }
  }

  s"Upgradeability Checks using `daml upgrade-check` tool" should {
    s"report no upgrade errors for valid upgrade" in {
      testPackagePair(
        "test-common/upgrades-ValidUpgrade-v1.dar",
        "test-common/upgrades-ValidUpgrade-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"report no upgrade errors for valid upgrades of parameterized data types" in {
      testPackagePair(
        "test-common/upgrades-ValidParameterizedTypesUpgrade-v1.dar",
        "test-common/upgrades-ValidParameterizedTypesUpgrade-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"report no upgrade errors for alpha-equivalent complex key types" in {
      testPackagePair(
        "test-common/upgrades-ValidKeyTypeEquality-v1.dar",
        "test-common/upgrades-ValidKeyTypeEquality-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"report error when module is missing in upgrading package" in {
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
    s"report error when template is missing in upgrading package" in {
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
    s"allow uploading a package with a missing template but for a different package-name" in {
      testPackagePair(
        "test-common/upgrades-MissingTemplate-v1.dar",
        "test-common/upgrades-MissingTemplateDifferentPackageName.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"report error when datatype is missing in upgrading package" in {
      testPackagePair(
        "test-common/upgrades-MissingDataCon-v1.dar",
        "test-common/upgrades-MissingDataCon-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "Data type U appears in package that is being upgraded, but does not appear in the upgrading package."
          )
        ),
      )
    }
    s"report error when choice is missing in upgrading package" in {
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
    s"report error when key type changes" in {
      testPackagePair(
        "test-common/upgrades-TemplateChangedKeyType-v1.dar",
        "test-common/upgrades-TemplateChangedKeyType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded template T cannot change its key type.")),
      )
    }
    s"report error when record fields change" in {
      testPackagePair(
        "test-common/upgrades-RecordFieldsNewNonOptional-v1.dar",
        "test-common/upgrades-RecordFieldsNewNonOptional-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "The upgraded data type Struct has added new fields, but those fields are not Optional."
          )
        ),
      )
    }

    // Ported from DamlcUpgrades.hs
    s"Fails when template changes key type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTemplateChangesKeyType-v1.dar",
        "test-common/upgrades-FailsWhenTemplateChangesKeyType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded template A cannot change its key type.")),
      )
    }
    s"Fails when template removes key type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v1.dar",
        "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded template A cannot remove its key.")),
      )
    }
    s"Fails when template adds key type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTemplateAddsKeyType-v1.dar",
        "test-common/upgrades-FailsWhenTemplateAddsKeyType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded template A cannot add a key.")),
      )
    }
    s"Fails when new field is added to template without Optional type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v1.dar",
        "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded template A has added new fields, but those fields are not Optional.")
        ),
      )
    }
    s"Fails when old field is deleted from template" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v1.dar",
        "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded template A is missing some of its original fields.")
        ),
      )
    }
    s"Fails when existing field in template is changed" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v1.dar",
        "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded template A has changed the types of some of its original fields.")
        ),
      )
    }
    s"Succeeds when new field with optional type is added to template" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v1.dar",
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"Fails when new field is added to template choice without Optional type" in {
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
    s"Fails when old field is deleted from template choice" in {
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
    s"Fails when existing field in template choice is changed" in {
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
    s"Fails when template choice changes its return type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v1.dar",
        "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded choice C cannot change its return type.")),
      )
    }
    s"Succeeds when template choice returns a template which has changed" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v1.dar",
        "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"Succeeds when template choice input argument has changed" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentHasChanged-v1.dar",
        "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentHasChanged-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"Succeeds when new field with optional type is added to template choice" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v1.dar",
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    "Fails when a top-level record adds a non-optional field" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenATopLevelRecordAddsANonOptionalField-v1.dar",
        "test-common/upgrades-FailsWhenATopLevelRecordAddsANonOptionalField-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded data type A has added new fields, but those fields are not Optional.")
        ),
      )
    }

    "Succeeds when a top-level record adds an optional field at the end" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v1.dar",
        "test-common/upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v2.dar",
        assertPackageUpgradeCheck(
          None
        ),
      )
    }

    "Fails when a top-level record adds an optional field before the end" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd-v1.dar",
        "test-common/upgrades-FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "The upgraded data type A has changed the order of its fields - any new fields must be added at the end of the record."
          )
        ),
      )
    }

    "Succeeds when a top-level variant adds a variant" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAConstructor-v1.dar",
        "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAConstructor-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    "Fails when a top-level variant removes a variant" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenATopLevelVariantRemovesAConstructor-v1.dar",
        "test-common/upgrades-FailsWhenATopLevelVariantRemovesAConstructor-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "Data type A.Z appears in package that is being upgraded, but does not appear in the upgrading package."
          )
        ),
      )
    }

    "Fail when a top-level variant changes changes the order of its variants" in {
      testPackagePair(
        "test-common/upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors-v1.dar",
        "test-common/upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the variant."
          )
        ),
      )
    }

    "Fails when a top-level variant adds a field to a variant's type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenATopLevelVariantAddsAFieldToAConstructorsType-v1.dar",
        "test-common/upgrades-FailsWhenATopLevelVariantAddsAFieldToAConstructorsType-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded variant constructor A.Y from variant A has added a field.")
        ),
      )
    }

    "Succeeds when a top-level variant adds an optional field to a variant's type" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType-v1.dar",
        "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    "Succeeds when a top-level enum changes" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenATopLevelEnumChanges-v1.dar",
        "test-common/upgrades-SucceedsWhenATopLevelEnumChanges-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    "Fail when a top-level enum changes changes the order of its variants" in {
      testPackagePair(
        "test-common/upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors-v1.dar",
        "test-common/upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the enum."
          )
        ),
      )
    }

    "Succeeds when a top-level type synonym changes" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenATopLevelTypeSynonymChanges-v1.dar",
        "test-common/upgrades-SucceedsWhenATopLevelTypeSynonymChanges-v2.dar",
        assertPackageUpgradeCheck(
          None
        ),
      )
    }

    "Succeeds when two deeply nested type synonyms resolve to the same datatypes" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes-v1.dar",
        "test-common/upgrades-SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes-v2.dar",
        assertPackageUpgradeCheck(
          None
        ),
      )
    }

    "Fails when two deeply nested type synonyms resolve to different datatypes" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes-v1.dar",
        "test-common/upgrades-FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded template A has changed the types of some of its original fields.")
        ),
      )
    }

    "Fails when datatype changes variety" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenDatatypeChangesVariety-v1.dar",
        "test-common/upgrades-FailsWhenDatatypeChangesVariety-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded data type RecordToEnum has changed from a record to a enum.")
        ),
      )
    }

    "Succeeds when adding non-optional fields to unserializable types" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes-v1.dar",
        "test-common/upgrades-SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes-v2.dar",
        assertPackageUpgradeCheck(
          None
        ),
      )
    }

    "Succeeds when changing variant of unserializable type" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenChangingConstructorOfUnserializableType-v1.dar",
        "test-common/upgrades-SucceedsWhenChangingConstructorOfUnserializableType-v2.dar",
        assertPackageUpgradeCheck(
          None
        ),
      )
    }

    "Succeeds when deleting unserializable type" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenDeletingUnserializableType-v1.dar",
        "test-common/upgrades-SucceedsWhenDeletingUnserializableType-v2.dar",
        assertPackageUpgradeCheck(
          None
        ),
      )
    }

    "Fails when making type unserializable" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenMakingTypeUnserializable-v1.dar",
        "test-common/upgrades-FailsWhenMakingTypeUnserializable-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "The upgraded data type MyData was serializable and is now unserializable. Datatypes cannot change their serializability via upgrades."
          )
        ),
      )
    }

    // Copied interface tests
    "Succeeds when an interface is only defined in the initial package." in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage-v1.dar",
        "test-common/upgrades-SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage-v2.dar",
        assertPackageUpgradeCheck(
          None
        ),
      )
    }

    "Fails when an interface is defined in an upgrading package when it was already in the prior package." in {
      testPackagePair(
        "test-common/upgrades-FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v1.dar",
        "test-common/upgrades-FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "Tried to upgrade interface I, but interfaces cannot be upgraded. They should be removed in any upgrading package."
          )
        ),
      )
    }

    "Fails when an instance is added (upgraded package)." in {
      testPackagePair(
        "test-common/upgrades-FailsWhenAnInstanceIsAddedUpgradedPackage-v1.dar",
        "test-common/upgrades-FailsWhenAnInstanceIsAddedUpgradedPackage-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "Implementation of interface .*:Main:I by template T appears in this package, but does not appear in package that is being upgraded."
          )
        ),
      )
    }

    "Succeeds when an instance is added to a new template (upgraded package)." in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage-v1.dar",
        "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage-v2.dar",
        assertPackageUpgradeCheck(
          None
        ),
      )
    }

    "Succeeds when an instance is added to a new template (separate dep)." in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep-v1.dar",
        "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep-v2.dar",
        assertPackageUpgradeCheck(
          None
        ),
      )
    }

    "Succeeds even when non-serializable types are incompatible" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenNonSerializableTypesAreIncompatible-v1.dar",
        "test-common/upgrades-SucceedsWhenNonSerializableTypesAreIncompatible-v2.dar",
        assertPackageUpgradeCheck(
          None
        ),
      )
    }

    "Fails when comparing types from packages with different names" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-v1.dar",
        "test-common/upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded data type A has changed the types of some of its original fields.")
        ),
      )
    }

    "Fails when comparing type constructors from other packages that resolve to incompatible types" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenUpgradedFieldPackagesAreNotUpgradable-v1.dar",
        "test-common/upgrades-FailsWhenUpgradedFieldPackagesAreNotUpgradable-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded data type T has changed the types of some of its original fields.")
        ),
      )
    }

    "FailWhenParamCountChanges" in {
      testPackagePair(
        "test-common/upgrades-FailWhenParamCountChanges-v1.dar",
        "test-common/upgrades-FailWhenParamCountChanges-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "The upgraded data type MyStruct has changed the number of type variables it has."
          )
        ),
      )
    }

    "SucceedWhenParamNameChanges" in {
      testPackagePair(
        "test-common/upgrades-SucceedWhenParamNameChanges-v1.dar",
        "test-common/upgrades-SucceedWhenParamNameChanges-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    "SucceedWhenPhantomParamBecomesUsed" in {
      testPackagePair(
        "test-common/upgrades-SucceedWhenPhantomParamBecomesUsed-v1.dar",
        "test-common/upgrades-SucceedWhenPhantomParamBecomesUsed-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
  }
}
