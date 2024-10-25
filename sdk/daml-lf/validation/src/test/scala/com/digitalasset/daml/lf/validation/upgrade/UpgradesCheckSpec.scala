// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation
package upgrade

import com.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.archive.{/*Dar,*/ DarReader/*, DarWriter*/}
import com.digitalasset.daml.lf.data.Ref.PackageId
import org.scalatest.{Assertion, Inside}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.{Path, Paths}
import java.io.File
import scala.concurrent.Future
import scala.sys.process._
import com.daml.scalautil.Statement.discard
import org.scalatest.{ParallelTestExecution}

final class UpgradesCheckSpec
    extends AsyncWordSpec
    with Matchers
    with Inside
    with ParallelTestExecution {
  protected def loadPackageId(path: Path): PackageId = {
    val dar = DarReader.assertReadArchiveFromFile(new File(path.toString))
    assert(dar != null, s"Unable to load test package resource '$path'")
    dar.main.pkgId
  }

  def testPackages(
      rawPaths: Seq[String],
      uploadAssertion: Seq[PackageId] => String => Assertion,
  ): Future[Assertion] = {
    val paths: Seq[Path] = rawPaths.map((x: String) => BazelRunfiles.rlocation(Paths.get(x)))
    val pkgIds: Seq[PackageId] = paths.map(loadPackageId)

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
      result <- runProc(damlSdk, "upgrade-check" +: paths.map(_.toString))
      assertion <- result match {
        case Left(out) =>
          uploadAssertion(pkgIds)(out)
        case Right(out) =>
          uploadAssertion(pkgIds)(out)
      }
    } yield assertion
  }

  def checkTwo(failureMessage: Option[String])(
      pkgIds: Seq[PackageId]
  )(upgradeCheckToolLogs: String): Assertion = {
    val testPackageFirstId: PackageId = pkgIds(0)
    val testPackageSecondId: PackageId = pkgIds(1)
    failureMessage match {
      case None => upgradeCheckToolLogs should not include regex(s"Error while checking two DARs:\nThe uploaded DAR contains a package $testPackageSecondId \\(.*\\), but upgrade checks indicate that (existing package $testPackageFirstId|new package $testPackageSecondId) \\(.*\\) cannot be an upgrade of (existing package $testPackageFirstId|new package $testPackageSecondId)")
      case Some(msg) => upgradeCheckToolLogs should include regex(s"Error while checking two DARs:\nThe uploaded DAR contains a package $testPackageSecondId \\(.*\\), but upgrade checks indicate that (existing package $testPackageFirstId|new package $testPackageSecondId) \\(.*\\) cannot be an upgrade of (existing package $testPackageFirstId|new package $testPackageSecondId). Reason: $msg")
    }
  }

  s"Upgradeability Checks using `daml upgrade-check` tool" should {
    s"report no upgrade errors for valid upgrade" in {
      testPackages(
        Seq("test-common/upgrades-ValidUpgrade-v1.dar", "test-common/upgrades-ValidUpgrade-v2.dar"),
        checkTwo(None),
      )
    }
    s"report no upgrade errors for valid upgrades of parameterized data types" in {
      testPackages(
        Seq("test-common/upgrades-ValidParameterizedTypesUpgrade-v1.dar", "test-common/upgrades-ValidParameterizedTypesUpgrade-v2.dar"),
        checkTwo(None),
      )
    }
    s"report no upgrade errors for alpha-equivalent complex key types" in {
      testPackages(
        Seq("test-common/upgrades-ValidKeyTypeEquality-v1.dar", "test-common/upgrades-ValidKeyTypeEquality-v2.dar"),
        checkTwo(None),
      )
    }
    s"report error when module is missing in upgrading package" in {
      testPackages(
        Seq("test-common/upgrades-MissingModule-v1.dar", "test-common/upgrades-MissingModule-v2.dar"),
        checkTwo(
          Some(
            "Module Other appears in package that is being upgraded, but does not appear in the upgrading package."
          )
        ),
      )
    }
    s"report error when template is missing in upgrading package" in {
      testPackages(
        Seq("test-common/upgrades-MissingTemplate-v1.dar", "test-common/upgrades-MissingTemplate-v2.dar"),
        checkTwo(
          Some(
            "Template U appears in package that is being upgraded, but does not appear in the upgrading package."
          )
        ),
      )
    }
    s"allow uploading a package with a missing template but for a different package-name" in {
      testPackages(
        Seq("test-common/upgrades-MissingTemplate-v1.dar", "test-common/upgrades-MissingTemplateDifferentPackageName.dar"),
        checkTwo(None),
      )
    }
    s"report error when datatype is missing in upgrading package" in {
      testPackages(
        Seq("test-common/upgrades-MissingDataCon-v1.dar", "test-common/upgrades-MissingDataCon-v2.dar"),
        checkTwo(
          Some(
            "Data type U appears in package that is being upgraded, but does not appear in the upgrading package."
          )
        ),
      )
    }
    s"report error when choice is missing in upgrading package" in {
      testPackages(
        Seq("test-common/upgrades-MissingChoice-v1.dar", "test-common/upgrades-MissingChoice-v2.dar"),
        checkTwo(
          Some(
            "Choice C2 appears in package that is being upgraded, but does not appear in the upgrading package."
          )
        ),
      )
    }
    s"report error when key type changes" in {
      testPackages(
        Seq("test-common/upgrades-TemplateChangedKeyType-v1.dar", "test-common/upgrades-TemplateChangedKeyType-v2.dar"),
        checkTwo(Some("The upgraded template T cannot change its key type.")),
      )
    }
    s"report error when record fields change" in {
      testPackages(
        Seq("test-common/upgrades-RecordFieldsNewNonOptional-v1.dar", "test-common/upgrades-RecordFieldsNewNonOptional-v2.dar"),
        checkTwo(
          Some(
            "The upgraded data type Struct has added new fields, but those fields are not Optional."
          )
        ),
      )
    }

    // Ported from DamlcUpgrades.hs
    s"Fails when template changes key type" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenTemplateChangesKeyType-v1.dar", "test-common/upgrades-FailsWhenTemplateChangesKeyType-v2.dar"),
        checkTwo(Some("The upgraded template A cannot change its key type.")),
      )
    }
    s"Fails when template removes key type" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenTemplateRemovesKeyType-v1.dar", "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v2.dar"),
        checkTwo(Some("The upgraded template A cannot remove its key.")),
      )
    }
    s"Fails when template adds key type" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenTemplateAddsKeyType-v1.dar", "test-common/upgrades-FailsWhenTemplateAddsKeyType-v2.dar"),
        checkTwo(Some("The upgraded template A cannot add a key.")),
      )
    }
    s"Fails when new field is added to template without Optional type" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v1.dar", "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v2.dar"),
        checkTwo(
          Some("The upgraded template A has added new fields, but those fields are not Optional.")
        ),
      )
    }
    s"Fails when old field is deleted from template" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v1.dar", "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v2.dar"),
        checkTwo(
          Some("The upgraded template A is missing some of its original fields.")
        ),
      )
    }
    s"Fails when existing field in template is changed" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v1.dar", "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v2.dar"),
        checkTwo(
          Some("The upgraded template A has changed the types of some of its original fields.")
        ),
      )
    }
    s"Succeeds when new field with optional type is added to template" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v1.dar", "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v2.dar"),
        checkTwo(None),
      )
    }
    s"Fails when new field is added to template choice without Optional type" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v1.dar", "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateChoiceWithoutOptionalType-v2.dar"),
        checkTwo(
          Some(
            "The upgraded input type of choice C on template A has added new fields, but those fields are not Optional."
          )
        ),
      )
    }
    s"Fails when old field is deleted from template choice" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v1.dar", "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplateChoice-v2.dar"),
        checkTwo(
          Some(
            "The upgraded input type of choice C on template A is missing some of its original fields."
          )
        ),
      )
    }
    s"Fails when existing field in template choice is changed" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v1.dar", "test-common/upgrades-FailsWhenExistingFieldInTemplateChoiceIsChanged-v2.dar"),
        checkTwo(
          Some(
            "The upgraded input type of choice C on template A has changed the types of some of its original fields."
          )
        ),
      )
    }
    s"Fails when template choice changes its return type" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v1.dar", "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v2.dar"),
        checkTwo(Some("The upgraded choice C cannot change its return type.")),
      )
    }
    s"Succeeds when template choice returns a template which has changed" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v1.dar", "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v2.dar"),
        checkTwo(None),
      )
    }
    s"Succeeds when template choice input argument has changed" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentHasChanged-v1.dar", "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentHasChanged-v2.dar"),
        checkTwo(None),
      )
    }
    s"Succeeds when new field with optional type is added to template choice" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v1.dar", "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v2.dar"),
        checkTwo(None),
      )
    }

    "Fails when a top-level record adds a non-optional field" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenATopLevelRecordAddsANonOptionalField-v1.dar", "test-common/upgrades-FailsWhenATopLevelRecordAddsANonOptionalField-v2.dar"),
        checkTwo(
          Some("The upgraded data type A has added new fields, but those fields are not Optional.")
        ),
      )
    }

    "Succeeds when a top-level record adds an optional field at the end" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v1.dar", "test-common/upgrades-SucceedsWhenATopLevelRecordAddsAnOptionalFieldAtTheEnd-v2.dar"),
        checkTwo(
          None
        ),
      )
    }

    "Fails when a top-level record adds an optional field before the end" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd-v1.dar", "test-common/upgrades-FailsWhenATopLevelRecordAddsAnOptionalFieldBeforeTheEnd-v2.dar"),
        checkTwo(
          Some(
            "The upgraded data type A has changed the order of its fields - any new fields must be added at the end of the record."
          )
        ),
      )
    }

    "Succeeds when a top-level variant adds a variant" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenATopLevelVariantAddsAConstructor-v1.dar", "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAConstructor-v2.dar"),
        checkTwo(None),
      )
    }

    "Fails when a top-level variant removes a variant" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenATopLevelVariantRemovesAConstructor-v1.dar", "test-common/upgrades-FailsWhenATopLevelVariantRemovesAConstructor-v2.dar"),
        checkTwo(
          Some(
            "Data type A.Z appears in package that is being upgraded, but does not appear in the upgrading package."
          )
        ),
      )
    }

    "Fail when a top-level variant changes changes the order of its variants" in {
      testPackages(
        Seq("test-common/upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors-v1.dar", "test-common/upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsConstructors-v2.dar"),
        checkTwo(
          Some(
            "The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the variant."
          )
        ),
      )
    }

    "Fails when a top-level variant adds a field to a variant's type" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenATopLevelVariantAddsAFieldToAConstructorsType-v1.dar", "test-common/upgrades-FailsWhenATopLevelVariantAddsAFieldToAConstructorsType-v2.dar"),
        checkTwo(
          Some("The upgraded variant constructor A.Y from variant A has added a field.")
        ),
      )
    }

    "Succeeds when a top-level variant adds an optional field to a variant's type" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType-v1.dar", "test-common/upgrades-SucceedsWhenATopLevelVariantAddsAnOptionalFieldToAConstructorsType-v2.dar"),
        checkTwo(None),
      )
    }

    "Succeeds when a top-level enum changes" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenATopLevelEnumChanges-v1.dar", "test-common/upgrades-SucceedsWhenATopLevelEnumChanges-v2.dar"),
        checkTwo(None),
      )
    }

    "Fail when a top-level enum changes changes the order of its variants" in {
      testPackages(
        Seq("test-common/upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors-v1.dar", "test-common/upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsConstructors-v2.dar"),
        checkTwo(
          Some(
            "The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the enum."
          )
        ),
      )
    }

    "Succeeds when a top-level type synonym changes" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenATopLevelTypeSynonymChanges-v1.dar", "test-common/upgrades-SucceedsWhenATopLevelTypeSynonymChanges-v2.dar"),
        checkTwo(
          None
        ),
      )
    }

    "Succeeds when two deeply nested type synonyms resolve to the same datatypes" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes-v1.dar", "test-common/upgrades-SucceedsWhenTwoDeeplyNestedTypeSynonymsResolveToTheSameDatatypes-v2.dar"),
        checkTwo(
          None
        ),
      )
    }

    "Fails when two deeply nested type synonyms resolve to different datatypes" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes-v1.dar", "test-common/upgrades-FailsWhenTwoDeeplyNestedTypeSynonymsResolveToDifferentDatatypes-v2.dar"),
        checkTwo(
          Some("The upgraded template A has changed the types of some of its original fields.")
        ),
      )
    }

    "Fails when datatype changes variety" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenDatatypeChangesVariety-v1.dar", "test-common/upgrades-FailsWhenDatatypeChangesVariety-v2.dar"),
        checkTwo(
          Some("The upgraded data type RecordToEnum has changed from a record to a enum.")
        ),
      )
    }

    "Succeeds when adding non-optional fields to unserializable types" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes-v1.dar", "test-common/upgrades-SucceedsWhenAddingNonOptionalFieldsToUnserializableTypes-v2.dar"),
        checkTwo(
          None
        ),
      )
    }

    "Succeeds when changing variant of unserializable type" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenChangingConstructorOfUnserializableType-v1.dar", "test-common/upgrades-SucceedsWhenChangingConstructorOfUnserializableType-v2.dar"),
        checkTwo(
          None
        ),
      )
    }

    "Succeeds when deleting unserializable type" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenDeletingUnserializableType-v1.dar", "test-common/upgrades-SucceedsWhenDeletingUnserializableType-v2.dar"),
        checkTwo(
          None
        ),
      )
    }

    "Fails when making type unserializable" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenMakingTypeUnserializable-v1.dar", "test-common/upgrades-FailsWhenMakingTypeUnserializable-v2.dar"),
        checkTwo(
          Some(
            "The upgraded data type MyData was serializable and is now unserializable. Datatypes cannot change their serializability via upgrades."
          )
        ),
      )
    }

    // Copied interface tests
    "Succeeds when an interface is only defined in the initial package." in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage-v1.dar", "test-common/upgrades-SucceedsWhenAnInterfaceIsOnlyDefinedInTheInitialPackage-v2.dar"),
        checkTwo(
          None
        ),
      )
    }

    "Fails when an interface is defined in an upgrading package when it was already in the prior package." in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v1.dar", "test-common/upgrades-FailsWhenAnInterfaceIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v2.dar"),
        checkTwo(
          Some(
            "Tried to upgrade interface I, but interfaces cannot be upgraded. They should be removed in any upgrading package."
          )
        ),
      )
    }

    "Fails when an instance is added (upgraded package)." in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenAnInstanceIsAddedUpgradedPackage-v1.dar", "test-common/upgrades-FailsWhenAnInstanceIsAddedUpgradedPackage-v2.dar"),
        checkTwo(
          Some(
            "Implementation of interface .*:Main:I by template T appears in this package, but does not appear in package that is being upgraded."
          )
        ),
      )
    }

    "Succeeds when an instance is added to a new template (upgraded package)." in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage-v1.dar", "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateUpgradedPackage-v2.dar"),
        checkTwo(
          None
        ),
      )
    }

    "Succeeds when an instance is added to a new template (separate dep)." in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep-v1.dar", "test-common/upgrades-SucceedsWhenAnInstanceIsAddedToNewTemplateSeparateDep-v2.dar"),
        checkTwo(
          None
        ),
      )
    }

    "Succeeds even when non-serializable types are incompatible" in {
      testPackages(
        Seq("test-common/upgrades-SucceedsWhenNonSerializableTypesAreIncompatible-v1.dar", "test-common/upgrades-SucceedsWhenNonSerializableTypesAreIncompatible-v2.dar"),
        checkTwo(
          None
        ),
      )
    }

    "Fails when comparing types from packages with different names" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-v1.dar", "test-common/upgrades-FailsWhenUpgradedFieldFromDifferentPackageName-v2.dar"),
        checkTwo(
          Some("The upgraded data type A has changed the types of some of its original fields.")
        ),
      )
    }

    "Fails when comparing type constructors from other packages that resolve to incompatible types" in {
      testPackages(
        Seq("test-common/upgrades-FailsWhenUpgradedFieldPackagesAreNotUpgradable-v1.dar", "test-common/upgrades-FailsWhenUpgradedFieldPackagesAreNotUpgradable-v2.dar"),
        checkTwo(
          Some("The upgraded data type T has changed the types of some of its original fields.")
        ),
      )
    }

    "FailWhenParamCountChanges" in {
      testPackages(
        Seq("test-common/upgrades-FailWhenParamCountChanges-v1.dar", "test-common/upgrades-FailWhenParamCountChanges-v2.dar"),
        checkTwo(
          Some(
            "The upgraded data type MyStruct has changed the number of type variables it has."
          )
        ),
      )
    }

    "SucceedWhenParamNameChanges" in {
      testPackages(
        Seq("test-common/upgrades-SucceedWhenParamNameChanges-v1.dar", "test-common/upgrades-SucceedWhenParamNameChanges-v2.dar"),
        checkTwo(None),
      )
    }

    "SucceedWhenPhantomParamBecomesUsed" in {
      testPackages(
        Seq("test-common/upgrades-SucceedWhenPhantomParamBecomesUsed-v1.dar", "test-common/upgrades-SucceedWhenPhantomParamBecomesUsed-v2.dar"),
        checkTwo(None),
      )
    }
  }
}
