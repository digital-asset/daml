// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation
package upgrade

import com.daml.SdkVersion
import com.daml.bazeltools.BazelRunfiles
import com.daml.crypto.MessageDigestPrototype
import com.daml.integrationtest.CantonFixture
import com.daml.lf.archive.{Dar, DarReader, DarWriter}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.LanguageVersion
import com.google.protobuf.ByteString

import org.scalatest.Inside
import org.scalatest.Inspectors.forEvery
import org.scalatest.compatible.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import java.io.FileInputStream

import scala.concurrent.Future
import scala.io.Source
import scala.util.{Success, Failure}

abstract class UpgradesSpecAdminAPI(override val suffix: String) extends UpgradesSpec(suffix) {
  override def uploadPackage(
      entry: (PackageId, ByteString)
  ): Future[(PackageId, Option[Throwable])] = {
    val (pkgId, _) = entry
    val client = AdminLedgerClient.singleHost(
      ledgerPorts(0).adminPort,
      config,
    )
    client
      .uploadDar(entry._2, "-archive-")
      .transform {
        case Failure(err) => Success(pkgId -> Some(err));
        case Success(_) => Success(pkgId -> None);
      }
  }
}

abstract class UpgradesSpecLedgerAPI(override val suffix: String = "Ledger API")
    extends UpgradesSpec(suffix) {

  override def uploadPackage(
      entry: (PackageId, ByteString)
  ): Future[(PackageId, Option[Throwable])] = {
    val (pkgId, archive) = entry
    for {
      client <- defaultLedgerClient()
      uploadResult <- client.packageManagementClient
        .uploadDarFile(archive)
        .transform {
          case Failure(err) => Success(Some(err))
          case Success(_) => Success(None)
        }
    } yield pkgId -> uploadResult
  }
}

trait ShortTests { this: UpgradesSpec =>
  s"Short upload-time Upgradeability Checks ($suffix)" should {
    s"report no upgrade errors for valid upgrade ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-ValidUpgrade-v1.dar",
        "test-common/upgrades-ValidUpgrade-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"report error when module is missing in upgrading package ($suffix)" in {
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
  }
}

trait LongTests { this: UpgradesSpec =>
  s"Long upload-time Upgradeability Checks ($suffix)" should {
    s"uploading the same package multiple times succeeds ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-ValidUpgrade-v1.dar",
        "test-common/upgrades-ValidUpgrade-v1.dar",
        assertDuplicatePackageUpload(),
      )
    }

    s"uploading the standard library twice for two different LF versions succeeds ($suffix)" in {
      for {
        result1 <- uploadPackage("test-common/upgrades-EmptyProject-v117.dar")
        result2 <- uploadPackage("test-common/upgrades-EmptyProject-v1dev.dar")
      } yield {
        // We expect both results to be error-free
        assert(result1._2.isEmpty && result2._2.isEmpty)
      }
    }

    s"uploads against the same package name must be version unique ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-CommonVersionFailure-v1a.dar",
        "test-common/upgrades-CommonVersionFailure-v1b.dar",
        assertPackageUploadVersionFailure(
          "1.0.0"
        ),
      )
    }
    s"report no upgrade errors for valid upgrade ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-ValidUpgrade-v1.dar",
        "test-common/upgrades-ValidUpgrade-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"report no upgrade errors for valid upgrades of parameterized data types ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-ValidParameterizedTypesUpgrade-v1.dar",
        "test-common/upgrades-ValidParameterizedTypesUpgrade-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"report no upgrade errors for alpha-equivalent complex key types ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-ValidKeyTypeEquality-v1.dar",
        "test-common/upgrades-ValidKeyTypeEquality-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"report error when module is missing in upgrading package ($suffix)" in {
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
    s"report error when template is missing in upgrading package ($suffix)" in {
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
    s"allow uploading a package with a missing template but for a different package-name ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-MissingTemplate-v1.dar",
        "test-common/upgrades-MissingTemplateDifferentPackageName.dar",
        assertPackageUpgradeCheckSecondOnly(None),
      )
    }
    s"report error when datatype is missing in upgrading package ($suffix)" in {
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
    s"report error when choice is missing in upgrading package ($suffix)" in {
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
    s"report error when key type changes ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-TemplateChangedKeyType-v1.dar",
        "test-common/upgrades-TemplateChangedKeyType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded template T cannot change its key type.")),
      )
    }
    s"report error when record fields change ($suffix)" in {
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
    s"Fails when template changes key type ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTemplateChangesKeyType-v1.dar",
        "test-common/upgrades-FailsWhenTemplateChangesKeyType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded template A cannot change its key type.")),
      )
    }
    s"Succeeds when template upgrades its key type ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenTemplateUpgradesKeyType-v1.dar",
        "test-common/upgrades-SucceedsWhenTemplateUpgradesKeyType-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"Fails when template removes key type ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v1.dar",
        "test-common/upgrades-FailsWhenTemplateRemovesKeyType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded template A cannot remove its key.")),
      )
    }
    s"Fails when template adds key type ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTemplateAddsKeyType-v1.dar",
        "test-common/upgrades-FailsWhenTemplateAddsKeyType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded template A cannot add a key.")),
      )
    }
    s"Fails when new field is added to template without Optional type ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v1.dar",
        "test-common/upgrades-FailsWhenNewFieldIsAddedToTemplateWithoutOptionalType-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded template A has added new fields, but those fields are not Optional.")
        ),
      )
    }
    s"Fails when old field is deleted from template ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v1.dar",
        "test-common/upgrades-FailsWhenOldFieldIsDeletedFromTemplate-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded template A is missing some of its original fields.")
        ),
      )
    }
    s"Fails when existing field in template is changed ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v1.dar",
        "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded template A has changed the types of some of its original fields.")
        ),
      )
    }
    s"Succeeds when new field with optional type is added to template ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v1.dar",
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplate-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"Fails when new field is added to template choice without Optional type ($suffix)" in {
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
    s"Fails when old field is deleted from template choice ($suffix)" in {
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
    s"Fails when existing field in template choice is changed ($suffix)" in {
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
    s"Fails when template choice changes its return type ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v1.dar",
        "test-common/upgrades-FailsWhenTemplateChoiceChangesItsReturnType-v2.dar",
        assertPackageUpgradeCheck(Some("The upgraded choice C cannot change its return type.")),
      )
    }
    s"Succeeds when template choice returns a template which has changed ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v1.dar",
        "test-common/upgrades-SucceedsWhenTemplateChoiceReturnsATemplateWhichHasChanged-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"Succeeds when template choice input argument has changed ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentHasChanged-v1.dar",
        "test-common/upgrades-SucceedsWhenTemplateChoiceInputArgumentHasChanged-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }
    s"Succeeds when new field with optional type is added to template choice ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v1.dar",
        "test-common/upgrades-SucceedsWhenNewFieldWithOptionalTypeIsAddedToTemplateChoice-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    s"Succeeds when v1 upgrades to v2 and then v3 ($suffix)" in {
      for {
        v1Upload <- uploadPackage("test-common/upgrades-SuccessUpgradingV2ThenV3-v1.dar")
        v2Upload <- uploadPackage("test-common/upgrades-SuccessUpgradingV2ThenV3-v2.dar")
        v3Upload <- uploadPackage("test-common/upgrades-SuccessUpgradingV2ThenV3-v3.dar")
      } yield {
        val cantonLog = Source.fromFile(s"$cantonTmpDir/canton.log")
        try {
          val rawCantonLog = cantonLog.mkString
          forEvery(
            List(
              assertPackageUpgradeCheck(None)(v1Upload, v2Upload)(rawCantonLog),
              assertPackageUpgradeCheck(None)(v2Upload, v3Upload)(rawCantonLog),
            )
          )(identity)
        } finally {
          cantonLog.close()
        }
      }
    }

    s"Succeeds when v1 upgrades to v3 and then v2 ($suffix)" in {
      for {
        v1Upload <- uploadPackage("test-common/upgrades-SuccessUpgradingV3ThenV2-v1.dar")
        v3Upload <- uploadPackage("test-common/upgrades-SuccessUpgradingV3ThenV2-v3.dar")
        v2Upload <- uploadPackage("test-common/upgrades-SuccessUpgradingV3ThenV2-v2.dar")
      } yield {
        val cantonLog = Source.fromFile(s"$cantonTmpDir/canton.log")
        try {
          val rawCantonLog = cantonLog.mkString
          forEvery(
            List(
              assertPackageUpgradeCheck(None)(v1Upload, v3Upload)(rawCantonLog),
              assertPackageUpgradeCheck(None)(v1Upload, v2Upload)(rawCantonLog),
            )
          ) { a => a }
        } finally {
          cantonLog.close()
        }
      }
    }

    s"Fails when v1 upgrades to v2, but v3 does not upgrade v2 ($suffix)" in {
      for {
        v1Upload <- uploadPackage("test-common/upgrades-FailsWhenUpgradingV2ThenV3-v1.dar")
        v2Upload <- uploadPackage("test-common/upgrades-FailsWhenUpgradingV2ThenV3-v2.dar")
        v3Upload <- uploadPackage("test-common/upgrades-FailsWhenUpgradingV2ThenV3-v3.dar")
      } yield {
        val cantonLog = Source.fromFile(s"$cantonTmpDir/canton.log")
        try {
          val rawCantonLog = cantonLog.mkString
          forEvery(
            List(
              assertPackageUpgradeCheck(None)(v1Upload, v2Upload)(rawCantonLog),
              assertPackageUpgradeCheck(
                Some("The upgraded template T is missing some of its original fields.")
              )(v2Upload, v3Upload)(rawCantonLog),
            )
          ) { a => a }
        } finally {
          cantonLog.close()
        }
      }
    }

    s"Fails when v1 upgrades to v3, but v3 does not upgrade v2 ($suffix)" in {
      for {
        v1Upload <- uploadPackage("test-common/upgrades-FailsWhenUpgradingV3ThenV2-v1.dar")
        v3Upload <- uploadPackage("test-common/upgrades-FailsWhenUpgradingV3ThenV2-v3.dar")
        v2Upload <- uploadPackage("test-common/upgrades-FailsWhenUpgradingV3ThenV2-v2.dar")
      } yield {
        val cantonLog = Source.fromFile(s"$cantonTmpDir/canton.log")
        try {
          val rawCantonLog = cantonLog.mkString
          forEvery(
            List(
              assertPackageUpgradeCheck(None)(v1Upload, v3Upload)(rawCantonLog),
              assertPackageUpgradeCheckSecondOnly(
                Some("The upgraded template T is missing some of its original fields.")
              )(v3Upload, v2Upload)(rawCantonLog),
            )
          ) { a => a }
        } finally {
          cantonLog.close()
        }
      }
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

    def mkTrivialPkg(pkgName: String, pkgVersion: String, lfVersion: LanguageVersion) = {
      import com.daml.lf.testing.parser._
      import com.daml.lf.testing.parser.Implicits._
      import com.daml.lf.archive.testing.Encode

      val selfPkgId = PackageId.assertFromString("-self-")

      implicit val parserParameters: ParserParameters[this.type] =
        ParserParameters(
          defaultPackageId = selfPkgId,
          languageVersion = lfVersion,
        )

      val pkg =
        p""" metadata ( '$pkgName' : '$pkgVersion' )
        module Mod {
          record @serializable T = { sig: Party };
          template (this: T) = {
             precondition True;
             signatories Cons @Party [Mod:T {sig} this] (Nil @Party);
             observers Nil @Party;
             agreement "";
          };
        }"""

      val archive = Encode.encodeArchive(selfPkgId -> pkg, lfVersion)
      val pkgId = PackageId.assertFromString(
        MessageDigestPrototype.Sha256.newDigest
          .digest(archive.getPayload.toByteArray)
          .map("%02x" format _)
          .mkString
      )
      val os = ByteString.newOutput()
      DarWriter.encode(
        SdkVersion.sdkVersion,
        Dar(("archive.dalf", archive.toByteArray), List()),
        os,
      )

      pkgId -> os.toByteString
    }

    "report no upgrade errors when the upgrade use a newer version of LF" in {
      testPackagePair(
        mkTrivialPkg("-increasing-lf-version-", "1.0.0", LanguageVersion.v1_17),
        mkTrivialPkg("-increasing-lf-version-", "2.0.0", LanguageVersion.v1_dev),
        assertPackageUpgradeCheck(None),
      )
    }

    "report upgrade errors when the upgrade use a older version of LF" in
      testPackagePair(
        mkTrivialPkg("-decreasing-lf-version-", "1.0.0", LanguageVersion.v1_dev),
        mkTrivialPkg("-decreasing-lf-version-", "2.0.0", LanguageVersion.v1_17),
        assertPackageUpgradeCheck(Some("The upgraded package uses an older LF version")),
      )

    "Succeeds when v2 depends on v2dep which is a valid upgrade of v1dep" in
      testPackagePair(
        "test-common/upgrades-UploadSucceedsWhenDepsAreValidUpgrades-v1.dar",
        "test-common/upgrades-UploadSucceedsWhenDepsAreValidUpgrades-v2.dar",
        assertPackageDependenciesUpgradeCheck(
          "test-common/upgrades-ValidUpgrade-v1.dar",
          "test-common/upgrades-ValidUpgrade-v2.dar",
          None,
        ),
      )

    "report upgrade errors when v2 depends on v2dep which is an invalid upgrade of v1dep" in
      testPackagePair(
        "test-common/upgrades-UploadFailsWhenDepsAreInvalidUpgrades-v1.dar",
        "test-common/upgrades-UploadFailsWhenDepsAreInvalidUpgrades-v2.dar",
        assertPackageDependenciesUpgradeCheck(
          "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v1.dar",
          "test-common/upgrades-FailsWhenExistingFieldInTemplateIsChanged-v2.dar",
          Some("The upgraded template A has changed the types of some of its original fields."),
        ),
      )

    "Fails when a package embeds a previous version of itself it is not a valid upgrade of" in {
      for {
        uploadResult <- uploadPackage(
          "test-common/upgrades-FailsWhenDepIsInvalidPreviousVersionOfSelf-v2.dar"
        )
      } yield {
        // We expect the optional error to be defined. We do not check the error message because the machinery for
        // reading the logs in this class is tailored towards package pairs. But the test below, which is identical to
        // this one except the upgrade is valid, proves that it fails for the expected reason.
        assert(uploadResult._2.isDefined)
      }
    }

    "Succeeds when a package embeds a previous version of itself it is a valid upgrade of" in {
      for {
        uploadResult <- uploadPackage(
          "test-common/upgrades-SucceedsWhenDepIsValidPreviousVersionOfSelf-v2.dar"
        )
      } yield {
        // We expect the optional error to be empty.
        assert(uploadResult._2.isEmpty)
      }
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

    "Fails when an instance is dropped." in {
      for {
        _ <- uploadPackage("test-common/upgrades-FailsWhenAnInstanceIsDropped-dep.dar")
        result <- testPackagePair(
          "test-common/upgrades-FailsWhenAnInstanceIsDropped-v1.dar",
          "test-common/upgrades-FailsWhenAnInstanceIsDropped-v2.dar",
          assertPackageUpgradeCheck(
            Some(
              "Implementation of interface .*:Dep:I by template T appears in package that is being upgraded, but does not appear in this package."
            )
          ),
        )
      } yield result
    }

    "Fails when an instance is added (separate dep)." in {
      for {
        _ <- uploadPackage("test-common/upgrades-FailsWhenAnInstanceIsAddedSeparateDep-dep.dar")
        result <- testPackagePair(
          "test-common/upgrades-FailsWhenAnInstanceIsAddedSeparateDep-v1.dar",
          "test-common/upgrades-FailsWhenAnInstanceIsAddedSeparateDep-v2.dar",
          assertPackageUpgradeCheck(
            Some(
              "Implementation of interface .*:Dep:I by template T appears in this package, but does not appear in package that is being upgraded."
            )
          ),
        )
      } yield result
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

    "Succeeds when an exception is only defined in the initial package." in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenAnExceptionIsOnlyDefinedInTheInitialPackage-v1.dar",
        "test-common/upgrades-SucceedsWhenAnExceptionIsOnlyDefinedInTheInitialPackage-v2.dar",
        assertPackageUpgradeCheck(
          None
        ),
      )
    }

    "Fails when an exception is defined in an upgrading package when it was already in the prior package." in {
      testPackagePair(
        "test-common/upgrades-FailsWhenAnExceptionIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v1.dar",
        "test-common/upgrades-FailsWhenAnExceptionIsDefinedInAnUpgradingPackageWhenItWasAlreadyInThePriorPackage-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "Tried to upgrade exception E, but exceptions cannot be upgraded. They should be removed in any upgrading package."
          )
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

abstract class UpgradesSpec(val suffix: String)
    extends AsyncWordSpec
    with Matchers
    with Inside
    with CantonFixture {
  override lazy val devMode = true;
  override val cantonFixtureDebugMode = CantonFixtureDebugKeepTmpFiles

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

  def uploadPackage(entry: (PackageId, ByteString)): Future[(PackageId, Option[Throwable])]

  def uploadPackage(
      path: String
  ): Future[(PackageId, Option[Throwable])] = {
    val (pkgId, archive) = loadPackageIdAndBS(path)
    uploadPackage(pkgId, archive)
  }

  def assertPackageUpgradeCheckSecondOnly(
      failureMessage: Option[String]
  )(
      uploadedFirst: (PackageId, Option[Throwable]),
      uploadedSecond: (PackageId, Option[Throwable]),
  )(cantonLogSrc: String): Assertion =
    assertPackageUpgradeCheckGeneral(failureMessage)(uploadedFirst, uploadedSecond, false)(
      cantonLogSrc
    )

  def assertPackageUpgradeCheck(failureMessage: Option[String])(
      uploadedFirst: (PackageId, Option[Throwable]),
      uploadedSecond: (PackageId, Option[Throwable]),
  )(cantonLogSrc: String): Assertion =
    assertPackageUpgradeCheckGeneral(failureMessage)(uploadedFirst, uploadedSecond, true)(
      cantonLogSrc
    )

  def assertPackageDependenciesUpgradeCheck(
      v1dep: String,
      v2dep: String,
      failureMessage: Option[String],
  )(
      v1: (PackageId, Option[Throwable]),
      v2: (PackageId, Option[Throwable]),
  )(cantonLogSrc: String): Assertion = {
    val v1depId = loadPackageIdAndBS(v1dep)._1
    val v2depId = loadPackageIdAndBS(v2dep)._1
    assertPackageUpgradeCheckGeneral(failureMessage)((v1depId, v1._2), (v2depId, v2._2), true)(
      cantonLogSrc
    )
  }

  def assertPackageUpgradeCheckGeneral(
      failureMessage: Option[String]
  )(
      uploadedFirst: (PackageId, Option[Throwable]),
      uploadedSecond: (PackageId, Option[Throwable]),
      validateFirstChecked: Boolean = true,
  )(cantonLogSrc: String): Assertion = {
    val (testPackageFirstId, uploadFirstResult) = uploadedFirst
    val (testPackageSecondId, uploadSecondResult) = uploadedSecond
    if (disableUpgradeValidation) {
      filterLog(cantonLogSrc, testPackageFirstId) should include regex (
        s"Skipping upgrade validation for packages .*$testPackageFirstId".r
      )
      filterLog(cantonLogSrc, testPackageSecondId) should include regex (
        s"Skipping upgrade validation for packages .*$testPackageSecondId".r
      )
      filterLog(cantonLogSrc, testPackageSecondId) should not include regex(
        s"The uploaded DAR contains a package $testPackageSecondId \\(.*\\), but upgrade checks indicate that (existing package $testPackageFirstId|new package $testPackageSecondId) \\(.*\\) cannot be an upgrade of (existing package $testPackageFirstId|new package $testPackageSecondId)"
      )
      cantonLogSrc should not include regex(
        s"Typechecking upgrades for $testPackageSecondId \\(.*\\) succeeded."
      )
    } else {
      uploadFirstResult match {
        case Some(err) if validateFirstChecked =>
          fail(s"Uploading first package $testPackageFirstId failed with message: $err");
        case _ => {}
      }

      if (validateFirstChecked) {
        filterLog(cantonLogSrc, testPackageSecondId) should include regex (
          s"Package $testPackageSecondId \\(.*\\) claims to upgrade package id $testPackageFirstId \\(.*\\)"
        )
      }

      failureMessage match {
        // If a failure message is expected, look for it in the canton logs
        case Some(additionalInfo) =>
          if (
            s"The uploaded DAR contains a package $testPackageSecondId \\(.*\\), but upgrade checks indicate that (existing package $testPackageFirstId|new package $testPackageSecondId) \\(.*\\) cannot be an upgrade of (existing package $testPackageFirstId|new package $testPackageSecondId)".r
              .findFirstIn(cantonLogSrc)
              .isEmpty
          ) fail("did not find upgrade failure in canton log:\n")

          uploadSecondResult match {
            case None =>
              fail(s"Uploading second package $testPackageSecondId should fail but didn't.");
            case Some(err) => {
              val msg = err.toString
              msg should include("INVALID_ARGUMENT: DAR_NOT_VALID_UPGRADE")
              msg should include regex (
                s"The uploaded DAR contains a package $testPackageSecondId \\(.*\\), but upgrade checks indicate that (existing package $testPackageFirstId|new package $testPackageSecondId) \\(.*\\) cannot be an upgrade of (existing package $testPackageFirstId|new package $testPackageSecondId).*Reason: $additionalInfo"
              )
            }
          }

        // If a failure is not expected, look for a success message
        case None =>
          filterLog(cantonLogSrc, testPackageSecondId) should include regex (
            s"Typechecking upgrades for $testPackageSecondId \\(.*\\) succeeded."
          )
          uploadSecondResult match {
            case None => succeed;
            case Some(err) =>
              fail(
                s"Uploading second package $testPackageSecondId shouldn't fail but did, with message: $err"
              );
          }
      }
    }
  }

  @scala.annotation.nowarn("cat=unused")
  def assertDuplicatePackageUpload()(
      v1: (PackageId, Option[Throwable]),
      v2: (PackageId, Option[Throwable]),
  )(cantonLogSrc: String): Assertion = {
    val (testPackageV1Id, uploadV1Result) = v1
    val (testPackageV2Id, uploadV2Result) = v2
    uploadV1Result should be(empty)
    filterLog(cantonLogSrc, testPackageV2Id) should include regex (
      s"Ignoring upload of package $testPackageV2Id \\(.*\\) as it has been previously uploaded"
    )
    uploadV2Result should be(empty)
  }

  def assertDontCheckUpload(
      @annotation.unused v1: (PackageId, Option[Throwable]),
      @annotation.unused v2: (PackageId, Option[Throwable]),
  )(@annotation.unused cantonLogSrc: String): Assertion = succeed

  def assertPackageUploadVersionFailure(packageVersion: String)(
      v1: (PackageId, Option[Throwable]),
      v2: (PackageId, Option[Throwable]),
  )(cantonLogSrc: String): Assertion = {
    val (testPackageV1Id, uploadV1Result) = v1
    val (testPackageV2Id, uploadV2Result) = v2
    val _ = packageVersion
    uploadV1Result match {
      case Some(err) =>
        fail(s"Uploading first package $testPackageV1Id failed with message: $err");
      case _ => {}
    }
    cantonLogSrc should include regex (
      s"KNOWN_DAR_VERSION\\(.+,.+\\): Tried to upload package $testPackageV2Id \\(.* v${packageVersion}\\), but a different package $testPackageV1Id with the same name and version has previously been uploaded."
    )
    uploadV2Result match {
      case None =>
        fail(s"Uploading second package $testPackageV2Id should fail but didn't.");
      case Some(err) => {
        val msg = err.toString
        msg should include("INVALID_ARGUMENT: KNOWN_DAR_VERSION")
        msg should include regex (s"KNOWN_DAR_VERSION\\(.+,.+\\): Tried to upload package $testPackageV2Id \\(.* v${packageVersion}\\), but a different package $testPackageV1Id with the same name and version has previously been uploaded.")
      }
    }
  }

  def testPackagePair(
      upgraded: (PackageId, ByteString),
      upgrading: (PackageId, ByteString),
      uploadAssertion: (
          (PackageId, Option[Throwable]),
          (PackageId, Option[Throwable]),
      ) => String => Assertion,
  ): Future[Assertion] = {
    for {
      v1Upload <- uploadPackage(upgraded)
      v2Upload <- uploadPackage(upgrading)
    } yield {
      val cantonLog = Source.fromFile(s"$cantonTmpDir/canton.log")
      try {
        uploadAssertion(v1Upload, v2Upload)(
          cantonLog.mkString
        )
      } finally {
        cantonLog.close()
      }
    }
  }

  def testPackagePair(
      upgraded: String,
      upgrading: String,
      uploadAssertion: (
          (PackageId, Option[Throwable]),
          (PackageId, Option[Throwable]),
      ) => String => Assertion,
  ): Future[Assertion] = {
    val v1Upload = loadPackageIdAndBS(upgraded)
    val v2Upload = loadPackageIdAndBS(upgrading)
    testPackagePair(v1Upload, v2Upload, uploadAssertion)
  }

  def testPackageTriple(
      first: String,
      second: String,
      third: String,
      assertFirstToSecond: (
          (PackageId, Option[Throwable]),
          (PackageId, Option[Throwable]),
      ) => String => Assertion,
      assertSecondToThird: (
          (PackageId, Option[Throwable]),
          (PackageId, Option[Throwable]),
      ) => String => Assertion,
      assertFirstToThird: (
          (PackageId, Option[Throwable]),
          (PackageId, Option[Throwable]),
      ) => String => Assertion,
  ): Future[Assertion] = {
    for {
      firstUpload <- uploadPackage(first)
      secondUpload <- uploadPackage(second)
      thirdUpload <- uploadPackage(third)
    } yield {
      val cantonLog = Source.fromFile(s"$cantonTmpDir/canton.log")
      try {
        val rawCantonLog = cantonLog.mkString
        forEvery(
          List(
            assertFirstToSecond(firstUpload, secondUpload)(rawCantonLog),
            assertSecondToThird(secondUpload, thirdUpload)(rawCantonLog),
            assertFirstToThird(firstUpload, thirdUpload)(rawCantonLog),
          )
        ) { a => a }
      } finally {
        cantonLog.close()
      }
    }
  }

  def filterLog(log: String, str: String): String =
    log.split("\n").view.filter(_.contains(str)).mkString("\n")
}
