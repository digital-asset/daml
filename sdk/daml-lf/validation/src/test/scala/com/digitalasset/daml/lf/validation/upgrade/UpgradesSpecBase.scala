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
    s"uploads against the same package name must be version unique ($suffix)" in {
      testPackagePair(
        "test-common/upgrades-CommonVersionFailure-v1a.dar",
        "test-common/upgrades-CommonVersionFailure-v1b.dar",
        assertPackageUploadVersionFailure(
          "A DAR with the same version number has previously been uploaded.",
          "1.0.0",
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
              assertPackageUpgradeCheck(
                Some("The upgraded template T is missing some of its original fields.")
              )(v1Upload, v2Upload)(rawCantonLog),
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
        "test-common/upgrades-FailsWhenATopLevelVariantAddsAVariant-v1.dar",
        "test-common/upgrades-FailsWhenATopLevelVariantAddsAVariant-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    "Fails when a top-level variant removes a variant" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenATopLevelVariantRemovesAVariant-v1.dar",
        "test-common/upgrades-FailsWhenATopLevelVariantRemovesAVariant-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "Data type A.Z appears in package that is being upgraded, but does not appear in the upgrading package."
          )
        ),
      )
    }

    "Fail when a top-level variant changes changes the order of its variants" in {
      testPackagePair(
        "test-common/upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsVariants-v1.dar",
        "test-common/upgrades-FailWhenATopLevelVariantChangesChangesTheOrderOfItsVariants-v2.dar",
        assertPackageUpgradeCheck(
          Some(
            "The upgraded data type A has changed the order of its variants - any new variant must be added at the end of the variant."
          )
        ),
      )
    }

    "Fails when a top-level variant adds a field to a variant's type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenATopLevelVariantAddsAFieldToAVariantsType-v1.dar",
        "test-common/upgrades-FailsWhenATopLevelVariantAddsAFieldToAVariantsType-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded variant constructor A.Y from variant A has added a field.")
        ),
      )
    }

    "Succeeds when a top-level variant adds an optional field to a variant's type" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenATopLevelVariantAddsAnOptionalFieldToAVariantsType-v1.dar",
        "test-common/upgrades-FailsWhenATopLevelVariantAddsAnOptionalFieldToAVariantsType-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    "Succeeds when a top-level enum changes" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenATopLevelEnumChanges-v1.dar",
        "test-common/upgrades-FailsWhenATopLevelEnumChanges-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    "Fail when a top-level enum changes changes the order of its variants" in {
      testPackagePair(
        "test-common/upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsVariants-v1.dar",
        "test-common/upgrades-FailWhenATopLevelEnumChangesChangesTheOrderOfItsVariants-v2.dar",
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
        mkTrivialPkg("-increasing-lf-version-", "1.0.0", LanguageVersion.v1_16),
        mkTrivialPkg("-increasing-lf-version-", "2.0.0", LanguageVersion.v1_dev),
        assertPackageUpgradeCheck(None),
      )
    }

    "report upgrade errors when the upgrade use a older version of LF" in
      testPackagePair(
        mkTrivialPkg("-decreasing-lf-version-", "1.0.0", LanguageVersion.v1_dev),
        mkTrivialPkg("-decreasing-lf-version-", "2.0.0", LanguageVersion.v1_16),
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

  def assertPackageUpgradeCheckSecondOnly(failureMessage: Option[String])(
      v1: (PackageId, Option[Throwable]),
      v2: (PackageId, Option[Throwable]),
  )(cantonLogSrc: String): Assertion =
    assertPackageUpgradeCheckGeneral(failureMessage)(v1, v2, false)(cantonLogSrc)

  def assertPackageUpgradeCheck(failureMessage: Option[String])(
      v1: (PackageId, Option[Throwable]),
      v2: (PackageId, Option[Throwable]),
  )(cantonLogSrc: String): Assertion =
    assertPackageUpgradeCheckGeneral(failureMessage)(v1, v2, true)(cantonLogSrc)

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

  def assertPackageUpgradeCheckGeneral(failureMessage: Option[String])(
      v1: (PackageId, Option[Throwable]),
      v2: (PackageId, Option[Throwable]),
      validateV1Checked: Boolean = true,
  )(cantonLogSrc: String): Assertion = {
    val (testPackageV1Id, uploadV1Result) = v1
    val (testPackageV2Id, uploadV2Result) = v2
    if (disableUpgradeValidation) {
      filterLog(cantonLogSrc, testPackageV1Id) should include(
        s"Skipping upgrade validation for package $testPackageV1Id"
      )
      filterLog(cantonLogSrc, testPackageV2Id) should include(
        s"Skipping upgrade validation for package $testPackageV2Id"
      )
    } else {
      uploadV1Result match {
        case Some(err) if validateV1Checked =>
          fail(s"Uploading first package $testPackageV1Id failed with message: $err");
        case _ => {}
      }

      if (validateV1Checked) {
        filterLog(cantonLogSrc, testPackageV2Id) should include(
          s"Package $testPackageV2Id claims to upgrade package id $testPackageV1Id"
        )
      }

      failureMessage match {
        // If a failure message is expected, look for it in the canton logs
        case Some(additionalInfo) =>
          if (
            !cantonLogSrc.contains(
              s"The DAR contains a package which claims to upgrade another package, but basic checks indicate the package is not a valid upgrade err-context:{additionalInfo=$additionalInfo"
            )
          ) fail("did not find upgrade failure in canton log")

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

        // If a failure is not expected, look for a success message
        case None =>
          filterLog(cantonLogSrc, testPackageV2Id) should include(
            s"Typechecking upgrades for $testPackageV2Id succeeded."
          )
          uploadV2Result match {
            case None => succeed;
            case Some(err) =>
              fail(
                s"Uploading second package $testPackageV2Id shouldn't fail but did, with message: $err"
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
    filterLog(cantonLogSrc, testPackageV2Id) should include(
      s"Ignoring upload of package $testPackageV2Id as it has been previously uploaded"
    )
    uploadV2Result should be(empty)
  }

  def assertDontCheckUpload(
      @annotation.unused v1: (PackageId, Option[Throwable]),
      @annotation.unused v2: (PackageId, Option[Throwable]),
  )(@annotation.unused cantonLogSrc: String): Assertion = succeed

  def assertPackageUploadVersionFailure(failureMessage: String, packageVersion: String)(
      v1: (PackageId, Option[Throwable]),
      v2: (PackageId, Option[Throwable]),
  )(cantonLogSrc: String): Assertion = {
    val (testPackageV1Id, uploadV1Result) = v1
    val (testPackageV2Id, uploadV2Result) = v2
    uploadV1Result match {
      case Some(err) =>
        fail(s"Uploading first package $testPackageV1Id failed with message: $err");
      case _ => {}
    }
    cantonLogSrc should include regex (
      s"KNOWN_DAR_VERSION\\(.+,.+\\): A DAR with the same version number has previously been uploaded. err-context:\\{existingPackage=$testPackageV2Id, location=.+, packageVersion=$packageVersion, uploadedPackage=$testPackageV1Id\\}"
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
