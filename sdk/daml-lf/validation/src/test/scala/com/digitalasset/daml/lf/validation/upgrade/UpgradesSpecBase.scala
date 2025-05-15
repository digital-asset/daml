// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation
package upgrade

import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonFixture
import com.daml.lf.archive.{DarReader}
import com.daml.lf.data.Ref.PackageId
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
import scala.util.{Failure, Success}

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

    // This test only fails when
    // 1. The precondition requiring that only >=LF1.17 packages get added to
    //    the package map
    // 2. The hash of the v2.dar is greater than the v1.dar, which lets the v2
    //    DAR get checked after the v1 dar.
    // This is very not-ideal because it means that the test only occasionally
    // detects when a regression has occurred, but is the best we can do until
    // we can force the checks to run in a specific but still
    // topologically-valid order.
    //
    // TODO: dylant-da: Make this always fail on regression, some existing work
    // lives in `dylant-da/disallow-interface-lf115-upgrades-explicit-ordering`
    "Succeeds when upgrading an LF1.15 dependency without a shared use site" in {
      testPackagePair(
        "test-common/upgrades-SucceedsWhenUpgradingLF115DepsWithoutSameUseSite-v1.dar",
        "test-common/upgrades-SucceedsWhenUpgradingLF115DepsWithoutSameUseSite-v2.dar",
        assertPackageUpgradeCheck(None),
      )
    }

    "Fails when upgrading an LF1.15 dependency at a use site" in {
      testPackagePair(
        "test-common/upgrades-FailsWhenUpgradingLF115DepsAtUseSite-v1.dar",
        "test-common/upgrades-FailsWhenUpgradingLF115DepsAtUseSite-v2.dar",
        assertPackageUpgradeCheck(
          Some("The upgraded data type MainD has changed the types of some of its original fields.")
        ),
      )
    }
  }

  s"uploading daml-script in upgrade context gives a warning" in {
    for {
      res1 <- uploadPackage("test-common/upgrades-daml-script-dep-lf15-on-lf15.dar")
      res2 <- uploadPackage("test-common/upgrades-daml-script-dep-lf17-on-lf15.dar")
      res3 <- uploadPackage("test-common/upgrades-daml-script-lts-dep-lf15-on-lf15.dar")
      res4 <- uploadPackage("test-common/upgrades-daml-script-lts-dep-lf17-on-lf15.dar")
      res5 <- uploadPackage("test-common/upgrades-daml-script-lts-dep-lf17-on-lf17.dar")
    } yield {
      val cantonLog = Source.fromFile(s"$cantonTmpDir/canton.log").mkString
      def expectedMessageRegex(
          res: (PackageId, Option[Throwable]),
          shouldContain: Boolean,
      ): Assertion = {
        val actual = filterLog(cantonLog, res._1)
        val expected = s"Upload of .*package ${res._1} contains .*daml-script.* as a dependency."
        if (shouldContain)
          actual should include regex (expected)
        else
          actual should not include regex(expected)
        res._2 shouldBe empty
      }

      // Should not warn, neither main or daml-script is LF1.17
      expectedMessageRegex(res1, false)
      // Main package is LF1.17, we should warn
      expectedMessageRegex(res2, true)
      // Should not warn, neither main or daml-script-lts is LF1.17
      expectedMessageRegex(res3, false)
      // Main package is LF1.17, we should warn
      expectedMessageRegex(res4, true)
      // Main package and daml-script-lts is LF1.17, we should warn
      expectedMessageRegex(res5, true)
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
            s"The uploaded DAR contains a package $testPackageSecondId \\(.*\\), but upgrade checks indicate that (existing package $testPackageFirstId|new package $testPackageSecondId) \\(.*\\) cannot be an upgrade of (existing package $testPackageFirstId \\(.*\\)|new package $testPackageSecondId \\(.*\\)). Reason: $additionalInfo".r
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
                s"The uploaded DAR contains a package $testPackageSecondId \\(.*\\), but upgrade checks indicate that (existing package $testPackageFirstId|new package $testPackageSecondId) \\(.*\\) cannot be an upgrade of (existing package|new package)"
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
