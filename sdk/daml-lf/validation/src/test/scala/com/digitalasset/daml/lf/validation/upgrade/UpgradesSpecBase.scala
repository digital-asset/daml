// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation
package upgrade

import com.daml.bazeltools.BazelRunfiles
import com.daml.integrationtest.CantonFixture
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.google.protobuf.ByteString
import org.scalatest.{Assertion, Inside}
import org.scalatest.Inspectors.forEvery
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import java.io.FileInputStream
import scala.concurrent.Future
import scala.util.{Failure, Success}
import com.digitalasset.daml.lf.archive.{Dar, DarReader, DarWriter}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.daml.SdkVersion
import com.daml.crypto.MessageDigestPrototype

abstract class UpgradesSpecAdminAPI(override val suffix: String) extends UpgradesSpec(suffix) {
  override def uploadPackageRaw(
      entry: (PackageId, ByteString),
      dryRun: Boolean,
  ): Future[(PackageId, Option[Throwable])] = {
    val (pkgId, archive) = entry
    val client = AdminLedgerClient.singleHost(
      ledgerPorts(0).adminPort,
      config,
    )
    val result =
      if (dryRun)
        client.validateDar(archive, "-archive-")
      else
        client.uploadDar(archive, "-archive-")
    result.transform {
      case Failure(err) => Success(pkgId -> Some(err))
      case Success(_) => Success(pkgId -> None)
    }
  }
}

class UpgradesSpecLedgerAPI(override val suffix: String = "Ledger API")
    extends UpgradesSpec(suffix) {
  final override def uploadPackageRaw(
      entry: (PackageId, ByteString),
      dryRun: Boolean,
  ): Future[(PackageId, Option[Throwable])] = {
    val (pkgId, archive) = entry
    for {
      client <- defaultLedgerClient()
      uploadResult <-
        (
          if (dryRun)
            client.packageManagementClient.validateDarFile(archive)
          else
            client.packageManagementClient.uploadDarFile(archive)
        ).transform {
          case Failure(err) => Success(Some(err));
          case Success(_) => Success(None);
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
        result1 <- uploadPackage("test-common/upgrades-EmptyProject-v21.dar")
        result2 <- uploadPackage("test-common/upgrades-EmptyProject-v2dev.dar")
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

    s"daml-prim with serializable datatypes can't upload" in {
      for {
        result <- uploadPackageRaw(
          mkTrivialPkg("daml-prim", "0.0.0", LanguageVersion.Features.default, "T1"),
          false,
        )
      } yield {
        val (pkgId, mbErr) = result
        mbErr match {
          case None => fail("daml-prim should not succeed")
          case Some(err) => {
            err.toString should include(
              s"Tried to upload a package $pkgId (daml-prim v0.0.0), but this package is not a utility package. All packages named `daml-prim` must be a utility package."
            )
          }
        }
      }
    }
  }
}

abstract class UpgradesSpec(val suffix: String)
    extends AsyncWordSpec
    with Matchers
    with Inside
    with CantonFixture {
  override lazy val devMode = true
  override val cantonFixtureDebugMode = CantonFixtureDebugRemoveTmpFiles
  val uploadSecondPackageDryRun: Boolean = false;

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

  def uploadPackageRaw(
      entry: (PackageId, ByteString),
      dryRun: Boolean,
  ): Future[(PackageId, Option[Throwable])]

  def uploadPackage(
      path: String,
      dryRun: Boolean = false,
  ): Future[(PackageId, Option[Throwable])] = {
    val (pkgId, archive) = loadPackageIdAndBS(path)
    uploadPackageRaw((pkgId, archive), dryRun)
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
        s"Upgrade checks indicate that (existing package $testPackageFirstId|new package $testPackageSecondId) \\(.*\\) cannot be an upgrade of (existing package $testPackageFirstId|new package $testPackageSecondId)"
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
        cantonLogSrc should include regex (
          s"Package $testPackageSecondId \\(.*\\) claims to upgrade package id $testPackageFirstId \\(.*\\)"
        )
      }

      failureMessage match {
        // If a failure message is expected, look for it in the canton logs
        case Some(additionalInfo) =>
          if (
            s"Upgrade checks indicate that (existing package $testPackageFirstId|new package $testPackageSecondId) \\(.*\\) cannot be an upgrade of (existing package $testPackageFirstId \\(.*\\)|new package $testPackageSecondId \\(.*\\)). Reason: $additionalInfo".r
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
                s"Upgrade checks indicate that (existing package $testPackageFirstId|new package $testPackageSecondId) \\(.*\\) cannot be an upgrade of (existing package|new package)"
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
    uploadV1Result match {
      case Some(err) =>
        fail(s"Uploading first package $testPackageV1Id failed with message: $err");
      case _ => {}
    }
    cantonLogSrc should include regex (
      s"KNOWN_DAR_VERSION\\(.+,.+\\): Tried to upload package $testPackageV2Id \\(.*\\), but a different package $testPackageV1Id with the same name and version has previously been uploaded. err-context:\\{existingPackage=$testPackageV1Id, location=.+, packageVersion=$packageVersion, uploadedPackageId=$testPackageV2Id \\(.*\\)\\}"
    )
    uploadV2Result match {
      case None =>
        fail(s"Uploading second package $testPackageV2Id should fail but didn't.");
      case Some(err) => {
        val msg = err.toString
        msg should include("INVALID_ARGUMENT: KNOWN_DAR_VERSION")
        msg should include regex (s"KNOWN_DAR_VERSION\\(.+,.+\\): Tried to upload package $testPackageV2Id \\(.*\\), but a different package $testPackageV1Id with the same name and version has previously been uploaded.")
      }
    }
  }

  private[this] val cantonLogPath = java.nio.file.Paths.get(s"$cantonTmpDir/canton.log")
  protected def currentLogOffset() = java.nio.file.Files.size(cantonLogPath)

  protected def cantonLog(beginOffset: Long): String = {
    import java.nio._
    val endOffset = currentLogOffset()
    val ch = file.Files.newByteChannel(cantonLogPath)
    try {
      val size = math.toIntExact(endOffset - beginOffset)
      ch.position(beginOffset)
      val buff = ByteBuffer.allocate(size)
      assert(size == ch.read(buff))
      buff.flip()
      charset.StandardCharsets.UTF_8.decode(buff).toString
    } finally ch.close()
  }

  def testPackagePair(
      upgraded: (PackageId, ByteString),
      upgrading: (PackageId, ByteString),
      uploadAssertion: (
          (PackageId, Option[Throwable]),
          (PackageId, Option[Throwable]),
      ) => String => Assertion,
  ): Future[Assertion] = {
    val logOffset = currentLogOffset()
    for {
      v1Upload <- uploadPackageRaw(upgraded, dryRun = false)
      v2Upload <- uploadPackageRaw(upgrading, dryRun = uploadSecondPackageDryRun)
      log = cantonLog(logOffset)
    } yield uploadAssertion(v1Upload, v2Upload)(log)
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
    val offset = currentLogOffset()
    for {
      firstUpload <- uploadPackage(first)
      secondUpload <- uploadPackage(second)
      thirdUpload <- uploadPackage(third)
      rawCantonLog = cantonLog(offset)
    } yield forEvery(
      List(
        assertFirstToSecond(firstUpload, secondUpload)(rawCantonLog),
        assertSecondToThird(secondUpload, thirdUpload)(rawCantonLog),
        assertFirstToThird(firstUpload, thirdUpload)(rawCantonLog),
      )
    ) { a => a }
  }

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

  def mkTrivialPkg(
      pkgName: String,
      pkgVersion: String,
      lfVersion: LanguageVersion,
      templateName: String,
  ): (PackageId, ByteString) = {
    import com.digitalasset.daml.lf.testing.parser._
    import com.digitalasset.daml.lf.testing.parser.Implicits._
    import com.digitalasset.daml.lf.archive.testing.Encode

    val selfPkgId = PackageId.assertFromString("-self-")

    implicit val parserParameters: ParserParameters[this.type] =
      ParserParameters(
        defaultPackageId = selfPkgId,
        languageVersion = lfVersion,
      )

    val pkg =
      p""" metadata ( '$pkgName' : '$pkgVersion' )
      module Mod {
        record @serializable $templateName = { sig: Party };
        template (this: $templateName) = {
           precondition True;
           signatories Cons @Party [Mod:$templateName {sig} this] (Nil @Party);
           observers Nil @Party;
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

  def filterLog(log: String, str: String): String =
    log.split("\n").view.filter(_.contains(str)).mkString("\n")
}
