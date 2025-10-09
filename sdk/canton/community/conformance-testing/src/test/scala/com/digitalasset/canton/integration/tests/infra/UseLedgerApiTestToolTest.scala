// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.infra

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.integration.plugins.UseLedgerApiTestTool.{
  extractVersionString,
  findAllReleases,
  findMatchingVersions,
  latestVersionFromArtifactory,
  releasesFromArtifactory,
}
import com.digitalasset.canton.version.{ReleaseVersion, ReleaseVersionToProtocolVersions}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.matching.Regex

final class UseLedgerApiTestToolTest extends AnyFlatSpec with Matchers with BaseTest {
  private val versions = Seq(
    "3.0.0-snapshot.20240209.12523.0.v2fa088f9",
    "3.0.0-snapshot.20240212.12541.0.v38aabe2f",
    "3.1.0-snapshot.20240404.13035.0.v35213e4a",
    "3.1.0-snapshot.20240405.13039.0.vc7eec3f0",
    "3.1.1-snapshot.20240407.13039.0.vc7eec3f0",
    "3.1.10-snapshot.20240408.13039.0.vc7eec3f0",
    "3.2.0-ad-hoc.20240827.13957.0.v70727775",
    "3.2.0-snapshot.20240826.13949.0.ve03dfcdf",
    "3.2.0-snapshot.20240828.13964.0.v26cc6ace",
    "3.3.0-snapshot.20250416.15779.0.v6cccc0c4",
    "dev",
  )

  "findAllReleases" should "find the major.minor.patch releases correctly" in {
    findAllReleases(versions) shouldBe Seq("3.0.0", "3.1.0", "3.1.1", "3.1.10", "3.2.0", "3.3.0")
  }

  "findMatchingVersions" should "find and sort (by date) all the versions matching the given release" in {
    findMatchingVersions(versions, "3.1.0") shouldBe Seq(
      "3.1.0-snapshot.20240404.13035.0.v35213e4a",
      "3.1.0-snapshot.20240405.13039.0.vc7eec3f0",
    )

    findMatchingVersions(versions, "3.2.0") shouldBe Seq(
      "3.2.0-snapshot.20240826.13949.0.ve03dfcdf",
      "3.2.0-ad-hoc.20240827.13957.0.v70727775",
      "3.2.0-snapshot.20240828.13964.0.v26cc6ace",
    )
  }

  private def extractVersion(version: String): ReleaseVersion = {
    val versionPattern: Regex = """(\d+)\.(\d+)\.(\d+).*""".r
    version match {
      case versionPattern(major, minor, patch) =>
        ReleaseVersion(major.toInt, minor.toInt, patch.toInt)
      case _ => throw new IllegalArgumentException(s"No version number found in '$version'")
    }
  }

  /*
   TODO(#16458)
   This is a lie because it does not contain all releases. Revisit when Canton 3 is stable and we can
   again get the list of releases via the release notes.
   */
  private val allReleases =
    ReleaseVersionToProtocolVersions.majorMinorToStableProtocolVersions.keys.toSeq.sorted

  /** Return the (major, minor) just before `version`
    */
  private def previous(version: ReleaseVersion): (Int, Int) =
    allReleases.zip(allReleases.tail).collectFirst {
      case (prev, curr) if curr == version.majorMinor => prev
    } match {
      case Some(previous) => previous
      case None => allReleases.last
    }

  "latestVersionFromArtifactory" should "be able to fetch the latest version from artifactory" in {
    val latestToolVersion = latestVersionFromArtifactory(logger)

    latestToolVersion should not be empty
    extractVersion(latestToolVersion).major shouldBe 3
    extractVersion(latestToolVersion).majorMinor shouldBe >=(
      previous(extractVersion(BuildInfo.version))
    )
  }

  // TODO(#28361): Re-enable after cleaning up the unnecessary release test list
  "releasesFromArtifactory" should "be able to fetch the lapitt for each release from artifactory" ignore {
    val releasedToolVersions = releasesFromArtifactory(logger)

    releasedToolVersions should not be empty

    val extractedVersions =
      releasedToolVersions.collect(extractVersionString).map(ReleaseVersion.tryCreate)
    val currentMajorMinor =
      ReleaseVersion(ReleaseVersion.current.major, ReleaseVersion.current.minor, 0)

    val releaseVersions3 = allReleases.filter { case (major, _) => major == 3 }

    extractedVersions
      .filter(_ <= currentMajorMinor)
      .map(_.majorMinor) should contain allElementsOf releaseVersions3.dropRight(1)
  }
}
