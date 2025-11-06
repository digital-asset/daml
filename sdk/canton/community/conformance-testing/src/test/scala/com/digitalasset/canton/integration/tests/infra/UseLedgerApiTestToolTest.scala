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
    "3.3.0-snapshot.20250416.15779.0.v6cccc0c4",
    "3.3.0-ad-hoc.20250905.16091.0.v704bf59d",
    "3.3.0-snapshot.20251007.16123.0.v670c8fae",
    "3.3.1-snapshot.20251007.16123.0.v670c8fae", // not existing just for testing patches
    "3.3.10-snapshot.20251007.16123.0.v670c8fae", // not existing just for testing patches
    "3.4.0-snapshot.20250429.15866.0.vc8f10812",
    "3.4.0-snapshot.20251003.17075.0.v69d92264",
    "dev",
  )

  "findAllReleases" should "find the major.minor.patch releases correctly" in {
    findAllReleases(versions) shouldBe Seq("3.3.0", "3.3.1", "3.3.10", "3.4.0")
  }

  "findMatchingVersions" should "find and sort (by date) all the versions matching the given release" in {
    findMatchingVersions(versions, "3.3.0") shouldBe Seq(
      "3.3.0-snapshot.20250416.15779.0.v6cccc0c4",
      "3.3.0-ad-hoc.20250905.16091.0.v704bf59d",
      "3.3.0-snapshot.20251007.16123.0.v670c8fae",
    )

    findMatchingVersions(versions, "3.4.0") shouldBe Seq(
      "3.4.0-snapshot.20250429.15866.0.vc8f10812",
      "3.4.0-snapshot.20251003.17075.0.v69d92264",
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

  "releasesFromArtifactory" should "be able to fetch the lapitt for each release from artifactory" in {
    val releasedToolVersions = releasesFromArtifactory(logger)

    releasedToolVersions should not be empty

    val extractedVersions =
      releasedToolVersions.collect(extractVersionString).map(ReleaseVersion.tryCreate)
    val currentMajorMinor =
      ReleaseVersion(ReleaseVersion.current.major, ReleaseVersion.current.minor, 0)

    // We only test releases starting from 3.3.0, as earlier releases are no longer supported
    // TODO(#16458): update the earliest tested release once Canton 3 is stable
    val releases33andLater = allReleases.filter { case (major, minor) =>
      major == 3 && minor >= 3
    }

    extractedVersions
      .filter(_ <= currentMajorMinor)
      .map(_.majorMinor) should contain allElementsOf releases33andLater.dropRight(1)
  }
}
