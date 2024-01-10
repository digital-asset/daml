// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class CantonVersionTest extends AnyWordSpec with BaseTest {
  lazy val v2_1_0_rc1: ReleaseVersion = ReleaseVersion(2, 1, 0, Some("rc1"))
  lazy val v2_0_0_snapshot: ReleaseVersion = ReleaseVersion(2, 0, 0, Some("SNAPSHOT"))

  private def v(rawVersion: String): ReleaseVersion = ReleaseVersion.create(rawVersion).value
  "CantonVersion" should {
    "parse version string if valid" in {
      ReleaseVersion.create("5.1.3").value shouldBe new ReleaseVersion(5, 1, 3)
      ReleaseVersion.create("1.43.3-SNAPSHOT").value shouldBe new ReleaseVersion(
        1,
        43,
        3,
        Some("SNAPSHOT"),
      )
      ReleaseVersion.create("1.43.3-rc").value shouldBe new ReleaseVersion(
        1,
        43,
        3,
        Some("rc"),
      )
      ReleaseVersion.create("1.43.3-rc9").value shouldBe new ReleaseVersion(
        1,
        43,
        3,
        Some("rc9"),
      )
      ReleaseVersion.create("1.1.1-SNAPSHT").value shouldBe new ReleaseVersion(
        1,
        1,
        1,
        Some("SNAPSHT"),
      )
      ReleaseVersion.create("1.1.1-rc10").value shouldBe new ReleaseVersion(
        1,
        1,
        1,
        Some("rc10"),
      )
      ReleaseVersion.create("1.1.1-rc0").value shouldBe new ReleaseVersion(
        1,
        1,
        1,
        Some("rc0"),
      )
      ReleaseVersion.create("1.1.1-").value shouldBe new ReleaseVersion(1, 1, 1, Some(""))
      ReleaseVersion.create("1.1.1-SNAPSHOT-rc").value shouldBe new ReleaseVersion(
        1,
        1,
        1,
        Some("SNAPSHOT-rc"),
      )
      ReleaseVersion.create("2.0.0-SNAPSHOT").value shouldBe v2_0_0_snapshot
      ReleaseVersion.create("2.1.0-rc1").value shouldBe v2_1_0_rc1

      ReleaseVersion.create("1").left.value shouldBe a[String]
      ReleaseVersion.create("1.0.-1").left.value shouldBe a[String]
      ReleaseVersion.create("1.1.10000").left.value shouldBe a[String]
      ReleaseVersion.create("foo.bar").left.value shouldBe a[String]
      ReleaseVersion.create("1.1.").left.value shouldBe a[String]
      ReleaseVersion.create("1.1.0snapshot").left.value shouldBe a[String]
      ReleaseVersion.create("1.1.0.rc").left.value shouldBe a[String]
      ReleaseVersion.create("1.1.0.1").left.value shouldBe a[String]
    }

    "should prove that releases are correctly compared" in {

      v("1.2.3") == v("1.2.3") shouldBe true
      v("1.2.3") > v("1.2.0") shouldBe true
      v("1.2.3") > v("1.0.0") shouldBe true

      v("1.0.0-rc") == v("1.0.0") shouldBe false
      v("1.0.0-rc") == v("1.0.0-ia") shouldBe false
      v("1.0.0-a") < v("1.0.0-b") shouldBe true

      v("1.0.0") > v("1.0.0-b") shouldBe true
      v("1.0.0-1") < v("1.0.0-b") shouldBe true
      v("1.0.0-1") < v("1.0.0-2") shouldBe true
      v("1.0.0-a.b.c") < v("1.0.0-a.b.d") shouldBe true
      v("1.0.0-a.b.c.e") < v("1.0.0-a.b.d") shouldBe true

      // examples given in SemVer specification
      v("1.0.0-alpha") < v("1.0.0-alpha.1") shouldBe true
      v("1.0.0-alpha.1") < v("1.0.0-alpha.beta") shouldBe true
      v("1.0.0-alpha.beta") < v("1.0.0-beta") shouldBe true
      v("1.0.0-beta") < v("1.0.0-beta.2") shouldBe true
      v("1.0.0-beta.2") < v("1.0.0-beta.11") shouldBe true
      v("1.0.0-rc.1") < v("1.0.0") shouldBe true

      v("1.0.0-rc") > v("1.0.0-ia") shouldBe true
      v("1.0.0-rc-SNAPSHOT") == v("1.0.0-ia-SNAPSHOT") shouldBe false

      v("1.0.0-SNAPSHOT") < v("1.0.0") shouldBe true
      v("1.1.0-SNAPSHOT") > v("1.0.0") shouldBe true
      v("1.0.1-SNAPSHOT") > v("1.0.0") shouldBe true

    }
  }
}
