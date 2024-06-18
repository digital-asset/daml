// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion.unsupportedErrorMessage
import org.scalatest.wordspec.AnyWordSpec

class ProtocolVersionTest extends AnyWordSpec with BaseTest {
  "ProtocolVersion" should {
    "refuse release versions which are not protocol versions" in {
      ProtocolVersion.create("5.1.3").left.value shouldBe a[String]
      ProtocolVersion.create("5.1.0").left.value shouldBe a[String]
      ProtocolVersion.create("1.43.3-SNAPSHOT").left.value shouldBe a[String]
      ProtocolVersion.create("1.43.3-rc").left.value shouldBe a[String]
      ProtocolVersion.create("1.43.3-rc9").left.value shouldBe a[String]
    }

    "parse version string if valid" in {
      // Old semver format
      ProtocolVersion.create("5.0.0").value shouldBe ProtocolVersion.v5

      // New format
      ProtocolVersion.create("5").value shouldBe ProtocolVersion.v5

      ProtocolVersion
        .create(Int.MaxValue.toString)
        .value shouldBe ProtocolVersion.dev

      ProtocolVersion
        .create(s"${Int.MaxValue.toString}.0.0")
        .value shouldBe ProtocolVersion.dev

      ProtocolVersion.create("DeV").value shouldBe ProtocolVersion.dev
    }

    "be comparable" in {
      ProtocolVersion.v5 < ProtocolVersion.v6 shouldBe true
      ProtocolVersion.v5 <= ProtocolVersion.v6 shouldBe true
      ProtocolVersion.v6 <= ProtocolVersion.v6 shouldBe true

      ProtocolVersion.v6 < ProtocolVersion.v5 shouldBe false
      ProtocolVersion.v6 <= ProtocolVersion.v5 shouldBe false

      ProtocolVersion.v6 <= ProtocolVersion.dev shouldBe true
      ProtocolVersion.v6 < ProtocolVersion.dev shouldBe true
      ProtocolVersion.dev <= ProtocolVersion.v6 shouldBe false

      ProtocolVersion.v6 == ProtocolVersion.v6 shouldBe true
      ProtocolVersion.v6 == ProtocolVersion.v5 shouldBe false
    }

    val invalidProtocolVersionNumber = Int.MinValue
    val invalidProtocolVersion = ProtocolVersion(invalidProtocolVersionNumber)

    "parse version string with create" in {
      ProtocolVersion.supported.foreach { supported =>
        ProtocolVersion.create(supported.toString).value shouldBe supported
      }
    }

    "fail parsing version string with create" in {
      ProtocolVersion.create(invalidProtocolVersionNumber.toString).left.value should be(
        unsupportedErrorMessage(invalidProtocolVersion)
      )
    }

    "fail parsing version string considering also deleted protocol versions with create" in {
      ProtocolVersion
        .create(invalidProtocolVersionNumber.toString, allowDeleted = true)
        .left
        .value should be(
        unsupportedErrorMessage(invalidProtocolVersion, includeDeleted = true)
      )
    }

    "parse version string with tryCreate" in {
      ProtocolVersion.supported.foreach { supported =>
        ProtocolVersion.tryCreate(supported.toString) shouldBe supported
      }
    }

    "fail parsing version string with tryCreate" in {
      the[RuntimeException] thrownBy {
        ProtocolVersion.tryCreate(invalidProtocolVersionNumber.toString)
      } should have message unsupportedErrorMessage(invalidProtocolVersion)
    }

    "parse version string with fromProtoPrimitiveS" in {
      ProtocolVersion.supported.foreach { supported =>
        val result = ProtocolVersion.fromProtoPrimitiveS(supported.toString)
        result shouldBe a[ParsingResult[?]]
        result.value shouldBe supported
      }
    }

    "fail parsing version string with fromProtoPrimitiveS" in {
      val result = ProtocolVersion.fromProtoPrimitiveS(invalidProtocolVersionNumber.toString)
      result shouldBe a[ParsingResult[?]]
      result.left.value should have message unsupportedErrorMessage(invalidProtocolVersion)
    }

    "parse version string with fromProtoPrimitive" in {
      ProtocolVersion.supported.foreach { supported =>
        val result = ProtocolVersion.fromProtoPrimitive(supported.toProtoPrimitive)
        result shouldBe a[ParsingResult[?]]
        result.value shouldBe supported
      }
    }

    "fail parsing version string fromProtoPrimitive" in {
      val result = ProtocolVersion.fromProtoPrimitive(invalidProtocolVersionNumber)
      result shouldBe a[ParsingResult[?]]
      result.left.value should have message unsupportedErrorMessage(invalidProtocolVersion)
    }

    "assert protocol versions of a release do not contain deleted protocol versions" in {
      ProtocolVersion.stable
    }
  }
}
