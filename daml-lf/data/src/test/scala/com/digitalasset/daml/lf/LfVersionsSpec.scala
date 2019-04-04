// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

class LfVersionsSpec extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {

  case class DummyVersion(value: Int) {
    def protoValue: String = value.toString
  }

  class DummyVersions(defaultVersion: DummyVersion, otherVersions: List[DummyVersion])
      extends LfVersions[DummyVersion](defaultVersion, otherVersions)(_.protoValue)

  case class DummyError(msg: String)

  private def dummyVersionGen: Gen[DummyVersion] = arbitrary[Int].map(DummyVersion)

  implicit private val dummyVersionArb: Arbitrary[DummyVersion] = Arbitrary(dummyVersionGen)

  "LfVersions.acceptedVersions" should {
    "be otherVersions + defaultVersion" in forAll {
      (default: DummyVersion, other: List[DummyVersion]) =>
        val versions = new DummyVersions(default, other)
        versions.acceptedVersions shouldBe other :+ default
        (other :+ default).forall(v => versions.acceptedVersions.contains(v)) shouldBe true
    }
  }

  "LfVersions.decodeVersion" should {
    "return failure if passed version value is null, don't throw exception" in {
      val versions = new DummyVersions(DummyVersion(1), List.empty)
      versions.isAcceptedVersion(null) shouldBe None
    }

    "return failure if passed version value is an empty string, don't throw exception" in {
      val versions = new DummyVersions(DummyVersion(1), List.empty)
      versions.isAcceptedVersion("") shouldBe None
    }

    "return failure if passed version is not default and not supported" in forAll {
      (default: DummyVersion, otherVersions: List[DummyVersion], version: DummyVersion) =>
        whenever(default != version && !otherVersions.contains(version)) {
          val versions = new DummyVersions(default, otherVersions)
          versions.acceptedVersions.contains(version) shouldBe false
          versions.isAcceptedVersion(version.protoValue) shouldBe None
        }
    }

    "return success if passed version is default" in forAll { default: DummyVersion =>
      val versions = new DummyVersions(default, List.empty)
      versions.isAcceptedVersion(default.protoValue) shouldBe Some(default)
    }

    "return success if passed version is one of other versions" in forAll {
      (default: DummyVersion, other: List[DummyVersion], version: DummyVersion) =>
        val versions = new DummyVersions(default, other :+ version)
        versions.acceptedVersions.contains(version) shouldBe true
        versions.isAcceptedVersion(version.protoValue) shouldBe Some(version)
    }
  }
}
