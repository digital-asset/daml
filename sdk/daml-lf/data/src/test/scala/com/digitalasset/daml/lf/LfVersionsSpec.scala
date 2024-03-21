// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.NonEmptyList
import scalaz.scalacheck.ScalazArbitrary._

class LfVersionsSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  case class DummyVersion(value: Int) {
    def protoValue: String = value.toString
  }

  class DummyVersions(versions: NonEmptyList[DummyVersion])
      extends LfVersions[DummyVersion](versions)(_.protoValue)

  case class DummyError(msg: String)

  private def dummyVersionGen: Gen[DummyVersion] = arbitrary[Int].map(DummyVersion)

  implicit private val dummyVersionArb: Arbitrary[DummyVersion] = Arbitrary(dummyVersionGen)

  "LfVersions.acceptedVersions" should {
    "be otherVersions + defaultVersion" in forAll { vs: NonEmptyList[DummyVersion] =>
      val versions = new DummyVersions(vs)
      versions.acceptedVersions should ===(vs.list.toList)
      vs.list.toList.forall(v => versions.acceptedVersions.contains(v)) shouldBe true
    }
  }

  "LfVersions.decodeVersion" should {
    "return failure if passed version value is null, don't throw exception" in {
      val versions = new DummyVersions(NonEmptyList(DummyVersion(1)))
      versions.isAcceptedVersion(null) shouldBe None
    }

    "return failure if passed version value is an empty string, don't throw exception" in {
      val versions = new DummyVersions(NonEmptyList(DummyVersion(1)))
      versions.isAcceptedVersion("") shouldBe None
    }

    "return failure if passed version is not default and not supported" in forAll {
      (vs: NonEmptyList[DummyVersion], version: DummyVersion) =>
        whenever(!vs.list.toList.contains(version)) {
          val versions = new DummyVersions(vs)
          versions.acceptedVersions.contains(version) shouldBe false
          versions.isAcceptedVersion(version.protoValue) shouldBe None
        }
    }

    "return success if passed version is default" in forAll { default: DummyVersion =>
      val versions = new DummyVersions(NonEmptyList(default))
      versions.isAcceptedVersion(default.protoValue) shouldBe Some(default)
    }

    "return success if passed version is one of other versions" in forAll {
      (vs: NonEmptyList[DummyVersion], version: DummyVersion) =>
        val versions = new DummyVersions(version <:: vs)
        versions.acceptedVersions.contains(version) shouldBe true
        versions.isAcceptedVersion(version.protoValue) shouldBe Some(version)
    }
  }
}
