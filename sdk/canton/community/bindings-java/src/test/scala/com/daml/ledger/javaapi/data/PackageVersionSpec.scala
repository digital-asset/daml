package com.daml.ledger.javaapi.data

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class PackageVersionSpec extends AnyFlatSpec with Matchers {

  "PackageVersion" should "be parsed correctly from String" in {
    val packageVersion = PackageVersion.unsafeFromString("1.22.333")
    packageVersion.toString shouldBe "1.22.333"
    packageVersion shouldBe new PackageVersion(Array(1, 22, 333))
  }

  "PackageVersion" should "not allow negative or non-integers" in {
    an[IllegalArgumentException] should be thrownBy PackageVersion.unsafeFromString("0.-1")
    an[IllegalArgumentException] should be thrownBy PackageVersion.unsafeFromString("0.beef")
  }

  "PackageVersion" should "be ordered correctly" in {

    val expectedOrderedPackageVersions = Seq(
      // Lowest possible package version
      PackageVersion.unsafeFromString("0"),
      PackageVersion.unsafeFromString("0.1"),
      PackageVersion.unsafeFromString("0.11"),
      PackageVersion.unsafeFromString("1.0"),
      PackageVersion.unsafeFromString("2"),
      PackageVersion.unsafeFromString("10"),
      PackageVersion.unsafeFromString(s"${Int.MaxValue}"),
      PackageVersion.unsafeFromString(s"${Int.MaxValue}.3"),
      PackageVersion.unsafeFromString(s"${Int.MaxValue}." * 23 + "99"),
    )

    Random
      .shuffle(expectedOrderedPackageVersions)
      .sorted should contain theSameElementsInOrderAs expectedOrderedPackageVersions
  }
}
