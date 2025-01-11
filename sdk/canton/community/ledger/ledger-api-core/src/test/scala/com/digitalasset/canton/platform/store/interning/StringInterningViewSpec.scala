// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.util.Try

class StringInterningViewSpec extends AsyncFlatSpec with Matchers with BaseTest {

  behavior of "StringInterningView"

  it should "provide working cache by extending" in {
    val testee = new StringInterningView(loggerFactory)
    partyAbsent(testee, "p1")
    partyAbsent(testee, "p2")
    partyAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:t:a")
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    synchronizerIdAbsent(testee, "x::synchronizer1")
    synchronizerIdAbsent(testee, "x::synchronizer2")
    packageNameAbsent(testee, "pkg-1")
    packageNameAbsent(testee, "pkg-2")
    packageVersionAbsent(testee, "1.0.0")
    packageVersionAbsent(testee, "2.0.0")

    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2", "22:same:name").iterator,
        templateIds = List("22:t:a", "22:t:b", "22:same:name").iterator,
        synchronizerIds = List("x::synchronizer1", "x::synchronizer2").iterator,
        packageNames = List("pkg-1", "pkg-2").iterator,
        packageVersions = List("1.0.0", "2.0.0").iterator,
      )
    ) shouldBe Vector(
      1 -> "p|p1",
      2 -> "p|p2",
      3 -> "p|22:same:name",
      4 -> "t|22:t:a",
      5 -> "t|22:t:b",
      6 -> "t|22:same:name",
      7 -> "d|x::synchronizer1",
      8 -> "d|x::synchronizer2",
      9 -> "n|pkg-1",
      10 -> "n|pkg-2",
      11 -> "v|1.0.0",
      12 -> "v|2.0.0",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22:same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "22:t:a", 4)
    templatePresent(testee, "22:t:b", 5)
    templatePresent(testee, "22:same:name", 6)
    templateAbsent(testee, "22:unkno:wn")
    synchronizerIdPresent(testee, "x::synchronizer1", 7)
    synchronizerIdPresent(testee, "x::synchronizer2", 8)
    synchronizerIdAbsent(testee, "x::synchronizerunknown")
    packageNamePresent(testee, "pkg-1", 9)
    packageNamePresent(testee, "pkg-2", 10)
    packageNameAbsent(testee, "pkg-unknown")
    packageVersionPresent(testee, "1.0.0", 11)
    packageVersionPresent(testee, "2.0.0", 12)
    packageVersionAbsent(testee, "1.2.3")
  }

  it should "extend working view correctly" in {
    val testee = new StringInterningView(loggerFactory)
    partyAbsent(testee, "p1")
    partyAbsent(testee, "p2")
    partyAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:t:a")
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    synchronizerIdAbsent(testee, "x::synchronizer1")
    synchronizerIdAbsent(testee, "x::synchronizer2")
    packageNameAbsent(testee, "pkg-1")
    packageNameAbsent(testee, "pkg-2")
    packageVersionAbsent(testee, "1.0.0")
    packageVersionAbsent(testee, "2.0.0")
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2", "22:same:name").iterator,
        templateIds = List("22:t:a").iterator,
        synchronizerIds = List("x::synchronizer1", "x::synchronizer2").iterator,
        packageNames = List("pkg-1").iterator,
        packageVersions = List("1.0.0").iterator,
      )
    ) shouldBe Vector(
      1 -> "p|p1",
      2 -> "p|p2",
      3 -> "p|22:same:name",
      4 -> "t|22:t:a",
      5 -> "d|x::synchronizer1",
      6 -> "d|x::synchronizer2",
      7 -> "n|pkg-1",
      8 -> "v|1.0.0",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22:same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "22:t:a", 4)
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:unkno:wn")
    synchronizerIdPresent(testee, "x::synchronizer1", 5)
    synchronizerIdPresent(testee, "x::synchronizer2", 6)
    synchronizerIdAbsent(testee, "x::synchronizerunknown")
    packageNamePresent(testee, "pkg-1", 7)
    packageNameAbsent(testee, "pkg-2")
    packageNameAbsent(testee, "pkg-unknown")
    packageVersionPresent(testee, "1.0.0", 8)
    packageVersionAbsent(testee, "2.0.0")
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2").iterator,
        templateIds = List("22:t:a", "22:t:b", "22:same:name").iterator,
        synchronizerIds = List("x::synchronizer1", "x::synchronizer3").iterator,
        packageNames = List("pkg-1", "pkg-2").iterator,
        packageVersions = List("1.0.0", "2.0.0").iterator,
      )
    ) shouldBe Vector(
      9 -> "t|22:t:b",
      10 -> "t|22:same:name",
      11 -> "d|x::synchronizer3",
      12 -> "n|pkg-2",
      13 -> "v|2.0.0",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22:same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "22:t:a", 4)
    templatePresent(testee, "22:t:b", 9)
    templatePresent(testee, "22:same:name", 10)
    templateAbsent(testee, "22:unkno:wn")
    synchronizerIdPresent(testee, "x::synchronizer1", 5)
    synchronizerIdPresent(testee, "x::synchronizer2", 6)
    synchronizerIdPresent(testee, "x::synchronizer3", 11)
    synchronizerIdAbsent(testee, "x::synchronizerunknown")
    packageNamePresent(testee, "pkg-1", 7)
    packageNamePresent(testee, "pkg-2", 12)
    packageNameAbsent(testee, "pkg-unknown")
    packageVersionPresent(testee, "1.0.0", 8)
    packageVersionPresent(testee, "2.0.0", 13)
  }

  it should "correctly load prefixing entries in the view on `update`" in {
    val testee = new StringInterningView(loggerFactory)
    partyAbsent(testee, "p1")
    partyAbsent(testee, "p2")
    partyAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:t:a")
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    testee
      .update(Some(6)) { (from, to) =>
        from shouldBe 0
        to shouldBe 6
        Future.successful(
          Vector(
            1 -> "p|p1",
            2 -> "p|p2",
            3 -> "p|22:same:name",
            4 -> "t|22:t:a",
            5 -> "t|22:t:b",
            6 -> "t|22:same:name",
            7 -> "n|pkg-1",
            8 -> "n|pkg-2",
            9 -> "v|1.0.0",
          )
        )
      }
      .map { _ =>
        partyPresent(testee, "p1", 1)
        partyPresent(testee, "p2", 2)
        partyPresent(testee, "22:same:name", 3)
        partyAbsent(testee, "unknown")
        templatePresent(testee, "22:t:a", 4)
        templatePresent(testee, "22:t:b", 5)
        templatePresent(testee, "22:same:name", 6)
        templateAbsent(testee, "22:unk:nown")
        packageNamePresent(testee, "pkg-1", 7)
        packageNamePresent(testee, "pkg-2", 8)
        packageNameAbsent(testee, "pkg-unknown")
        packageVersionPresent(testee, "1.0.0", 9)
        packageVersionAbsent(testee, "1.2.3")
      }
  }

  it should "be able to update working view correctly" in {
    val testee = new StringInterningView(loggerFactory)
    partyAbsent(testee, "p1")
    partyAbsent(testee, "p2")
    partyAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:t:a")
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    packageNameAbsent(testee, "pkg-1")
    packageVersionAbsent(testee, "1.0.0")
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2").iterator,
        templateIds = List().iterator,
        synchronizerIds = List("x::synchronizer1").iterator,
        packageNames = List("pkg-1").iterator,
        packageVersions = List("1.0.0").iterator,
      )
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:t:a")
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    packageNamePresent(testee, "pkg-1", 4)
    packageNameAbsent(testee, "pkg-2")
    packageVersionPresent(testee, "1.0.0", 5)
    packageVersionAbsent(testee, "2.0.0")
    testee
      .update(Some(11)) { (from, to) =>
        from shouldBe 5
        to shouldBe 11
        Future.successful(
          Vector(
            6 -> "p|22:same:name",
            7 -> "t|22:t:a",
            8 -> "t|22:t:b",
            9 -> "t|22:same:name",
            10 -> "n|pkg-2",
            11 -> "v|2.0.0",
          )
        )
      }
      .map { _ =>
        partyPresent(testee, "p1", 1)
        partyPresent(testee, "p2", 2)
        packageNamePresent(testee, "pkg-1", 4)
        packageVersionPresent(testee, "1.0.0", 5)
        partyPresent(testee, "22:same:name", 6)
        partyAbsent(testee, "unknown")
        templatePresent(testee, "22:t:a", 7)
        templatePresent(testee, "22:t:b", 8)
        templatePresent(testee, "22:same:name", 9)
        packageNamePresent(testee, "pkg-2", 10)
        packageNameAbsent(testee, "pkg-unknown")
        templateAbsent(testee, "22:unk:nown")
        packageVersionPresent(testee, "2.0.0", 11)
      }
  }

  it should "remove entries if lastStringInterningId is greater than lastId" in {
    val testee = new StringInterningView(loggerFactory)
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2", "22:same:name").iterator,
        templateIds = List("22:t:a", "22:t:b", "22:same:name").iterator,
        synchronizerIds = List("x::synchronizer1", "x::synchronizer2").iterator,
        packageNames = List("pkg-1").iterator,
        packageVersions = List("1.0.0").iterator,
      )
    ) shouldBe Vector(
      1 -> "p|p1",
      2 -> "p|p2",
      3 -> "p|22:same:name",
      4 -> "t|22:t:a",
      5 -> "t|22:t:b",
      6 -> "t|22:same:name",
      7 -> "d|x::synchronizer1",
      8 -> "d|x::synchronizer2",
      9 -> "n|pkg-1",
      10 -> "v|1.0.0",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22:same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "22:t:a", 4)
    templatePresent(testee, "22:t:b", 5)
    templatePresent(testee, "22:same:name", 6)
    templateAbsent(testee, "22:unkno:wn")
    synchronizerIdPresent(testee, "x::synchronizer1", 7)
    synchronizerIdPresent(testee, "x::synchronizer2", 8)
    packageNamePresent(testee, "pkg-1", 9)
    packageVersionPresent(testee, "1.0.0", 10)
    packageNameAbsent(testee, "pkg-unknown")

    testee
      .update(Some(4))((_, _) =>
        fail("should not be called if lastStringInterningId is greater than lastId")
      )
      .map { _ =>
        partyPresent(testee, "p1", 1)
        partyPresent(testee, "p2", 2)
        partyPresent(testee, "22:same:name", 3)
        partyAbsent(testee, "unknown")
        templatePresent(testee, "22:t:a", 4)
        templateAbsent(testee, "22:t:b")
        templateAbsent(testee, "22:same:name")
        templateAbsent(testee, "22:unkno:wn")
        synchronizerIdAbsent(testee, "x::synchronizer1")
        synchronizerIdAbsent(testee, "x::synchronizer2")
        packageNameAbsent(testee, "pkg-1")
        packageNameAbsent(testee, "pkg-2")
        packageNameAbsent(testee, "pkg-unknown")
      }
  }

  private def partyPresent(view: StringInterning, party: String, id: Int) =
    interningEntryPresent(view.party, party, id, Ref.Party.assertFromString)

  private def partyAbsent(view: StringInterning, party: String) =
    interningEntryAbsent(view.party, party, Ref.Party.assertFromString)

  private def templatePresent(view: StringInterning, template: String, id: Int) =
    interningEntryPresent(view.templateId, template, id, Ref.Identifier.assertFromString)

  private def templateAbsent(view: StringInterning, template: String) =
    interningEntryAbsent(view.templateId, template, Ref.Identifier.assertFromString)

  private def synchronizerIdPresent(view: StringInterning, synchronizerId: String, id: Int) =
    interningEntryPresent(view.synchronizerId, synchronizerId, id, SynchronizerId.tryFromString)

  private def synchronizerIdAbsent(view: StringInterning, synchronizerId: String) =
    interningEntryAbsent(view.synchronizerId, synchronizerId, SynchronizerId.tryFromString)

  private def packageNamePresent(view: StringInterning, packageName: String, id: Int) =
    interningEntryPresent(view.packageName, packageName, id, Ref.PackageName.assertFromString)

  private def packageNameAbsent(view: StringInterning, packageName: String) =
    interningEntryAbsent(view.packageName, packageName, Ref.PackageName.assertFromString)

  private def packageVersionPresent(view: StringInterning, pkgVersion: String, id: Int) =
    interningEntryPresent(view.packageVersion, pkgVersion, id, Ref.PackageVersion.assertFromString)

  private def packageVersionAbsent(view: StringInterning, pkgVersion: String) =
    interningEntryAbsent(view.packageVersion, pkgVersion, Ref.PackageVersion.assertFromString)

  private def interningEntryPresent[T](
      interningDomain: StringInterningDomain[T],
      stringValue: String,
      id: Int,
      toTypedValue: String => T,
  ): Assertion = {
    val typedValue = toTypedValue(stringValue)
    interningDomain.internalize(typedValue) shouldBe id
    interningDomain.tryInternalize(typedValue) shouldBe Some(id)
    interningDomain.externalize(id) shouldBe typedValue
    interningDomain.tryExternalize(id) shouldBe Some(typedValue)
    interningDomain.unsafe.internalize(stringValue) shouldBe id
    interningDomain.unsafe.tryInternalize(stringValue) shouldBe Some(id)
    interningDomain.unsafe.externalize(id) shouldBe stringValue
    interningDomain.unsafe.tryExternalize(id) shouldBe Some(stringValue)
  }

  private def interningEntryAbsent[T](
      interningDomain: StringInterningDomain[T],
      stringValue: String,
      toTypedValue: String => T,
  ): Assertion = {
    val typedValue = toTypedValue(stringValue)
    Try(interningDomain.internalize(typedValue)).isFailure shouldBe true
    interningDomain.tryInternalize(typedValue) shouldBe None
    Try(interningDomain.unsafe.internalize(stringValue)).isFailure shouldBe true
    interningDomain.unsafe.tryInternalize(stringValue) shouldBe None
  }
}
