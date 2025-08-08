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
    partyAbsent(testee, "22::same:name")
    templateAbsent(testee, "#22:t:a")
    templateAbsent(testee, "#22:t:b")
    synchronizerIdAbsent(testee, "x::synchronizer1")
    synchronizerIdAbsent(testee, "x::synchronizer2")
    synchronizerIdAbsent(testee, "22::same:name")
    packageIdAbsent(testee, "pkg-1")
    packageIdAbsent(testee, "pkg-2")

    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2", "22::same:name").iterator,
        templateIds = List("#22:t:a", "#22:t:b").iterator,
        synchronizerIds = List("22::same:name", "x::synchronizer1", "x::synchronizer2").iterator,
        packageIds = List("pkg-1", "pkg-2").iterator,
      )
    ) shouldBe Vector(
      1 -> "p|p1",
      2 -> "p|p2",
      3 -> "p|22::same:name",
      4 -> "t|#22:t:a",
      5 -> "t|#22:t:b",
      6 -> "d|22::same:name",
      7 -> "d|x::synchronizer1",
      8 -> "d|x::synchronizer2",
      9 -> "i|pkg-1",
      10 -> "i|pkg-2",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22::same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "#22:t:a", 4)
    templatePresent(testee, "#22:t:b", 5)
    templateAbsent(testee, "#22:unkno:wn")
    synchronizerIdPresent(testee, "22::same:name", 6)
    synchronizerIdPresent(testee, "x::synchronizer1", 7)
    synchronizerIdPresent(testee, "x::synchronizer2", 8)
    synchronizerIdAbsent(testee, "x::synchronizerunknown")
    packageIdPresent(testee, "pkg-1", 9)
    packageIdPresent(testee, "pkg-2", 10)
    packageIdAbsent(testee, "pkg-unknown")
  }

  it should "extend working view correctly" in {
    val testee = new StringInterningView(loggerFactory)
    partyAbsent(testee, "p1")
    partyAbsent(testee, "p2")
    partyAbsent(testee, "22::same:name")
    templateAbsent(testee, "#22:t:a")
    templateAbsent(testee, "#22:t:b")
    synchronizerIdAbsent(testee, "22::same:name")
    synchronizerIdAbsent(testee, "x::synchronizer1")
    synchronizerIdAbsent(testee, "x::synchronizer2")
    packageIdAbsent(testee, "pkg-1")
    packageIdAbsent(testee, "pkg-2")
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2", "22::same:name").iterator,
        templateIds = List("#22:t:a").iterator,
        synchronizerIds = List("x::synchronizer1", "x::synchronizer2").iterator,
        packageIds = List("pkg-1").iterator,
      )
    ) shouldBe Vector(
      1 -> "p|p1",
      2 -> "p|p2",
      3 -> "p|22::same:name",
      4 -> "t|#22:t:a",
      5 -> "d|x::synchronizer1",
      6 -> "d|x::synchronizer2",
      7 -> "i|pkg-1",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22::same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "#22:t:a", 4)
    templateAbsent(testee, "#22:t:b")
    templateAbsent(testee, "#22:unkno:wn")
    synchronizerIdAbsent(testee, "22::same:name")
    synchronizerIdPresent(testee, "x::synchronizer1", 5)
    synchronizerIdPresent(testee, "x::synchronizer2", 6)
    synchronizerIdAbsent(testee, "x::synchronizerunknown")
    packageIdPresent(testee, "pkg-1", 7)
    packageIdAbsent(testee, "pkg-2")
    packageIdAbsent(testee, "pkg-unknown")
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2").iterator,
        templateIds = List("#22:t:a", "#22:t:b").iterator,
        synchronizerIds = List("22::same:name", "x::synchronizer1", "x::synchronizer3").iterator,
        packageIds = List("pkg-1", "pkg-2").iterator,
      )
    ) shouldBe Vector(
      8 -> "t|#22:t:b",
      9 -> "d|22::same:name",
      10 -> "d|x::synchronizer3",
      11 -> "i|pkg-2",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22::same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "#22:t:a", 4)
    templatePresent(testee, "#22:t:b", 8)
    templateAbsent(testee, "#22:unkno:wn")
    synchronizerIdPresent(testee, "x::synchronizer1", 5)
    synchronizerIdPresent(testee, "x::synchronizer2", 6)
    synchronizerIdPresent(testee, "22::same:name", 9)
    synchronizerIdPresent(testee, "x::synchronizer3", 10)
    synchronizerIdAbsent(testee, "x::synchronizerunknown")
    packageIdPresent(testee, "pkg-1", 7)
    packageIdPresent(testee, "pkg-2", 11)
    packageIdAbsent(testee, "pkg-unknown")
  }

  it should "correctly load prefixing entries in the view on `update`" in {
    val testee = new StringInterningView(loggerFactory)
    partyAbsent(testee, "p1")
    partyAbsent(testee, "p2")
    partyAbsent(testee, "22::same:name")
    synchronizerIdAbsent(testee, "x::synchronizer1")
    synchronizerIdAbsent(testee, "x::synchronizer2")
    synchronizerIdAbsent(testee, "22::same:name")
    testee
      .update(Some(6)) { (from, to) =>
        from shouldBe 0
        to shouldBe 6
        Future.successful(
          Vector(
            1 -> "p|p1",
            2 -> "p|p2",
            3 -> "p|22::same:name",
            4 -> "d|x::synchronizer1",
            5 -> "d|x::synchronizer2",
            6 -> "d|22::same:name",
            7 -> "i|pkg-1",
            8 -> "i|pkg-2",
          )
        )
      }
      .map { _ =>
        partyPresent(testee, "p1", 1)
        partyPresent(testee, "p2", 2)
        partyPresent(testee, "22::same:name", 3)
        partyAbsent(testee, "unknown")
        synchronizerIdPresent(testee, "x::synchronizer1", 4)
        synchronizerIdPresent(testee, "x::synchronizer2", 5)
        synchronizerIdPresent(testee, "22::same:name", 6)
        templateAbsent(testee, "#22:unk:nown")
        packageIdPresent(testee, "pkg-1", 7)
        packageIdPresent(testee, "pkg-2", 8)
        packageIdAbsent(testee, "pkg-unknown")
      }
  }

  it should "be able to update working view correctly" in {
    val testee = new StringInterningView(loggerFactory)
    partyAbsent(testee, "p1")
    partyAbsent(testee, "p2")
    partyAbsent(testee, "22:same:name")
    templateAbsent(testee, "#22:t:a")
    templateAbsent(testee, "#22:t:b")
    templateAbsent(testee, "#22:same:name")
    packageIdAbsent(testee, "pkg-1")
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2").iterator,
        templateIds = List().iterator,
        synchronizerIds = List("x::synchronizer1").iterator,
        packageIds = List("pkg-1").iterator,
      )
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyAbsent(testee, "22:same:name")
    templateAbsent(testee, "#22:t:a")
    templateAbsent(testee, "#22:t:b")
    templateAbsent(testee, "#22:same:name")
    packageIdPresent(testee, "pkg-1", 4)
    packageIdAbsent(testee, "pkg-2")
    testee
      .update(Some(11)) { (from, to) =>
        from shouldBe 4
        to shouldBe 11
        Future.successful(
          Vector(
            6 -> "p|22:same:name",
            7 -> "t|#22:t:a",
            8 -> "t|#22:t:b",
            9 -> "t|#22:same:name",
            10 -> "i|pkg-2",
          )
        )
      }
      .map { _ =>
        partyPresent(testee, "p1", 1)
        partyPresent(testee, "p2", 2)
        packageIdPresent(testee, "pkg-1", 4)
        partyPresent(testee, "22:same:name", 6)
        partyAbsent(testee, "unknown")
        templatePresent(testee, "#22:t:a", 7)
        templatePresent(testee, "#22:t:b", 8)
        templatePresent(testee, "#22:same:name", 9)
        packageIdPresent(testee, "pkg-2", 10)
        packageIdAbsent(testee, "pkg-unknown")
        templateAbsent(testee, "#22:unk:nown")
      }
  }

  it should "remove entries if lastStringInterningId is greater than lastId" in {
    val testee = new StringInterningView(loggerFactory)
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2", "22::same:name").iterator,
        templateIds = List("#22:t:a", "#22:t:b").iterator,
        synchronizerIds = List("22::same:name", "x::synchronizer1", "x::synchronizer2").iterator,
        packageIds = List("pkg-1").iterator,
      )
    ) shouldBe Vector(
      1 -> "p|p1",
      2 -> "p|p2",
      3 -> "p|22::same:name",
      4 -> "t|#22:t:a",
      5 -> "t|#22:t:b",
      6 -> "d|22::same:name",
      7 -> "d|x::synchronizer1",
      8 -> "d|x::synchronizer2",
      9 -> "i|pkg-1",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22::same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "#22:t:a", 4)
    templatePresent(testee, "#22:t:b", 5)
    templateAbsent(testee, "#22:unkno:wn")
    synchronizerIdPresent(testee, "22::same:name", 6)
    synchronizerIdPresent(testee, "x::synchronizer1", 7)
    synchronizerIdPresent(testee, "x::synchronizer2", 8)
    packageIdPresent(testee, "pkg-1", 9)
    packageIdAbsent(testee, "pkg-unknown")

    testee
      .update(Some(4))((_, _) =>
        fail("should not be called if lastStringInterningId is greater than lastId")
      )
      .map { _ =>
        partyPresent(testee, "p1", 1)
        partyPresent(testee, "p2", 2)
        partyPresent(testee, "22::same:name", 3)
        partyAbsent(testee, "unknown")
        templatePresent(testee, "#22:t:a", 4)
        templateAbsent(testee, "#22:t:b")
        templateAbsent(testee, "#22:unkno:wn")
        synchronizerIdAbsent(testee, "22::same:name")
        synchronizerIdAbsent(testee, "x::synchronizer1")
        synchronizerIdAbsent(testee, "x::synchronizer2")
        packageIdAbsent(testee, "pkg-1")
        packageIdAbsent(testee, "pkg-2")
        packageIdAbsent(testee, "pkg-unknown")
      }
  }

  private def partyPresent(view: StringInterning, party: String, id: Int) =
    interningEntryPresent(view.party, party, id, Ref.Party.assertFromString)

  private def partyAbsent(view: StringInterning, party: String) =
    interningEntryAbsent(view.party, party, Ref.Party.assertFromString)

  private def templatePresent(view: StringInterning, template: String, id: Int) =
    interningEntryPresent(view.templateId, template, id, Ref.NameTypeConRef.assertFromString)

  private def templateAbsent(view: StringInterning, template: String) =
    interningEntryAbsent(view.templateId, template, Ref.NameTypeConRef.assertFromString)

  private def synchronizerIdPresent(view: StringInterning, synchronizerId: String, id: Int) =
    interningEntryPresent(view.synchronizerId, synchronizerId, id, SynchronizerId.tryFromString)

  private def synchronizerIdAbsent(view: StringInterning, synchronizerId: String) =
    interningEntryAbsent(view.synchronizerId, synchronizerId, SynchronizerId.tryFromString)

  private def packageIdPresent(view: StringInterning, packageId: String, id: Int) =
    interningEntryPresent(view.packageId, packageId, id, Ref.PackageId.assertFromString)

  private def packageIdAbsent(view: StringInterning, packageId: String) =
    interningEntryAbsent(view.packageId, packageId, Ref.PackageId.assertFromString)

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
