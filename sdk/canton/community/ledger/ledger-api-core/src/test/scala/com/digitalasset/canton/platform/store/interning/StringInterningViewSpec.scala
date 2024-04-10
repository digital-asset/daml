// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.daml.lf.data.Ref
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.DomainId
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
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2", "22:same:name").iterator,
        templateIds = List("22:t:a", "22:t:b", "22:same:name").iterator,
        domainIds = List("x::domain1", "x::domain2").iterator,
        packageNames = List("pkg-1", "pkg-2").iterator,
      )
    ) shouldBe Vector(
      1 -> "p|p1",
      2 -> "p|p2",
      3 -> "p|22:same:name",
      4 -> "t|22:t:a",
      5 -> "t|22:t:b",
      6 -> "t|22:same:name",
      7 -> "d|x::domain1",
      8 -> "d|x::domain2",
      9 -> "n|pkg-1",
      10 -> "n|pkg-2",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22:same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "22:t:a", 4)
    templatePresent(testee, "22:t:b", 5)
    templatePresent(testee, "22:same:name", 6)
    templateAbsent(testee, "22:unkno:wn")
    domainIdPresent(testee, "x::domain1", 7)
    domainIdPresent(testee, "x::domain2", 8)
    domainIdAbsent(testee, "x::domainunknown")
    packageNamePresent(testee, "pkg-1", 9)
    packageNamePresent(testee, "pkg-2", 10)
    packageNameAbsent(testee, "pkg-unknown")
  }

  it should "extend working view correctly" in {
    val testee = new StringInterningView(loggerFactory)
    partyAbsent(testee, "p1")
    partyAbsent(testee, "p2")
    partyAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:t:a")
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    domainIdAbsent(testee, "x::domain1")
    domainIdAbsent(testee, "x::domain2")
    packageNameAbsent(testee, "pkg-1")
    packageNameAbsent(testee, "pkg-2")
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2", "22:same:name").iterator,
        templateIds = List("22:t:a").iterator,
        domainIds = List("x::domain1", "x::domain2").iterator,
        packageNames = List("pkg-1").iterator,
      )
    ) shouldBe Vector(
      1 -> "p|p1",
      2 -> "p|p2",
      3 -> "p|22:same:name",
      4 -> "t|22:t:a",
      5 -> "d|x::domain1",
      6 -> "d|x::domain2",
      7 -> "n|pkg-1",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22:same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "22:t:a", 4)
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:unkno:wn")
    domainIdPresent(testee, "x::domain1", 5)
    domainIdPresent(testee, "x::domain2", 6)
    domainIdAbsent(testee, "x::domainunknown")
    packageNamePresent(testee, "pkg-1", 7)
    packageNameAbsent(testee, "pkg-2")
    packageNameAbsent(testee, "pkg-unknown")
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2").iterator,
        templateIds = List("22:t:a", "22:t:b", "22:same:name").iterator,
        domainIds = List("x::domain1", "x::domain3").iterator,
        packageNames = List("pkg-1", "pkg-2").iterator,
      )
    ) shouldBe Vector(
      8 -> "t|22:t:b",
      9 -> "t|22:same:name",
      10 -> "d|x::domain3",
      11 -> "n|pkg-2",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22:same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "22:t:a", 4)
    templatePresent(testee, "22:t:b", 8)
    templatePresent(testee, "22:same:name", 9)
    templateAbsent(testee, "22:unkno:wn")
    domainIdPresent(testee, "x::domain1", 5)
    domainIdPresent(testee, "x::domain2", 6)
    domainIdPresent(testee, "x::domain3", 10)
    domainIdAbsent(testee, "x::domainunknown")
    packageNamePresent(testee, "pkg-1", 7)
    packageNamePresent(testee, "pkg-2", 11)
    packageNameAbsent(testee, "pkg-unknown")
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
      .update(6)((from, to) => {
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
          )
        )
      })
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
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2").iterator,
        templateIds = List().iterator,
        domainIds = List("x::domain1").iterator,
        packageNames = List("pkg-1").iterator,
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

    testee
      .update(8)((from, to) => {
        from shouldBe 4
        to shouldBe 8
        Future.successful(
          Vector(
            5 -> "p|22:same:name",
            6 -> "t|22:t:a",
            7 -> "t|22:t:b",
            8 -> "t|22:same:name",
            9 -> "n|pkg-2",
          )
        )
      })
      .map { _ =>
        partyPresent(testee, "p1", 1)
        partyPresent(testee, "p2", 2)
        packageNamePresent(testee, "pkg-1", 4)
        partyPresent(testee, "22:same:name", 5)
        partyAbsent(testee, "unknown")
        templatePresent(testee, "22:t:a", 6)
        templatePresent(testee, "22:t:b", 7)
        templatePresent(testee, "22:same:name", 8)
        packageNamePresent(testee, "pkg-2", 9)
        packageNameAbsent(testee, "pkg-unknown")
        templateAbsent(testee, "22:unk:nown")
      }
  }

  it should "remove entries if lastStringInterningId is greater than lastId" in {
    val testee = new StringInterningView(loggerFactory)
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2", "22:same:name").iterator,
        templateIds = List("22:t:a", "22:t:b", "22:same:name").iterator,
        domainIds = List("x::domain1", "x::domain2").iterator,
        packageNames = List("pkg-1").iterator,
      )
    ) shouldBe Vector(
      1 -> "p|p1",
      2 -> "p|p2",
      3 -> "p|22:same:name",
      4 -> "t|22:t:a",
      5 -> "t|22:t:b",
      6 -> "t|22:same:name",
      7 -> "d|x::domain1",
      8 -> "d|x::domain2",
      9 -> "n|pkg-1",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22:same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "22:t:a", 4)
    templatePresent(testee, "22:t:b", 5)
    templatePresent(testee, "22:same:name", 6)
    templateAbsent(testee, "22:unkno:wn")
    domainIdPresent(testee, "x::domain1", 7)
    domainIdPresent(testee, "x::domain2", 8)
    packageNamePresent(testee, "pkg-1", 9)
    packageNameAbsent(testee, "pkg-unknown")

    testee
      .update(4)((from, to) => {
        from shouldBe 2
        to shouldBe 6
        Future.successful(
          Vector(
            3 -> "p|22:same:name",
            4 -> "t|22:t:a",
            5 -> "t|22:t:b",
            6 -> "t|22:same:name",
            7 -> "n|pkg-2",
          )
        )
      })
      .map { _ =>
        partyPresent(testee, "p1", 1)
        partyPresent(testee, "p2", 2)
        partyPresent(testee, "22:same:name", 3)
        partyAbsent(testee, "unknown")
        templatePresent(testee, "22:t:a", 4)
        templateAbsent(testee, "22:t:b")
        templateAbsent(testee, "22:same:name")
        templateAbsent(testee, "22:unkno:wn")
        domainIdAbsent(testee, "x::domain1")
        domainIdAbsent(testee, "x::domain2")
        packageNameAbsent(testee, "pkg-1")
        packageNameAbsent(testee, "pkg-2")
        packageNameAbsent(testee, "pkg-unknown")
      }
  }

  private def partyPresent(view: StringInterning, party: String, id: Int) = {
    val typedParty = Ref.Party.assertFromString(party)
    view.party.internalize(typedParty) shouldBe id
    view.party.tryInternalize(typedParty) shouldBe Some(id)
    view.party.externalize(id) shouldBe typedParty
    view.party.tryExternalize(id) shouldBe Some(typedParty)
    view.party.unsafe.internalize(party) shouldBe id
    view.party.unsafe.tryInternalize(party) shouldBe Some(id)
    view.party.unsafe.externalize(id) shouldBe party
    view.party.unsafe.tryExternalize(id) shouldBe Some(party)
  }

  private def partyAbsent(view: StringInterning, party: String) = {
    val typedParty = Ref.Party.assertFromString(party)
    Try(view.party.internalize(typedParty)).isFailure shouldBe true
    view.party.tryInternalize(typedParty) shouldBe None
    Try(view.party.unsafe.internalize(party)).isFailure shouldBe true
    view.party.unsafe.tryInternalize(party) shouldBe None
  }

  private def templatePresent(view: StringInterning, template: String, id: Int) = {
    val typedTemplate = Ref.Identifier.assertFromString(template)
    view.templateId.internalize(typedTemplate) shouldBe id
    view.templateId.tryInternalize(typedTemplate) shouldBe Some(id)
    view.templateId.externalize(id) shouldBe typedTemplate
    view.templateId.tryExternalize(id) shouldBe Some(typedTemplate)
    view.templateId.unsafe.internalize(template) shouldBe id
    view.templateId.unsafe.tryInternalize(template) shouldBe Some(id)
    view.templateId.unsafe.externalize(id) shouldBe template
    view.templateId.unsafe.tryExternalize(id) shouldBe Some(template)
  }

  private def templateAbsent(view: StringInterning, template: String) = {
    val typedTemplate = Ref.Identifier.assertFromString(template)
    Try(view.templateId.internalize(typedTemplate)).isFailure shouldBe true
    view.templateId.tryInternalize(typedTemplate) shouldBe None
    Try(view.templateId.unsafe.internalize(template)).isFailure shouldBe true
    view.templateId.unsafe.tryInternalize(template) shouldBe None
  }

  private def domainIdPresent(view: StringInterning, domainId: String, id: Int) = {
    val typedDomainId = DomainId.tryFromString(domainId)
    view.domainId.internalize(typedDomainId) shouldBe id
    view.domainId.tryInternalize(typedDomainId) shouldBe Some(id)
    view.domainId.externalize(id) shouldBe typedDomainId
    view.domainId.tryExternalize(id) shouldBe Some(typedDomainId)
    view.domainId.unsafe.internalize(domainId) shouldBe id
    view.domainId.unsafe.tryInternalize(domainId) shouldBe Some(id)
    view.domainId.unsafe.externalize(id) shouldBe domainId
    view.domainId.unsafe.tryExternalize(id) shouldBe Some(domainId)
  }

  private def domainIdAbsent(view: StringInterning, domainId: String) = {
    val typedDomainId = DomainId.tryFromString(domainId)
    Try(view.domainId.internalize(typedDomainId)).isFailure shouldBe true
    view.domainId.tryInternalize(typedDomainId) shouldBe None
    Try(view.domainId.unsafe.internalize(domainId)).isFailure shouldBe true
    view.domainId.unsafe.tryInternalize(domainId) shouldBe None
  }

  private def packageNamePresent(view: StringInterning, packageName: String, id: Int) = {
    val typedPackageName = Ref.PackageName.assertFromString(packageName)
    view.packageName.internalize(typedPackageName) shouldBe id
    view.packageName.tryInternalize(typedPackageName) shouldBe Some(id)
    view.packageName.externalize(id) shouldBe typedPackageName
    view.packageName.tryExternalize(id) shouldBe Some(typedPackageName)
    view.packageName.unsafe.internalize(packageName) shouldBe id
    view.packageName.unsafe.tryInternalize(packageName) shouldBe Some(id)
    view.packageName.unsafe.externalize(id) shouldBe packageName
    view.packageName.unsafe.tryExternalize(id) shouldBe Some(packageName)
  }

  private def packageNameAbsent(view: StringInterning, packageName: String) = {
    val typedPackageName = Ref.PackageName.assertFromString(packageName)
    Try(view.packageName.internalize(typedPackageName)).isFailure shouldBe true
    view.packageName.tryInternalize(typedPackageName) shouldBe None
    Try(view.packageName.unsafe.internalize(packageName)).isFailure shouldBe true
    view.packageName.unsafe.tryInternalize(packageName) shouldBe None
  }
}
