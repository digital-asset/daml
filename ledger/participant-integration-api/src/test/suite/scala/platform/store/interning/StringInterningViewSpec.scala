// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.interning

import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.util.Try

class StringInterningViewSpec extends AsyncFlatSpec with Matchers {
  private implicit val lc: LoggingContext = LoggingContext.ForTesting

  behavior of "StringInterningView"

  it should "provide working cache by extending" in {
    val testee = new StringInterningView()
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
      )
    ) shouldBe Vector(
      1 -> "p|p1",
      2 -> "p|p2",
      3 -> "p|22:same:name",
      4 -> "t|22:t:a",
      5 -> "t|22:t:b",
      6 -> "t|22:same:name",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22:same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "22:t:a", 4)
    templatePresent(testee, "22:t:b", 5)
    templatePresent(testee, "22:same:name", 6)
    templateAbsent(testee, "22:unkno:wn")
  }

  it should "extend working view correctly" in {
    val testee = new StringInterningView()
    partyAbsent(testee, "p1")
    partyAbsent(testee, "p2")
    partyAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:t:a")
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2", "22:same:name").iterator,
        templateIds = List("22:t:a").iterator,
      )
    ) shouldBe Vector(
      1 -> "p|p1",
      2 -> "p|p2",
      3 -> "p|22:same:name",
      4 -> "t|22:t:a",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22:same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "22:t:a", 4)
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:unkno:wn")
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2").iterator,
        templateIds = List("22:t:a", "22:t:b", "22:same:name").iterator,
      )
    ) shouldBe Vector(
      5 -> "t|22:t:b",
      6 -> "t|22:same:name",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22:same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "22:t:a", 4)
    templatePresent(testee, "22:t:b", 5)
    templatePresent(testee, "22:same:name", 6)
    templateAbsent(testee, "22:unkno:wn")
  }

  it should "correctly load prefixing entries in the view on `update`" in {
    val testee = new StringInterningView()
    partyAbsent(testee, "p1")
    partyAbsent(testee, "p2")
    partyAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:t:a")
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    testee
      .update(6)((from, to) =>
        _ => {
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
            )
          )
        }
      )
      .map { _ =>
        partyPresent(testee, "p1", 1)
        partyPresent(testee, "p2", 2)
        partyPresent(testee, "22:same:name", 3)
        partyAbsent(testee, "unknown")
        templatePresent(testee, "22:t:a", 4)
        templatePresent(testee, "22:t:b", 5)
        templatePresent(testee, "22:same:name", 6)
        templateAbsent(testee, "22:unk:nown")
      }
  }

  it should "be able to update working view correctly" in {
    val testee = new StringInterningView()
    partyAbsent(testee, "p1")
    partyAbsent(testee, "p2")
    partyAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:t:a")
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2").iterator,
        templateIds = List().iterator,
      )
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyAbsent(testee, "22:same:name")
    templateAbsent(testee, "22:t:a")
    templateAbsent(testee, "22:t:b")
    templateAbsent(testee, "22:same:name")
    testee
      .update(6)((from, to) =>
        _ => {
          from shouldBe 2
          to shouldBe 6
          Future.successful(
            Vector(
              3 -> "p|22:same:name",
              4 -> "t|22:t:a",
              5 -> "t|22:t:b",
              6 -> "t|22:same:name",
            )
          )
        }
      )
      .map { _ =>
        partyPresent(testee, "p1", 1)
        partyPresent(testee, "p2", 2)
        partyPresent(testee, "22:same:name", 3)
        partyAbsent(testee, "unknown")
        templatePresent(testee, "22:t:a", 4)
        templatePresent(testee, "22:t:b", 5)
        templatePresent(testee, "22:same:name", 6)
        templateAbsent(testee, "22:unk:nown")
      }
  }

  it should "remove entries if lastStringInterningId is greater than lastId" in {
    val testee = new StringInterningView()
    testee.internize(
      new DomainStringIterators(
        parties = List("p1", "p2", "22:same:name").iterator,
        templateIds = List("22:t:a", "22:t:b", "22:same:name").iterator,
      )
    ) shouldBe Vector(
      1 -> "p|p1",
      2 -> "p|p2",
      3 -> "p|22:same:name",
      4 -> "t|22:t:a",
      5 -> "t|22:t:b",
      6 -> "t|22:same:name",
    )
    partyPresent(testee, "p1", 1)
    partyPresent(testee, "p2", 2)
    partyPresent(testee, "22:same:name", 3)
    partyAbsent(testee, "unknown")
    templatePresent(testee, "22:t:a", 4)
    templatePresent(testee, "22:t:b", 5)
    templatePresent(testee, "22:same:name", 6)
    templateAbsent(testee, "22:unkno:wn")

    testee
      .update(4)((from, to) =>
        _ => {
          from shouldBe 2
          to shouldBe 6
          Future.successful(
            Vector(
              3 -> "p|22:same:name",
              4 -> "t|22:t:a",
              5 -> "t|22:t:b",
              6 -> "t|22:same:name",
            )
          )
        }
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
}
