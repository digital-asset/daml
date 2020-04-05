// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.lf.data.Ref
import com.daml.lf.value.Value
import com.daml.ledger.api.domain.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event}
import com.daml.ledger.api.v1.value.{Identifier, Record}
import com.daml.platform.api.v1.event.EventOps.EventOps
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, OptionValues, WordSpec}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
final class EventFilterSpec extends WordSpec with Matchers with ScalaFutures with OptionValues {

  private val otherPartyWhoSeesEvents = Ref.Party.assertFromString("otherParty")
  private val packageId = "myPackage"
  private val eventId = Ref.LedgerString.assertFromString("someEventId")
  private val contractId = Value.AbsoluteContractId.assertFromString("#someContractId")
  private val party1 = Ref.Party.assertFromString("party1")
  private val party2 = Ref.Party.assertFromString("party2")
  private val module1 = "module1"
  private val module2 = "module2"
  private val template1 = "template1"
  private val template2 = "template2"
  private val templateId1 = mkApiIdent(module1, template1)
  private val templateId2 = mkApiIdent(module2, template2)

  private def mkApiIdent(mod: String, ent: String, pkgId: String = packageId): Identifier =
    Identifier(
      Ref.PackageId.assertFromString(pkgId),
      mod,
      ent
    )

  private def mkIdent(mod: String, ent: String, pkgId: String = packageId) =
    Ref.Identifier(
      Ref.PackageId.assertFromString(pkgId),
      Ref.QualifiedName(Ref.ModuleName.assertFromString(mod), Ref.DottedName.assertFromString(ent)))

  private val mapping = Map(
    party1 -> getFilter(Seq(module1 -> template1)),
    party2 -> getFilter(Seq(module1 -> template1, module2 -> template2))
  )

  private val filter = (event: Event) => EventFilter(event)(TransactionFilter(mapping))

  def getFilter(templateIds: Seq[(String, String)]) =
    Filters(InclusiveFilters(templateIds.map {
      case (mod, ent) => mkIdent(mod, ent)
    }.toSet))

  "EventFilter" when {

    "filtered by TemplateIds" should {
      runTemplateFilterAssertions("CreatedEvent")(createdEvent)
      runTemplateFilterAssertions("ArchivedEvent")(archivedEvent)

      "remove non-requesting witnesses from the disclosed event" in {
        val resultO = filter(createdEvent(party1, templateId1)).map(_.witnessParties)
        resultO should not be empty
        val result = resultO.get
        result should not contain otherPartyWhoSeesEvents
        result should contain theSameElementsAs List(party1)
      }
    }

  }

  private def runTemplateFilterAssertions(eventType: String)(
      createEvent: (Ref.Party, Identifier) => Event): Unit = {
    val isExercised = eventType == "ExercisedEvent"
    val negateIfRequired = if (isExercised) "not " else ""

    s"${negateIfRequired}let $eventType through when both party and templateId matches" in {
      filter(createEvent(party1, templateId1)) should (if (isExercised) be(empty) else not be empty)
    }

    s"${negateIfRequired}let $eventType through when interested in multiple templateIds" in {
      filter(createEvent(party2, templateId1)) should (if (isExercised) be(empty) else not be empty)
      filter(createEvent(party2, templateId2)) should (if (isExercised) be(empty) else not be empty)
    }

    s"not let $eventType through when party is not listened to" in {
      filter(createEvent(Ref.Party.assertFromString("unknownParty"), templateId1)) shouldEqual None
    }

    s"not let $eventType through when packageId does not match" in {
      filter(createEvent(
        party1,
        mkApiIdent(pkgId = "someOtherPackageId", mod = module1, ent = template1))) shouldEqual None
    }

    s"not let $eventType through when templateId is not listened to" in {
      filter(createEvent(party1, templateId2)) shouldEqual None
    }
  }

  private def createdEvent(party: Ref.Party, templateId: Identifier): Event =
    Event(
      Event.Event.Created(
        CreatedEvent(
          eventId,
          contractId.coid,
          Some(templateId),
          None,
          Some(Record(None, Seq.empty)),
          Seq(party, otherPartyWhoSeesEvents),
          Seq.empty,
          Seq.empty,
          None
        )))

  private def archivedEvent(party: Ref.Party, templateId: Identifier): Event =
    Event(
      Event.Event.Archived(
        ArchivedEvent(
          eventId,
          contractId.coid,
          Some(templateId),
          Seq(party, otherPartyWhoSeesEvents)
        )))
}
