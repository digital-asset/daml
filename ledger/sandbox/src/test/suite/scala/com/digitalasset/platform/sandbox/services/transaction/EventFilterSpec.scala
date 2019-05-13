// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.transaction

import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.event._
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.api.validation.TransactionFilterValidator
import com.digitalasset.platform.api.v1.event.EventOps._
import com.digitalasset.platform.participant.util.EventFilter
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, OptionValues, WordSpec}

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class EventFilterSpec extends WordSpec with Matchers with ScalaFutures with OptionValues {

  private val otherPartyWhoSeesEvents = "otherParty"
  private val packageId = "myPackage"
  private val eventId = "someEventId"
  private val contractId = "someContractId"
  private val party1 = "party1"
  private val party2 = "party2"
  private val party3 = "party3"
  private val module1 = "module1"
  private val module2 = "module2"
  private val template1 = "template1"
  private val template2 = "template2"
  private val templateId1 = Identifier(packageId, moduleName = module1, entityName = template1)
  private val templateId2 = Identifier(packageId, moduleName = module2, entityName = template2)

  private val filterValidator = new TransactionFilterValidator(
    IdentifierResolver(_ => Future.successful(None)))

  private val mapping = Map(
    party1 -> getFilter(Seq(module1 -> template1)),
    party2 -> getFilter(Seq(module1 -> template1, module2 -> template2))
  )

  private val filter = (event: Event) =>
    filterValidator
      .validate(TransactionFilter(mapping), "filter")
      .toOption
      .flatMap(
        EventFilter
          .TemplateAwareFilter(_)
          .filterEvent(event))

  def getFilter(templateIds: Seq[(String, String)]) =
    Filters(Some(InclusiveFilters(templateIds.map {
      case (mod, ent) => Identifier(packageId, moduleName = mod, entityName = ent)
    })))

  "EventFilter" when {

    "filtered by TemplateIds" should {
      runTemplateFilterAssertions("CreatedEvent")(createdEvent)
      runTemplateFilterAssertions("ArchivedEvent")(archivedEvent)

      "remove non-requesting witnesses from the disclosed event" in {
        val resultO = filter(createdEvent(party1, templateId1)).map(_.witnesses)
        resultO should not be empty
        val result = resultO.get
        result should not contain otherPartyWhoSeesEvents
        result should contain theSameElementsAs List(party1)
      }
    }

  }

  def runTemplateFilterAssertions(eventType: String)(createEvent: (String, Identifier) => Event) = {
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
      filter(createEvent("unknownParty", templateId1)) shouldEqual None
    }

    s"not let $eventType through when packageId does not match" in {
      filter(createEvent(
        party1,
        Identifier("someOtherPackageId", moduleName = module1, entityName = template1))) shouldEqual None
    }

    s"not let $eventType through when templateId is not listened to" in {
      filter(createEvent(party1, templateId2)) shouldEqual None
    }
  }

  private def createdEvent(party: String, templateId: Identifier) =
    Event(
      Created(
        CreatedEvent(
          eventId = eventId,
          contractId = contractId,
          templateId = Some(templateId),
          witnessParties = Seq(party, otherPartyWhoSeesEvents)
        )
      ))

  private def archivedEvent(party: String, templateId: Identifier) =
    Event(
      Archived(
        ArchivedEvent(
          eventId = eventId,
          contractId = contractId,
          templateId = Some(templateId),
          witnessParties = Seq(party, otherPartyWhoSeesEvents)
        )
      ))
}
