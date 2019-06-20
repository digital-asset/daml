// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.transaction

import com.digitalasset.daml.lf.data.Ref.QualifiedName
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.value.Value.ValueRecord
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.Event.{ArchivedEvent, CreateOrArchiveEvent, CreatedEvent}
import com.digitalasset.ledger.api.domain.{Filters, InclusiveFilters, TransactionFilter}
import com.digitalasset.ledger.api.validation.TransactionFilterValidator
import com.digitalasset.platform.participant.util.EventFilter
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, OptionValues, WordSpec}

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class EventFilterSpec extends WordSpec with Matchers with ScalaFutures with OptionValues {

  private val otherPartyWhoSeesEvents = Ref.Party.assertFromString("otherParty")
  private val packageId = "myPackage"
  private val eventId = domain.EventId(Ref.LedgerString.assertFromString("someEventId"))
  private val contractId =
    domain.ContractId(Ref.ContractIdString.assertFromString("someContractId"))
  private val party1 = Ref.Party.assertFromString("party1")
  private val party2 = Ref.Party.assertFromString("party2")
  private val party3 = "party3"
  private val module1 = "module1"
  private val module2 = "module2"
  private val template1 = "template1"
  private val template2 = "template2"
  private val templateId1 = mkIdent(module1, template1)
  private val templateId2 = mkIdent(module2, template2)

  def mkIdent(mod: String, ent: String, pkgId: String = packageId) =
    Ref.Identifier(
      Ref.PackageId.assertFromString(pkgId),
      QualifiedName(Ref.ModuleName.assertFromString(mod), Ref.DottedName.assertFromString(ent)))

  private val filterValidator = new TransactionFilterValidator(
    IdentifierResolver(_ => Future.successful(None)))

  private val mapping = Map(
    party1 -> getFilter(Seq(module1 -> template1)),
    party2 -> getFilter(Seq(module1 -> template1, module2 -> template2))
  )

  private val filter = (event: CreateOrArchiveEvent) =>
    EventFilter
      .TemplateAwareFilter(TransactionFilter(mapping))
      .filterCreateOrArchiveWitnesses(event)

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

  def runTemplateFilterAssertions(eventType: String)(
      createEvent: (Ref.Party, Ref.Identifier) => CreateOrArchiveEvent) = {
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
        mkIdent(pkgId = "someOtherPackageId", mod = module1, ent = template1))) shouldEqual None
    }

    s"not let $eventType through when templateId is not listened to" in {
      filter(createEvent(party1, templateId2)) shouldEqual None
    }
  }

  private def createdEvent(party: Ref.Party, templateId: Ref.Identifier): CreatedEvent =
    CreatedEvent(
      eventId,
      contractId,
      templateId,
      ValueRecord(None, ImmArray.empty),
      Set(party, otherPartyWhoSeesEvents),
      Set.empty,
      Set.empty,
      "",
      None
    )

  private def archivedEvent(party: Ref.Party, templateId: Ref.Identifier): ArchivedEvent =
    ArchivedEvent(
      eventId,
      contractId,
      templateId,
      Set(party, otherPartyWhoSeesEvents)
    )
}
