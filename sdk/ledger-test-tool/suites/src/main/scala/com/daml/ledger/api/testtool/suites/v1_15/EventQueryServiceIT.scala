// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_15

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v1.event_query_service.{
  GetEventsByContractIdRequest,
  GetEventsByContractKeyRequest,
}
import com.daml.ledger.api.v1.value._
import com.daml.ledger.test.model.Test.{Dummy, _}
import com.daml.lf.value.Value.ContractId
import scalapb.GeneratedMessage
import scalaz.Tag
import scalaz.syntax.tag.ToTagOps

import scala.collection.immutable
import scala.concurrent.Future

class EventQueryServiceIT extends LedgerTestSuite {

  private def toOption(protoString: String): Option[String] = {
    if (protoString.nonEmpty) Some(protoString) else None
  }

  // Note that the Daml template must be inspected to establish the key type and fields
  // For the TextKey template the key is: (tkParty, tkKey) : (Party, Text)
  // When populating the Record identifiers are not required.
  private def makeTextKeyKey(party: ApiTypes.Party, keyText: String) = {
    Value(
      Value.Sum.Record(
        Record(fields =
          Vector(
            RecordField(value = Some(Value(Value.Sum.Party(party.unwrap)))),
            RecordField(value = Some(Value(Value.Sum.Text(keyText)))),
          )
        )
      )
    )
  }

  test(
    "TXEventsByContractIdBasic",
    "Expose a create event by contract id",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      tx <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitRequest(party, Dummy(party).create.command)
      )
      expected = assertDefined(
        tx.transaction.flatMap(_.events.flatMap(_.event.created).headOption),
        "Expected a created event",
      )
      events <- ledger.getEventsByContractId(
        GetEventsByContractIdRequest(expected.contractId, Tag.unsubst(immutable.Seq(party)))
      )
    } yield {
      val actual = assertDefined(events.createEvent, "Expected a created event")
      assertEquals("Looked up event should match the transaction event", actual, expected)
    }
  })

  test(
    "TXEventsByContractIdConsumed",
    "Expose an archive event by contract id",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummyCid <- ledger.create(party, Dummy(party))
      tx <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitRequest(party, dummyCid.exerciseDummyChoice1().command)
      )
      expected = assertDefined(
        tx.getTransaction.events.flatMap(_.event.archived).headOption,
        "Expected an exercised event",
      )
      events <- ledger.getEventsByContractId(
        GetEventsByContractIdRequest(dummyCid.unwrap, immutable.Seq(party.unwrap))
      )
    } yield {
      assertDefined(events.createEvent, "Expected a create event")
      val actual = assertDefined(events.archiveEvent, "Expected a exercise event")
      assertEquals("Looked up event should match the transaction event", actual, expected)
    }
  })

  test(
    "TXEventsByContractIdNotExistent",
    "No events are returned for a non-existent contract id",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val nonExistentContractId = ContractId.V1.assertFromString("00" * 32 + "0001")
    for {
      events <- ledger.getEventsByContractId(
        GetEventsByContractIdRequest(nonExistentContractId.coid, Tag.unsubst(immutable.Seq(party)))
      )
    } yield {
      assertIsEmpty(Seq(events.createEvent, events.archiveEvent).flatten[GeneratedMessage])
    }
  })

  test(
    "TXEventsByContractIdNotVisible",
    "No events are returned for a non-visible contract id",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, party, notTheSubmittingParty)) =>
    for {
      tx <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitRequest(party, Dummy(party).create.command)
      )
      expected = assertDefined(
        tx.transaction.flatMap(_.events.flatMap(_.event.created).headOption),
        "Expected a created event",
      )
      events <- ledger.getEventsByContractId(
        GetEventsByContractIdRequest(
          expected.contractId,
          immutable.Seq(notTheSubmittingParty.unwrap),
        )
      )
    } yield {
      assertIsEmpty(Seq(events.createEvent, events.archiveEvent).flatten[GeneratedMessage])
    }
  })

  test(
    "TXEventsByContractKeyBasic",
    "Expose a visible create event by contract key",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val someKey = "some key"
    val key = makeTextKeyKey(party, someKey)

    for {
      tx <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitRequest(party, TextKey(party, someKey, Nil).create.command)
      )
      expected = assertDefined(
        tx.transaction.flatMap(_.events.flatMap(_.event.created).headOption),
        "Expected a created event",
      )

      events <- ledger.getEventsByContractKey(
        GetEventsByContractKeyRequest(
          contractKey = Some(key),
          templateId = Some(TextKey.id.unwrap),
          requestingParties = Tag.unsubst(immutable.Seq(party)),
        )
      )
    } yield {
      val actual = assertDefined(events.createEvent, "Expected a created event")
      assertEquals("Looked up event should match the transaction event", actual, expected)
    }
  })

  test(
    "TXArchiveEventByContractKeyBasic",
    "Expose a visible archive event by contract key",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val someKey = "some archive key"
    val key = makeTextKeyKey(party, someKey)

    for {
      cId <- ledger.create(party, TextKey(party, someKey, Nil))
      tx <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitRequest(party, cId.exerciseTextKeyChoice().command)
      )
      expected = assertDefined(
        tx.transaction.flatMap(_.events.flatMap(_.event.archived).headOption),
        "Expected an archived event",
      )
      events <- ledger.getEventsByContractKey(
        GetEventsByContractKeyRequest(
          contractKey = Some(key),
          templateId = Some(TextKey.id.unwrap),
          requestingParties = Tag.unsubst(immutable.Seq(party)),
        )
      )
    } yield {
      assertDefined(events.createEvent, "Expected a create event")
      val actual = assertDefined(events.archiveEvent, "Expected a archived event")
      assertEquals("Looked up event should match the transaction event", actual, expected)
    }
  })

  test(
    "TXEventsByContractKeyNoKey",
    "No events are returned for a non existent contract key",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val key = makeTextKeyKey(party, "non existent key")

    for {
      events <- ledger.getEventsByContractKey(
        GetEventsByContractKeyRequest(
          contractKey = Some(key),
          templateId = Some(TextKey.id.unwrap),
          requestingParties = Tag.unsubst(immutable.Seq(party)),
        )
      )
    } yield {
      assertIsEmpty(Seq(events.createEvent, events.archiveEvent).flatten[GeneratedMessage])
    }
  })

  test(
    "TXEventsByContractKeyNotVisible",
    "No events are returned for a non visible contract key",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, party, notTheSubmittingParty)) =>
    val nonVisibleKey = "non visible key"
    val key = makeTextKeyKey(party, nonVisibleKey)

    for {
      _ <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitRequest(party, TextKey(party, nonVisibleKey, Nil).create.command)
      )

      events <- ledger.getEventsByContractKey(
        GetEventsByContractKeyRequest(
          contractKey = Some(key),
          templateId = Some(TextKey.id.unwrap),
          requestingParties = immutable.Seq(notTheSubmittingParty.unwrap),
        )
      )
    } yield {
      assertIsEmpty(Seq(events.createEvent, events.archiveEvent).flatten[GeneratedMessage])
    }
  })

  test(
    "TXEventsByContractKeyEndExclusive",
    "Should return event prior to the end exclusive event",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val exercisedKey = "paging key"
    val key = makeTextKeyKey(party, exercisedKey)

    def getNextResult(continuationToken: Option[String]): Future[Option[String]] = {
      ledger
        .getEventsByContractKey(
          GetEventsByContractKeyRequest(
            contractKey = Some(key),
            templateId = Some(TextKey.id.unwrap),
            requestingParties = Tag.unsubst(immutable.Seq(party)),
            continuationToken = continuationToken.getOrElse(
              GetEventsByContractKeyRequest.defaultInstance.continuationToken
            ),
          )
        )
        .map(r => toOption(r.continuationToken))
    }

    for {
      textKeyCid1 <- ledger.create(party, TextKey(party, exercisedKey, Nil))
      _ <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitRequest(party, textKeyCid1.exerciseTextKeyChoice().command)
      )
      textKeyCid2 <- ledger.create(party, TextKey(party, exercisedKey, Nil))
      _ <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitRequest(party, textKeyCid2.exerciseTextKeyChoice().command)
      )
      eventId1 <- getNextResult(None)
      eventId2 <- getNextResult(Some(assertDefined(eventId1, "Expected eventId2")))
      eventId3 <- getNextResult(Some(assertDefined(eventId2, "Expected eventId3")))
    } yield {
      assertEquals("Expected the final offset to be empty", eventId3, None)
    }
  })

  test(
    "TXEventsByContractKeyChained",
    "Should not miss events where the choice recreates the key",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val exercisedKey = "paging key"
    val key = makeTextKeyKey(party, exercisedKey)

    // (contract-id, continuation-token)
    def getNextResult(
        continuationToken: Option[String]
    ): Future[(Option[String], Option[String])] = {
      ledger
        .getEventsByContractKey(
          GetEventsByContractKeyRequest(
            contractKey = Some(key),
            templateId = Some(TextKey.id.unwrap),
            requestingParties = Tag.unsubst(immutable.Seq(party)),
            continuationToken = continuationToken.getOrElse(
              GetEventsByContractKeyRequest.defaultInstance.continuationToken
            ),
          )
        )
        .map(r => (r.createEvent.map(_.contractId), toOption(r.continuationToken)))
    }

    for {
      expected <- ledger.create(party, TextKey(party, exercisedKey, Nil))
      _ <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitRequest(
          party,
          expected.exerciseTextKeyDisclose(tkNewDisclosedTo = immutable.Seq.empty).command,
        )
      )
      (cId2, token2) <- getNextResult(None)
      (cId1, token1) <- getNextResult(token2)
      (cId0, _) <- getNextResult(token1)
    } yield {
      assertEquals("Expected the first offset to be empty", cId2.isDefined, true)
      assertEquals("Expected the final offset to be empty", cId1, Some(expected))
      assertEquals("Expected the final offset to be empty", cId0.isDefined, false)
    }
  })

}
