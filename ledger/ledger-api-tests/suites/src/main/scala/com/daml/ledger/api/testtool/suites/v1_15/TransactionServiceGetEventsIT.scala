// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_15

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service.{
  GetEventsByContractIdRequest,
  GetEventsByContractKeyRequest,
}
import com.daml.ledger.api.v1.value._
import com.daml.ledger.test.model.Test.{Dummy, _}
import com.daml.lf.value.Value.ContractId
import scalaz.Tag
import scalaz.syntax.tag.ToTagOps

import scala.collection.immutable
import scala.concurrent.Future

class TransactionServiceGetEventsIT extends LedgerTestSuite {

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
    "Expose a visible create event by contract id",
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
      val actual =
        assertDefined(events.events.flatMap(_.kind.created).headOption, "Expected a created event")
      assertEquals("Looked up event should match the transaction event", actual, expected)
    }
  })

  test(
    "TXEventsByContractIdConsumed",
    "Expose both create and exercise events contract id",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummyCid <- ledger.create(party, Dummy(party))
      tx <- ledger.submitAndWaitForTransactionTree(
        ledger.submitAndWaitRequest(party, dummyCid.exerciseDummyChoice1().command)
      )
      expected = assertDefined(
        tx.getTransaction.eventsById.values.headOption.flatMap(_.kind.exercised),
        "Expected an exercised event",
      )
      events <- ledger.getEventsByContractId(
        GetEventsByContractIdRequest(dummyCid.unwrap, immutable.Seq(party.unwrap))
      )
    } yield {
      val actual =
        assertDefined(
          events.events.drop(1).flatMap(_.kind.exercised).headOption,
          "Expected a exercise event",
        )
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
      assertIsEmpty(events.events)
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
      assertIsEmpty(events.events)
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
          maxEvents = 10,
          beginExclusive = None,
          endInclusive = None,
        )
      )
    } yield {
      val actual =
        assertDefined(
          events.events.flatMap(_.kind.created).headOption,
          "Expected a created event",
        )
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
          maxEvents = 10,
          beginExclusive = None,
          endInclusive = None,
        )
      )
    } yield {
      assertIsEmpty(events.events)
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
          maxEvents = 10,
          beginExclusive = None,
          endInclusive = None,
        )
      )
    } yield {
      assertIsEmpty(events.events)
    }
  })

  test(
    "TXEventsByContractKeyMaxEvents",
    "At most max events events should be returned",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val exercisedKey = "max events key"
    val key = makeTextKeyKey(party, exercisedKey)
    val expected = 3
    for {
      textKeyCid1 <- ledger.create(party, TextKey(party, exercisedKey, Nil))
      _ <- ledger.submitAndWaitForTransactionTree(
        ledger.submitAndWaitRequest(party, textKeyCid1.exerciseTextKeyChoice().command)
      )
      textKeyCid2 <- ledger.create(party, TextKey(party, exercisedKey, Nil))
      _ <- ledger.submitAndWaitForTransactionTree(
        ledger.submitAndWaitRequest(party, textKeyCid2.exerciseTextKeyChoice().command)
      )
      events <- ledger.getEventsByContractKey(
        GetEventsByContractKeyRequest(
          contractKey = Some(key),
          templateId = Some(TextKey.id.unwrap),
          requestingParties = Tag.unsubst(immutable.Seq(party)),
          maxEvents = expected,
          beginExclusive = None,
          endInclusive = None,
        )
      )
    } yield {
      val actual = events.events.size
      assertEquals("Maximum events", actual, expected)
    }
  })

  test(
    "TXEventsByContractKeyOffsetRange",
    "Should only select rows in target range",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val someKey = "range test key"
    val key = makeTextKeyKey(party, someKey)

    def hasKey(
        beginExclusive: Option[LedgerOffset],
        endInclusive: Option[LedgerOffset],
    ): Future[Boolean] = {
      ledger
        .getEventsByContractKey(
          GetEventsByContractKeyRequest(
            contractKey = Some(key),
            templateId = Some(TextKey.id.unwrap),
            requestingParties = Tag.unsubst(immutable.Seq(party)),
            maxEvents = 10,
            beginExclusive = beginExclusive,
            endInclusive = endInclusive,
          )
        )
        .map(_.events.nonEmpty)
    }

    for {
      tx1 <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitRequest(party, TextKey(party, "dummy range key", Nil).create.command)
      )
      tx2 <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitRequest(party, TextKey(party, someKey, Nil).create.command)
      )
      Seq(offset1, offset2) = Seq(tx1, tx2).map(tx =>
        Some(LedgerOffset(LedgerOffset.Value.Absolute(tx.completionOffset)))
      )
      hasKey1 <- hasKey(beginExclusive = None, endInclusive = None)
      hasKey2 <- hasKey(
        beginExclusive =
          Some(LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))),
        endInclusive =
          Some(LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))),
      )
      hasKey3 <- hasKey(beginExclusive = None, endInclusive = offset2)
      hasKey4 <- hasKey(beginExclusive = None, endInclusive = offset1)
      hasKey5 <- hasKey(beginExclusive = offset1, endInclusive = None)
      hasKey6 <- hasKey(beginExclusive = offset2, endInclusive = None)
      hasKey7 <- hasKey(beginExclusive = offset2, endInclusive = offset2)
    } yield {
      assert(hasKey1, s"Unspecified offsets do not restrict selection")
      assert(hasKey2, s"Ledger boundaries do not restrict selection")
      assert(hasKey3, s"End offset on key is included")
      assert(!hasKey4, s"End offset before key is excluded")
      assert(hasKey5, s"Begin offset before key is included")
      assert(!hasKey6, s"Begin offset on key is excluded")
      assert(!hasKey7, s"Identical begin/end offset results in no rows")
    }
  })

  test(
    "TXEventsByContractKeyPaging",
    "Should page requests if there may be more rows",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val exercisedKey = "paging key"
    val key = makeTextKeyKey(party, exercisedKey)

    def getNextTwoResults(
        beginExclusive: LedgerOffset
    ): Future[Option[LedgerOffset]] = {
      ledger
        .getEventsByContractKey(
          GetEventsByContractKeyRequest(
            contractKey = Some(key),
            templateId = Some(TextKey.id.unwrap),
            requestingParties = Tag.unsubst(immutable.Seq(party)),
            maxEvents = 2,
            beginExclusive = Some(beginExclusive),
            endInclusive = None,
          )
        )
        .map(_.lastOffset)
    }

    for {
      textKeyCid1 <- ledger.create(party, TextKey(party, exercisedKey, Nil))
      _ <- ledger.submitAndWaitForTransactionTree(
        ledger.submitAndWaitRequest(party, textKeyCid1.exerciseTextKeyChoice().command)
      )
      textKeyCid2 <- ledger.create(party, TextKey(party, exercisedKey, Nil))
      _ <- ledger.submitAndWaitForTransactionTree(
        ledger.submitAndWaitRequest(party, textKeyCid2.exerciseTextKeyChoice().command)
      )
      lastOffset1 <- getNextTwoResults(
        LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
      )
      lastOffset2 <- getNextTwoResults(assertDefined(lastOffset1, "Expected lastOffset1"))
      lastOffset3 <- getNextTwoResults(assertDefined(lastOffset2, "Expected lastOffset2"))
    } yield {
      assertEquals("Expected the final offset to be empty", lastOffset3, None)
    }
  })

}
