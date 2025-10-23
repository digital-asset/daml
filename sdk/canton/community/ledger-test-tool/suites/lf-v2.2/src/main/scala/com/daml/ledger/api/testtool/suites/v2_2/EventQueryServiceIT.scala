// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdRequest
import com.daml.ledger.test.java.model.test.{Agreement, Dummy}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.TransactionShape.LedgerEffects
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.daml.lf.value.Value.ContractId

class EventQueryServiceIT extends LedgerTestSuite {
  import com.daml.ledger.api.testtool.suites.v2_2.CompanionImplicits.*

  // TODO(i16065): Re-enable getEventsByContractKey tests
//  private def toOption(protoString: String): Option[String] = {
//    if (protoString.nonEmpty) Some(protoString) else None
//  }

  // Note that the Daml template must be inspected to establish the key type and fields
  // For the TextKey template the key is: (tkParty, tkKey) : (Party, Text)
  // When populating the Record identifiers are not required.
  // TODO(i16065): Re-enable getEventsByContractKey tests
//  private def makeTextKeyKey(party: Party, keyText: String) = {
//    Value.Sum.Record(
//      Record(fields =
//        Vector(
//          RecordField(value = Some(Value(Value.Sum.Party(party.underlying)))),
//          RecordField(value = Some(Value(Value.Sum.Text(keyText)))),
//        )
//      )
//    )
//  }

  test(
    "TXEventsByContractIdBasic",
    "Expose a create event by contract id",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummyCid <- ledger.create(party, new Dummy(party))
      acs <- ledger.activeContracts(Some(Seq(party)))
      expected = assertDefined(
        acs.headOption,
        "Expected a created event",
      )
      events <- ledger.getEventsByContractId(
        GetEventsByContractIdRequest(
          expected.contractId,
          Some(ledger.eventFormat(verbose = true, Some(Seq(party)))),
        )
      )
      // archive not to interfere with other tests
      _ <- ledger.exercise(party, dummyCid.exerciseArchive())
    } yield {
      val created = assertDefined(events.created, "Expected a created event wrapper")
      val actual = assertDefined(created.createdEvent, "Expected a created event")
      assertEquals("Looked up event should match the transaction event", actual, expected)
    }
  })

  test(
    "TXEventsByContractIdConsumed",
    "Expose an archive event by contract id",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummyCid <- ledger.create(party, new Dummy(party))
      tx <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitForTransactionRequest(party, dummyCid.exerciseDummyChoice1().commands)
      )
      expected = assertDefined(
        tx.getTransaction.events.flatMap(_.event.archived).headOption,
        "Expected an exercised event",
      )
      events <- ledger.getEventsByContractId(
        GetEventsByContractIdRequest(
          dummyCid.contractId,
          Some(ledger.eventFormat(verbose = true, Some(Seq(party)))),
        )
      )
    } yield {
      assertDefined(events.created, "Expected a create event").discard
      val archived = assertDefined(events.archived, "Expected a exercise event wrapper")
      val actual = assertDefined(archived.archivedEvent, "Expected a exercise event")
      assertEquals("Looked up event should match the transaction event", actual, expected)
    }
  })

  test(
    "TXEventsByContractIdNotExistent",
    "No events are returned for a non-existent contract id",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val nonExistentContractId = ContractId.V1.assertFromString("00" * 32 + "0001")
    for {
      error <- ledger
        .getEventsByContractId(
          GetEventsByContractIdRequest(
            nonExistentContractId.coid,
            Some(ledger.eventFormat(verbose = true, Some(Seq(party)))),
          )
        )
        .failed
    } yield {
      assertGrpcError(
        error,
        RequestValidationErrors.NotFound.ContractEvents,
        Some("Contract events not found, or not visible"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "TXEventsByContractIdNotVisible",
    "CONTRACT_EVENTS_NOT_FOUND returned for a non-visible contract id",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party, notTheSubmittingParty))) =>
    for {
      dummyCid <- ledger.create(party, new Dummy(party))
      acs <- ledger.activeContracts(Some(Seq(party)))
      expected = assertDefined(
        acs.headOption,
        "Expected a created event",
      )
      error <- ledger
        .getEventsByContractId(
          GetEventsByContractIdRequest(
            expected.contractId,
            Some(ledger.eventFormat(verbose = true, Some(Seq(notTheSubmittingParty)))),
          )
        )
        .failed
      // archive not to interfere with other tests
      _ <- ledger.exercise(party, dummyCid.exerciseArchive())
    } yield {
      assertGrpcError(
        error,
        RequestValidationErrors.NotFound.ContractEvents,
        Some("Contract events not found, or not visible"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "TXEventsByContractIdFilterCombinations",
    "EventsByContractId should support event format",
    allocate(TwoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party, notTheSubmittingParty))) =>
    for {
      dummyCid <- ledger.create(party, new Dummy(party))
      acs <- ledger.activeContracts(Some(Seq(party)), verbose = false)
      expected = assertDefined(
        acs.headOption,
        "Expected a created event",
      )
      wildcardPartiesResult <- ledger.getEventsByContractId(
        GetEventsByContractIdRequest(
          expected.contractId,
          Some(ledger.eventFormat(verbose = false, None)),
        )
      )
      templateFiltersResult <- ledger.getEventsByContractId(
        GetEventsByContractIdRequest(
          expected.contractId,
          Some(ledger.eventFormat(verbose = false, Some(Seq(party)), Seq(Dummy.TEMPLATE_ID))),
        )
      )
      templateFiltersNotFoundDueToParty <- ledger
        .getEventsByContractId(
          GetEventsByContractIdRequest(
            expected.contractId,
            Some(
              ledger.eventFormat(
                verbose = false,
                Some(Seq(notTheSubmittingParty)),
                Seq(Dummy.TEMPLATE_ID),
              )
            ),
          )
        )
        .failed
      templateFiltersNotFoundDueToTemplate <- ledger
        .getEventsByContractId(
          GetEventsByContractIdRequest(
            expected.contractId,
            Some(ledger.eventFormat(verbose = false, Some(Seq(party)), Seq(Agreement.TEMPLATE_ID))),
          )
        )
        .failed
      // archive not to interfere with other tests
      _ <- ledger.exercise(party, dummyCid.exerciseArchive())
    } yield {
      assertIsEmpty(wildcardPartiesResult.archived)
      assertEquals(
        "Looked up wildcardPartiesResult should match the transaction event",
        assertDefined(
          wildcardPartiesResult.created.flatMap(_.createdEvent),
          "Expected a create event",
        ),
        expected,
      )

      assertIsEmpty(templateFiltersResult.archived)
      assertEquals(
        "Looked up templateFiltersResult should match the transaction event",
        assertDefined(
          templateFiltersResult.created.flatMap(_.createdEvent),
          "Expected a create event",
        ),
        expected,
      )

      assertGrpcError(
        templateFiltersNotFoundDueToParty,
        RequestValidationErrors.NotFound.ContractEvents,
        Some("Contract events not found, or not visible"),
        checkDefiniteAnswerMetadata = true,
      )

      assertGrpcError(
        templateFiltersNotFoundDueToTemplate,
        RequestValidationErrors.NotFound.ContractEvents,
        Some("Contract events not found, or not visible"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "TXEventsByContractIdTransient",
    "Expose transient events (created and archived in the same transaction) by contract id",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      // Create command with transient contract
      tx <- ledger.submitAndWaitForTransaction(
        ledger.submitAndWaitForTransactionRequest(
          party = party,
          commands = new Dummy(party).createAnd.exerciseArchive().commands,
          transactionShape = LedgerEffects,
        )
      )
      contractId = assertDefined(
        tx.getTransaction.events.flatMap(_.event.created).headOption,
        "Expected a created event",
      ).contractId
      transient <- ledger
        .getEventsByContractId(
          GetEventsByContractIdRequest(
            contractId,
            Some(ledger.eventFormat(verbose = false, parties = Some(Seq(party)))),
          )
        )
      transientPartyWildcard <- ledger
        .getEventsByContractId(
          GetEventsByContractIdRequest(
            contractId,
            Some(ledger.eventFormat(verbose = false, parties = None)),
          )
        )
    } yield {
      assertDefined(transient.created.flatMap(_.createdEvent), "Expected a created event")
      assertDefined(transient.archived.flatMap(_.archivedEvent), "Expected an archived event")

      assertDefined(
        transientPartyWildcard.created.flatMap(_.createdEvent),
        "Expected a created event",
      )
      assertDefined(
        transientPartyWildcard.archived.flatMap(_.archivedEvent),
        "Expected an archived event",
      )
    }
  })

  // TODO(i16065): Re-enable getEventsByContractKey tests
//  test(
//    "TXEventsByContractKeyBasic",
//    "Expose a visible create event by contract key",
//    allocate(SingleParty),
//  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
//    val someKey = "some key"
//    val key = makeTextKeyKey(party, someKey)
//
//    for {
//      tx <- ledger.submitAndWaitForTransaction(
//        ledger.submitAndWaitRequest(party, new TextKey(party.underlying, someKey, JList.of()).create.commands)
//      )
//      _ = logger.error(tx.toString)
//      expected = assertDefined(
//        tx.transaction.flatMap(_.events.flatMap(_.event.created).headOption),
//        "Expected a created event",
//      )
//
//      events <- ledger.getEventsByContractKey(
//        GetEventsByContractKeyRequest(
//          contractKey = Some(key),
//          templateId = Some(TextKey.TEMPLATE_ID.toV1),
//          requestingParties = Seq(party),
//        )
//      )
//      _ = logger.error(events.toString)
//    } yield {
//      val actual = assertDefined(events.createEvent, "Expected a created event")
//      assertEquals("Looked up event should match the transaction event", actual, expected)
//    }
//  })

//  test(
//    "TXArchiveEventByContractKeyBasic",
//    "Expose a visible archive event by contract key",
//    allocate(SingleParty),
//  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
//    val someKey = "some archive key"
//    val key = makeTextKeyKey(party, someKey)
//
//    for {
//      cId: TextKey.ContractId <- ledger.create(party, new TextKey(party, someKey, JList.of()))
//      tx <- ledger.submitAndWaitForTransaction(
//        ledger.submitAndWaitRequest(party, cId.exerciseTextKeyChoice().commands)
//      )
//      expected = assertDefined(
//        tx.transaction.flatMap(_.events.flatMap(_.event.archived).headOption),
//        "Expected an archived event",
//      )
//      events <- ledger.getEventsByContractKey(
//        GetEventsByContractKeyRequest(
//          contractKey = Some(key),
//          templateId = Some(TextKey.TEMPLATE_ID.toV1),
//          requestingParties = Seq(party),
//        )
//      )
//    } yield {
//      assertDefined(events.createEvent, "Expected a create event").discard
//      val actual = assertDefined(events.archiveEvent, "Expected a archived event")
//      assertEquals("Looked up event should match the transaction event", actual, expected)
//    }
//  })

//  test(
//    "TXEventsByContractKeyNoKey",
//    "No events are returned for a non existent contract key",
//    allocate(SingleParty),
//  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
//    val key = makeTextKeyKey(party, "non existent key")
//
//    for {
//      events <- ledger.getEventsByContractKey(
//        GetEventsByContractKeyRequest(
//          contractKey = Some(key),
//          templateId = Some(TextKey.TEMPLATE_ID.toV1),
//          requestingParties = Seq(party),
//        )
//      )
//    } yield {
//      assertIsEmpty(Seq(events.createEvent, events.archiveEvent).flatten[GeneratedMessage])
//    }
//  })

//  test(
//    "TXEventsByContractKeyNotVisible",
//    "No events are returned for a non visible contract key",
//    allocate(TwoParties),
//  )(implicit ec => { case Participants(Participant(ledger, Seq(party, notTheSubmittingParty))) =>
//    val nonVisibleKey = "non visible key"
//    val key = makeTextKeyKey(party, nonVisibleKey)
//
//    for {
//      _ <- ledger.submitAndWaitForTransaction(
//        ledger.submitAndWaitRequest(
//          party,
//          new TextKey(party, nonVisibleKey, JList.of()).create.commands,
//        )
//      )
//
//      events <- ledger.getEventsByContractKey(
//        GetEventsByContractKeyRequest(
//          contractKey = Some(key),
//          templateId = Some(TextKey.TEMPLATE_ID.toV1),
//          requestingParties = Seq(notTheSubmittingParty),
//        )
//      )
//    } yield {
//      assertIsEmpty(Seq(events.createEvent, events.archiveEvent).flatten[GeneratedMessage])
//    }
//  })

//  test(
//    "TXEventsByContractKeyEndExclusive",
//    "Should return event prior to the end exclusive event",
//    allocate(SingleParty),
//  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
//    val exercisedKey = "paging key"
//    val key = makeTextKeyKey(party, exercisedKey)
//
//    def getNextResult(continuationToken: Option[String]): Future[Option[String]] = {
//      ledger
//        .getEventsByContractKey(
//          GetEventsByContractKeyRequest(
//            contractKey = Some(key),
//            templateId = Some(TextKey.TEMPLATE_ID.toV1),
//            requestingParties = Seq(party),
//            continuationToken = continuationToken.getOrElse(
//              GetEventsByContractKeyRequest.defaultInstance.continuationToken
//            ),
//          )
//        )
//        .map(r => toOption(r.continuationToken))
//    }
//
//    for {
//      textKeyCid1: TextKey.ContractId <- ledger.create(
//        party,
//        new TextKey(party, exercisedKey, Nil.asJava),
//      )
//      _ <- ledger.submitAndWaitForTransaction(
//        ledger.submitAndWaitRequest(party, textKeyCid1.exerciseTextKeyChoice().commands)
//      )
//      textKeyCid2: TextKey.ContractId <- ledger.create(
//        party,
//        new TextKey(party, exercisedKey, Nil.asJava),
//      )
//      _ <- ledger.submitAndWaitForTransaction(
//        ledger.submitAndWaitRequest(party, textKeyCid2.exerciseTextKeyChoice().commands)
//      )
//      eventId1 <- getNextResult(None)
//      eventId2 <- getNextResult(Some(assertDefined(eventId1, "Expected eventId2")))
//      eventId3 <- getNextResult(Some(assertDefined(eventId2, "Expected eventId3")))
//    } yield {
//      assertEquals("Expected the final offset to be empty", eventId3, None)
//    }
//  })

//  test(
//    "TXEventsByContractKeyChained",
//    "Should not miss events where the choice recreates the key",
//    allocate(SingleParty),
//  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
//    val exercisedKey = "paging key"
//    val key = makeTextKeyKey(party, exercisedKey)
//
//    // (contract-id, continuation-token)
//    def getNextResult(
//        continuationToken: Option[String]
//    ): Future[(Option[String], Option[String])] = {
//      ledger
//        .getEventsByContractKey(
//          GetEventsByContractKeyRequest(
//            contractKey = Some(key),
//            templateId = Some(TextKey.TEMPLATE_ID.toV1),
//            requestingParties = Seq(party),
//            continuationToken = continuationToken.getOrElse(
//              GetEventsByContractKeyRequest.defaultInstance.continuationToken
//            ),
//          )
//        )
//        .map(r => (r.createEvent.map(_.contractId), toOption(r.continuationToken)))
//    }
//
//    for {
//      expected: TextKey.ContractId <- ledger.create(
//        party,
//        new TextKey(party, exercisedKey, Nil.asJava),
//      )
//      _ <- ledger.submitAndWaitForTransaction(
//        ledger.submitAndWaitRequest(
//          party,
//          expected.exerciseTextKeyDisclose(JList.of(): JList[String]).commands,
//        )
//      )
//      (cId2, token2) <- getNextResult(None)
//      (cId1, token1) <- getNextResult(token2)
//      (cId0, _) <- getNextResult(token1)
//    } yield {
//      assertEquals("Expected the first offset to be empty", cId2.isDefined, true)
//      assertEquals("Expected the final offset to be empty", cId1, Some(expected.contractId))
//      assertEquals("Expected the final offset to be empty", cId0.isDefined, false)
//    }
//  })

}
