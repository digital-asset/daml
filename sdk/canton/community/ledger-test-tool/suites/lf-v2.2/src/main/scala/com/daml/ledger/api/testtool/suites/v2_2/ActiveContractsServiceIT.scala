// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party, TestConstraints}
import com.daml.ledger.api.v2.event.Event.Event.Created
import com.daml.ledger.api.v2.event.{CreatedEvent, Event}
import com.daml.ledger.api.v2.state_service.GetActiveContractsRequest
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
  WildcardFilter,
}
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi.data.codegen.ContractId
import com.daml.ledger.javaapi.data.{Identifier as JavaIdentifier, Template}
import com.daml.ledger.test.java.model.test.{
  Divulgence1,
  Divulgence2,
  Dummy,
  DummyFactory,
  DummyWithParam,
  TriAgreement,
  TriProposal,
  WithObservers,
  Witnesses as TestWitnesses,
}
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.Random

class ActiveContractsServiceIT extends LedgerTestSuite {
  import CompanionImplicits.*

  test(
    "ACSemptyResponse",
    "The ActiveContractService should succeed with an empty response if no contracts have been created for a party",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      activeContracts <- ledger.activeContracts(Some(Seq(party)))
    } yield {
      assert(
        activeContracts.isEmpty,
        s"There should be no active contracts, but received $activeContracts",
      )
    }
  })

  test(
    "ACSallContracts",
    "The ActiveContractService should return all active contracts",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      offsetAtTestStart <- ledger.currentEnd()
      (dummy, dummyWithParam, dummyFactory) <- createDummyContracts(party, ledger)
      activeContracts <- ledger.activeContracts(Some(Seq(party)))
      activeContractsPartyWildcard <- ledger
        .activeContracts(None)
        .map(_.filter(_.offset > offsetAtTestStart))
      end <- ledger.currentEnd()
      activeContractsPartyWildcardAndSpecificParty <- ledger
        .activeContracts(
          GetActiveContractsRequest(
            eventFormat = Some(
              ledger
                .eventFormat(verbose = true, Some(Seq(party)))
                .update(_.filtersForAnyParty := Filters(Nil))
            ),
            activeAtOffset = end,
          )
        )
        .map(_.filter(_.offset > offsetAtTestStart))

      // archive the contracts to not be active on the next tests
      _ <- archive(ledger, party)(dummy, dummyWithParam, dummyFactory)
    } yield {
      assert(
        activeContracts.size == 3,
        s"Expected 3 contracts, but received ${activeContracts.size}.",
      )

      assert(
        activeContracts.exists(_.contractId == dummy.contractId),
        s"Didn't find Dummy contract with contractId $dummy.",
      )
      assert(
        activeContracts.exists(_.contractId == dummyWithParam.contractId),
        s"Didn't find DummyWithParam contract with contractId $dummy.",
      )
      assert(
        activeContracts.exists(_.contractId == dummyFactory.contractId),
        s"Didn't find DummyFactory contract with contractId $dummy.",
      )

      assert(
        activeContracts.forall(_.acsDelta),
        s"Expected acs_delta to be true for all the created events",
      )

      val invalidSignatories = activeContracts.filterNot(_.signatories == Seq(party.getValue))
      assert(
        invalidSignatories.isEmpty,
        s"Found contracts with signatories other than $party: $invalidSignatories",
      )

      val invalidObservers = activeContracts.filterNot(_.observers.isEmpty)
      assert(
        invalidObservers.isEmpty,
        s"Found contracts with non-empty observers: $invalidObservers",
      )

      assert(
        activeContracts == activeContractsPartyWildcard,
        s"Active contracts read for any party are not the same with the fetched for the specific party.",
      )

      assert(
        activeContracts == activeContractsPartyWildcardAndSpecificParty,
        s"Active contracts with both filters used are not the same with the fetched for the specific party.",
      )

    }
  })

  test(
    "ACSfilterContracts",
    "The ActiveContractService should return contracts filtered by templateId",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      offsetAtTestStart <- ledger.currentEnd()
      (dummy, dummyWithParam, dummyFactory) <- createDummyContracts(party, ledger)
      activeContracts <- ledger.activeContractsByTemplateId(
        Seq(Dummy.TEMPLATE_ID),
        Some(Seq(party)),
      )
      activeContractsPartyWildcard <- ledger
        .activeContractsByTemplateId(Seq(Dummy.TEMPLATE_ID), None)
        .map(_.filter(_.offset > offsetAtTestStart))
      _ <- archive(ledger, party)(dummy, dummyWithParam, dummyFactory)
    } yield {
      assert(
        activeContracts.size == 1,
        s"Expected 1 contract, but received ${activeContracts.size}.",
      )

      assert(
        activeContracts.head.getTemplateId == Identifier.fromJavaProto(
          Dummy.TEMPLATE_ID_WITH_PACKAGE_ID.toProto
        ),
        s"Received contract is not of type Dummy, but ${activeContracts.head.templateId}.",
      )
      assert(
        activeContracts.head.contractId == dummy.contractId,
        s"Expected contract with contractId $dummy, but received ${activeContracts.head.contractId}.",
      )

      assert(
        activeContracts == activeContractsPartyWildcard,
        "Active contracts read for any party are not the same as for the specific party.\n" +
          s"Active contracts read for any party: $activeContractsPartyWildcard\n" +
          s" active contracts for the specific party: $activeContracts",
      )

    }
  })

  test(
    "ACSarchivedContracts",
    "The ActiveContractService does not return archived contracts",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      (dummy, dummyWithParam, dummyFactory) <- createDummyContracts(party, ledger)
      contractsBeforeExercise <- ledger.activeContracts(Some(Seq(party)))
      _ <- ledger.exercise(party, dummy.exerciseDummyChoice1())
      contractsAfterExercise <- ledger.activeContracts(Some(Seq(party)))
      _ <- ledger.exercise(party, dummyWithParam.exerciseArchive())
      _ <- ledger.exercise(party, dummyFactory.exerciseArchive())
    } yield {
      // check the contracts BEFORE the exercise
      assert(
        contractsBeforeExercise.size == 3,
        s"Expected 3 contracts, but received ${contractsBeforeExercise.size}.",
      )

      assert(
        contractsBeforeExercise.exists(_.contractId == dummy.contractId),
        s"Expected to receive contract with contractId $dummy, but received ${contractsBeforeExercise
            .map(_.contractId)
            .mkString(", ")} instead.",
      )

      // check the contracts AFTER the exercise
      assert(
        contractsAfterExercise.size == 2,
        s"Expected 2 contracts, but received ${contractsAfterExercise.size}",
      )

      assert(
        !contractsAfterExercise.exists(_.contractId == dummy.contractId),
        s"Expected to not receive contract with contractId $dummy.",
      )
    }
  })

  test(
    "ACSusableOffset",
    "The ActiveContractService should return a usable offset to resume streaming transactions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummy <- ledger.create(party, new Dummy(party))
      offset <- ledger.currentEnd()
      onlyDummy <- ledger.activeContracts(
        ledger.activeContractsRequest(Some(Seq(party)), offset)
      )
      dummyWithParam <- ledger.create(party, new DummyWithParam(party))
      request <- ledger.getTransactionsRequest(ledger.transactionFormat(Some(Seq(party))))
      fromOffset = request.update(_.beginExclusive := offset)
      transactions <- ledger.transactions(fromOffset)
      _ <- ledger.exercise(party, dummy.exerciseArchive())
      _ <- ledger.exercise(party, dummyWithParam.exerciseArchive())
    } yield {
      assert(onlyDummy.size == 1)
      assert(
        onlyDummy.exists(_.contractId == dummy.contractId),
        s"Expected to receive $dummy in active contracts, but didn't receive it.",
      )

      assert(
        transactions.size == 1,
        s"Expected to receive only 1 transaction from offset $offset, but received ${transactions.size}.",
      )

      val transaction = transactions.head
      assert(
        transaction.events.size == 1,
        s"Expected only 1 event in the transaction, but received ${transaction.events.size}.",
      )

      val createdEvent = transaction.events.collect { case Event(Created(createdEvent)) =>
        createdEvent
      }
      assert(
        createdEvent.exists(_.contractId == dummyWithParam.contractId),
        s"Expected a CreateEvent for $dummyWithParam, but received $createdEvent.",
      )
    }
  })

  test(
    "ACSverbosity",
    "The ActiveContractService should emit field names only if the verbose flag is set to true",
    allocate(SingleParty),
    runConcurrently = false,
    limitation = TestConstraints.GrpcOnly("Labels are always emitted by Transcode/SchemaProcessor"),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummy <- ledger.create(party, new Dummy(party))
      end <- ledger.currentEnd()
      verboseRequest = ledger
        .activeContractsRequest(Some(Seq(party)), end)
        .update(_.eventFormat.verbose := true)
      nonVerboseRequest = verboseRequest.update(_.eventFormat.verbose := false)
      verboseEvents <- ledger.activeContracts(verboseRequest)
      nonVerboseEvents <- ledger.activeContracts(nonVerboseRequest)
      verboseEventsPartyWildcard <- ledger.activeContracts(
        ledger.activeContractsRequest(None, end).update(_.eventFormat.verbose := true)
      )
      nonVerboseEventsPartyWildcard <- ledger.activeContracts(
        ledger.activeContractsRequest(None, end).update(_.eventFormat.verbose := false)
      )
      _ <- ledger.exercise(party, dummy.exerciseArchive())
    } yield {
      val verboseCreateArgs = verboseEvents.map(_.getCreateArguments).flatMap(_.fields)
      assert(
        verboseEvents.nonEmpty && verboseCreateArgs.forall(_.label.nonEmpty),
        s"$party expected a contract with labels, but received $verboseEvents.",
      )

      val nonVerboseCreateArgs = nonVerboseEvents.map(_.getCreateArguments).flatMap(_.fields)
      assert(
        nonVerboseEvents.nonEmpty && nonVerboseCreateArgs.forall(_.label.isEmpty),
        s"$party expected a contract without labels, but received $nonVerboseEvents.",
      )

      assert(
        verboseEvents == verboseEventsPartyWildcard,
        s"""Verbose events for any party are not the same as for specific party.
           |Verbose events for any party: $verboseEventsPartyWildcard
           |Verbose events for the specific party. $verboseEvents""".stripMargin,
      )
      assert(
        nonVerboseEvents == nonVerboseEventsPartyWildcard,
        s"""Non verbose events for any party are not the same as for specific party.
           |Non verbose events for any party: $nonVerboseEventsPartyWildcard.
           |Non verbose events for the specific party: $nonVerboseEvents.""".stripMargin,
      )

    }
  })

  test(
    "ACSmultiParty",
    "The ActiveContractsService should return contracts for the requesting parties",
    allocate(TwoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    for {
      offsetAtTestStart <- ledger.currentEnd()
      dummyAlice <- createDummyContracts(alice, ledger)
      dummyBob <- createDummyContracts(bob, ledger)
      allContractsForAlice <- ledger.activeContracts(Some(Seq(alice)))
      allContractsForBob <- ledger.activeContracts(Some(Seq(bob)))
      allContractsForAliceAndBob <- ledger.activeContracts(Some(Seq(alice, bob)))
      allContractsPartyWildcard <- ledger
        .activeContracts(None)
        .map(_.filter(_.offset > offsetAtTestStart))
      dummyContractsForAlice <- ledger.activeContractsByTemplateId(
        Seq(Dummy.TEMPLATE_ID),
        Some(Seq(alice)),
      )
      dummyContractsForAliceAndBob <- ledger.activeContractsByTemplateId(
        Seq(Dummy.TEMPLATE_ID),
        Some(Seq(alice, bob)),
      )
      dummyContractsPartyWildcard <- ledger
        .activeContractsByTemplateId(
          templateIds = Seq(Dummy.TEMPLATE_ID),
          parties = None,
        )
        .map(_.filter(_.offset > offsetAtTestStart))
      _ <- (archive(ledger, alice) _).tupled(dummyAlice)
      _ <- (archive(ledger, bob) _).tupled(dummyBob)
    } yield {
      assert(
        allContractsForAlice.size == 3,
        s"$alice expected 3 events, but received ${allContractsForAlice.size}.",
      )
      assertTemplates(Seq(alice), allContractsForAlice, Dummy.TEMPLATE_ID_WITH_PACKAGE_ID, 1)
      assertTemplates(
        Seq(alice),
        allContractsForAlice,
        DummyWithParam.TEMPLATE_ID_WITH_PACKAGE_ID,
        1,
      )
      assertTemplates(Seq(alice), allContractsForAlice, DummyFactory.TEMPLATE_ID_WITH_PACKAGE_ID, 1)

      assert(
        allContractsForBob.size == 3,
        s"$bob expected 3 events, but received ${allContractsForBob.size}.",
      )
      assertTemplates(Seq(bob), allContractsForBob, Dummy.TEMPLATE_ID_WITH_PACKAGE_ID, 1)
      assertTemplates(Seq(bob), allContractsForBob, DummyWithParam.TEMPLATE_ID_WITH_PACKAGE_ID, 1)
      assertTemplates(Seq(bob), allContractsForBob, DummyFactory.TEMPLATE_ID_WITH_PACKAGE_ID, 1)

      assert(
        allContractsForAliceAndBob.size == 6,
        s"$alice and $bob expected 6 events, but received ${allContractsForAliceAndBob.size}.",
      )
      assertTemplates(
        Seq(alice, bob),
        allContractsForAliceAndBob,
        Dummy.TEMPLATE_ID_WITH_PACKAGE_ID,
        2,
      )
      assertTemplates(
        Seq(alice, bob),
        allContractsForAliceAndBob,
        DummyWithParam.TEMPLATE_ID_WITH_PACKAGE_ID,
        2,
      )
      assertTemplates(
        Seq(alice, bob),
        allContractsForAliceAndBob,
        DummyFactory.TEMPLATE_ID_WITH_PACKAGE_ID,
        2,
      )

      assert(
        allContractsForAliceAndBob == allContractsPartyWildcard,
        s"""Active contracts for any party are not the same as for specific parties.
           |Active contracts for any party: $allContractsPartyWildcard.
           |Active contracts for specific parties: $allContractsForAliceAndBob.""".stripMargin,
      )

      assert(
        dummyContractsForAlice.size == 1,
        s"$alice expected 1 event, but received ${dummyContractsForAlice.size}.",
      )
      assertTemplates(Seq(alice), dummyContractsForAlice, Dummy.TEMPLATE_ID_WITH_PACKAGE_ID, 1)

      assert(
        dummyContractsForAliceAndBob.size == 2,
        s"$alice and $bob expected 2 events, but received ${dummyContractsForAliceAndBob.size}.",
      )
      assertTemplates(
        Seq(alice, bob),
        dummyContractsForAliceAndBob,
        Dummy.TEMPLATE_ID_WITH_PACKAGE_ID,
        2,
      )

      assert(
        dummyContractsForAliceAndBob == dummyContractsPartyWildcard,
        s"""Active Dummy contracts for any party are not the same as for specific parties.
           |Active Dummy contracts for any party: $dummyContractsPartyWildcard.
           |Active Dummy contracts for specific parties: $dummyContractsForAliceAndBob.""".stripMargin,
      )

    }
  })

  test(
    "ACSEventOffset",
    "The ActiveContractService should properly fill the offset field of the events",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummy <- ledger.create(party, new Dummy(party))
      Vector(dummyEvent) <- ledger.activeContracts(Some(Seq(party)))
      offset <- ledger.currentEnd()
      acsDeltaTransaction <- ledger.transactionByOffset(dummyEvent.offset, Seq(party), AcsDelta)
      ledgerEffectsTransaction <- ledger.transactionByOffset(
        dummyEvent.offset,
        Seq(party),
        LedgerEffects,
      )
      _ <- ledger.exercise(party, dummy.exerciseArchive())
    } yield {
      assert(
        acsDeltaTransaction.updateId == ledgerEffectsTransaction.updateId,
        s"Offset ${dummyEvent.offset} did not resolve to the same acs delta transaction (${acsDeltaTransaction.updateId}) and ledger effects transaction (${ledgerEffectsTransaction.updateId}).",
      )
      assert(
        dummyEvent.offset == acsDeltaTransaction.offset,
        s"Active contract's created event ${dummyEvent.offset} was not equal to transaction's offset $offset.",
      )
      assert(
        acsDeltaTransaction.events.sizeIs == 1,
        "Expected only a single event for flat transaction",
      )
      acsDeltaTransaction.events
        .flatMap(_.event.created)
        .foreach(createdEvent =>
          assert(
            acsDeltaTransaction.offset == createdEvent.offset,
            s"AcsDelta transaction's created event ${dummyEvent.offset} was not equal to transaction's offset ${acsDeltaTransaction.offset}.",
          )
        )
      assert(
        ledgerEffectsTransaction.events.sizeIs == 1,
        "Expected only a single event for LedgerEffects transaction",
      )
      ledgerEffectsTransaction.events
        .flatMap(_.event.created)
        .foreach(createdEvent =>
          assert(
            ledgerEffectsTransaction.offset == createdEvent.offset,
            s"LedgerEffects' transaction created event ${dummyEvent.offset} was not equal to transaction's offset ${ledgerEffectsTransaction.offset}.",
          )
        )

      ledgerEffectsTransaction.events
        .flatMap(_.event.created)
        .zip(
          acsDeltaTransaction.events
            .flatMap(_.event.created)
        )
        .foreach { case (ledgerEffectsEvent, acsDeltaEvent) =>
          assert(
            ledgerEffectsEvent.nodeId == acsDeltaEvent.nodeId,
            s"LedgerEffects' transaction event (at offset ${ledgerEffectsEvent.offset}) node id ${ledgerEffectsEvent.nodeId} was not " +
              s"equal to AcsDelta transaction's event (at offset ${acsDeltaEvent.offset}) node id ${acsDeltaEvent.nodeId}.",
          )
        }
    }
  })

  test(
    "ACSnoWitnessedContracts",
    "The ActiveContractService should not return witnessed contracts",
    allocate(TwoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    for {
      offsetAtTestStart <- ledger.currentEnd()
      witnesses: TestWitnesses.ContractId <- ledger.create(
        alice,
        new TestWitnesses(alice, bob, bob),
      )
      newWitnesses <- ledger.exerciseAndGetContract[TestWitnesses.ContractId, TestWitnesses](
        bob,
        witnesses.exerciseWitnessesCreateNewWitnesses(),
      )
      bobContracts <- ledger.activeContracts(Some(Seq(bob)))
      aliceContracts <- ledger.activeContracts(Some(Seq(alice)))
      partyWildcardContracts <- ledger
        .activeContracts(None)
        .map(_.filter(_.offset > offsetAtTestStart))
      _ <- ledger.exercise(alice, witnesses.exerciseArchive())
      _ <- ledger.exercise(bob, newWitnesses.exerciseArchive())
    } yield {
      assert(
        bobContracts.size == 2,
        s"Expected to receive 2 active contracts for $bob, but received ${bobContracts.size}.",
      )
      assert(
        aliceContracts.size == 1,
        s"Expected to receive 1 active contracts for $alice, but received ${aliceContracts.size}.",
      )
      assert(
        partyWildcardContracts.size == 2,
        s"Expected to receive 2 active contracts for all parties, but received ${partyWildcardContracts.size}.",
      )
    }
  })

  test(
    "ACSnoDivulgedContracts",
    "The ActiveContractService should not return divulged contracts",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    for {
      divulgence1 <- ledger.create(alice, new Divulgence1(alice))
      divulgence2 <- ledger.create(bob, new Divulgence2(bob, alice))
      _ <- ledger.exercise(alice, divulgence2.exerciseDivulgence2Fetch(divulgence1))
      bobContracts <- ledger.activeContracts(Some(Seq(bob)))
      aliceContracts <- ledger.activeContracts(Some(Seq(alice)))
      _ <- ledger.exercise(alice, divulgence1.exerciseArchive())
      _ <- ledger.exercise(bob, divulgence2.exerciseArchive())
    } yield {
      assert(
        bobContracts.size == 1,
        s"Expected to receive 1 active contracts for $bob, but received ${bobContracts.size}.",
      )
      assert(
        aliceContracts.size == 2,
        s"Expected to receive 2 active contracts for $alice, but received ${aliceContracts.size}.",
      )
    }
  })

  test(
    "ACSnoSignatoryObservers",
    "The ActiveContractService should not return overlapping signatories and observers",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    for {
      contract <- ledger.create(
        alice,
        new WithObservers(alice, Seq(alice, bob).map(_.getValue).asJava),
      )
      contracts <- ledger.activeContracts(Some(Seq(alice)))
      _ <- ledger.exercise(alice, contract.exerciseArchive())
    } yield {
      assert(contracts.nonEmpty)
      contracts.foreach(ce =>
        assert(
          ce.observers == Seq(bob.getValue),
          s"Expected observers to only contain $bob, but received ${ce.observers}",
        )
      )
    }
  })

  test(
    "ACFilterWitnesses",
    "The ActiveContractService should filter witnesses by the transaction filter",
    allocate(Parties(3)),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie))) =>
    for {
      offsetAtTestStart <- ledger.currentEnd()
      contract <- ledger.create(
        alice,
        new WithObservers(alice, List(bob, charlie).map(_.getValue).asJava),
      )
      bobContracts <- ledger.activeContracts(Some(Seq(bob)))
      aliceBobContracts <- ledger.activeContracts(Some(Seq(alice, bob)))
      bobCharlieContracts <- ledger.activeContracts(Some(Seq(bob, charlie)))
      partyWildcardContracts <- ledger
        .activeContracts(None)
        .map(_.filter(_.offset > offsetAtTestStart))
      _ <- ledger.exercise(alice, contract.exerciseArchive())
    } yield {
      def assertWitnesses(contracts: Vector[CreatedEvent], requesters: Set[Party]): Unit = {
        assert(
          contracts.size == 1,
          s"Expected to receive 1 active contracts for $requesters, but received ${contracts.size}.",
        )
        assert(
          contracts.head.witnessParties.toSet == requesters.map(_.getValue),
          s"Expected witness parties to equal to $requesters, but received ${contracts.head.witnessParties}",
        )
      }

      assertWitnesses(bobContracts, Set(bob))
      assertWitnesses(aliceBobContracts, Set(alice, bob))
      assertWitnesses(bobCharlieContracts, Set(bob, charlie))
      assertWitnesses(partyWildcardContracts, Set(alice, bob, charlie))
    }
  })

  test(
    "ACSFilterCombinations",
    "Testing ACS filter combinations",
    allocate(Parties(3)),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(p1, p2, p3))) =>
    // Let us have 3 templates
    val templateIds: Vector[Identifier] =
      Vector(TriAgreement.TEMPLATE_ID, TriProposal.TEMPLATE_ID, WithObservers.TEMPLATE_ID).map(t =>
        Identifier.fromJavaProto(t.toProto)
      )
    // Let us have 3 parties
    val parties: Vector[Party] = Vector(p1, p2, p3)
    // Let us have all combinations for the 3 parties
    val partyCombinations =
      Vector(Set(0), Set(1), Set(2), Set(0, 1), Set(1, 2), Set(0, 2), Set(0, 1, 2))
    // Let us populate 3 contracts for each template/partyCombination pair (see createContracts below)
    // Then we require the following Filter - Expectations to be upheld

    // Key is the index of a test party (see parties), if None the wildcard-party is used
    // Value is
    //   either empty, meaning a template-wildcard filter
    //   or the Set of indices of a test template (see templateIds)
    val * = Set.empty[Int]
    val w = -1
    type ACSFilter = Map[Option[Int], Set[Int]]

    case class FilterCoord(templateId: Int, stakeholders: Set[Int])

    def filterCoordsForFilter(filter: ACSFilter): Set[FilterCoord] =
      (for {
        (partyO, templates) <- filter
        templateId <- if (templates.isEmpty) templateIds.indices.toSet else templates
        allowedPartyCombination <- partyO.fold(partyCombinations)(party =>
          partyCombinations.filter(_(party))
        )
      } yield FilterCoord(templateId, allowedPartyCombination)).toSet

    val filters: Vector[ACSFilter] =
      Vector(
        Map(0 -> *),
        Map(0 -> Set(0)),
        Map(0 -> Set(1)),
        Map(0 -> Set(2)),
        Map(0 -> Set(0, 1)),
        Map(0 -> Set(0, 2)),
        Map(0 -> Set(1, 2)),
        Map(0 -> Set(0, 1, 2)),
        // multi filter
        Map(0 -> *, 1 -> *),
        Map(0 -> *, 2 -> *),
        Map(0 -> *, 1 -> *, 2 -> *),
        Map(0 -> Set(0), 1 -> Set(1)),
        Map(0 -> Set(0), 1 -> Set(1), 2 -> Set(2)),
        Map(0 -> Set(0, 1), 1 -> Set(0, 2)),
        Map(0 -> Set(0, 1), 1 -> Set(0, 2), 2 -> Set(1, 2)),
        Map(0 -> *, 1 -> Set(0)),
        Map(0 -> *, 1 -> Set(0), 2 -> Set(1, 2)),
        Map(0 -> *),
        // party-wildcard
        Map(w -> Set(0)),
        Map(w -> Set(1)),
        Map(w -> Set(2)),
        Map(w -> Set(0, 1)),
        Map(w -> Set(0, 2)),
        Map(w -> Set(1, 2)),
        Map(w -> Set(0, 1, 2)),
        // multi filter w/ party-wildcard
        Map(w -> *, 1 -> *),
        Map(0 -> *, w -> *),
        Map(0 -> *, w -> *, 2 -> *),
        Map(w -> Set(0), 1 -> Set(1)),
        Map(0 -> Set(0), w -> Set(1)),
        Map(0 -> Set(0), 1 -> Set(1), w -> Set(2)),
        Map(0 -> Set(0, 1), w -> Set(0, 2)),
        Map(w -> Set(0, 1), 1 -> Set(0, 2), 2 -> Set(1, 2)),
        Map(w -> *, 1 -> Set(0)),
        Map(0 -> *, w -> Set(0)),
        Map(w -> *, 1 -> Set(0), 2 -> Set(1, 2)),
        Map(0 -> *, w -> Set(0), 2 -> Set(1, 2)),
        Map(0 -> *, 1 -> Set(0), w -> Set(1, 2)),
        Map(0 -> *, w -> Set(0), 2 -> Set(1, 2)),
      ).map(_.map { case (k, v) => (Option.when(k >= 0)(k), v) })
    val fixtures: Vector[(ACSFilter, Set[FilterCoord])] =
      filters.map(filter => filter -> filterCoordsForFilter(filter))

    def createContracts: Future[Map[FilterCoord, Set[String]]] = {
      def withThreeParties[T <: Template](
          f: (String, String, String) => T
      )(partySet: Set[Party]): T =
        partySet.toList.map(_.getValue) match {
          case a :: b :: c :: Nil => f(a, b, c)
          case a :: b :: Nil => f(a, b, b)
          case a :: Nil => f(a, a, a)
          case invalid =>
            throw new Exception(s"Invalid partySet, length must be 1 or 2 or 3 but it was $invalid")
        }

      val templateFactories: Vector[Set[Party] => ? <: Template] =
        Vector(
          (parties: Set[Party]) => withThreeParties(new TriAgreement(_, _, _))(parties),
          (parties: Set[Party]) => withThreeParties(new TriProposal(_, _, _))(parties),
          (parties: Set[Party]) =>
            new WithObservers(parties.head, parties.toList.map(_.getValue).asJava),
        )

      def createContractFor(
          template: Int,
          partyCombination: Int,
      ): Future[ContractId[?]] = {
        val partiesSet = partyCombinations(partyCombination).map(parties)
        templateFactories(template)(partiesSet) match {
          case agreement: TriAgreement =>
            ledger.create(
              partiesSet.toList,
              partiesSet.toList,
              agreement,
            )(TriAgreement.COMPANION)
          case proposal: TriProposal =>
            ledger.create(
              partiesSet.toList,
              partiesSet.toList,
              proposal,
            )(TriProposal.COMPANION)
          case withObservers: WithObservers =>
            ledger.create(
              partiesSet.toList,
              partiesSet.toList,
              withObservers,
            )(WithObservers.COMPANION)
          case t => throw new RuntimeException(s"the template given has an unexpected type $t")
        }
      }

      val createFs = for {
        partyCombinationIndex <- partyCombinations.indices
        templateIndex <- templateFactories.indices
        _ <- 1 to 3
      } yield createContractFor(templateIndex, partyCombinationIndex)
        .map((partyCombinationIndex, templateIndex) -> _)

      Future
        .sequence(createFs)
        .map(_.groupBy(_._1).map { case ((partyCombinationIndex, templateIndex), contractIds) =>
          (
            FilterCoord(templateIndex, partyCombinations(partyCombinationIndex)),
            contractIds.view.map(_._2.contractId).toSet,
          )
        })
    }

    val random = new Random(System.nanoTime())

    def testForFixtures(
        fixtures: Vector[(ACSFilter, Set[FilterCoord])],
        allContracts: Map[FilterCoord, Set[String]],
        offsetAtTestStart: Long,
    ) = {
      def filtersForTemplates(templates: Set[Int]) =
        Filters(
          random
            .shuffle(templates.toSeq.map(templateIds))
            .map(templateId =>
              CumulativeFilter(
                IdentifierFilter.TemplateFilter(TemplateFilter(Some(templateId), false))
              )
            )
        )

      def activeContractIdsFor(filter: ACSFilter): Future[Vector[String]] =
        for {
          end <- ledger.currentEnd()
          filtersByParty = filter
            .collect {
              case (Some(party), templates) if templates.isEmpty =>
                (parties(party).getValue, Filters(Seq.empty))
              case (Some(party), templates) =>
                (
                  parties(party).getValue,
                  filtersForTemplates(templates),
                )
            }
          filtersForAnyParty = filter.get(None) match {
            // no party-wildcard defined
            case None => None
            case Some(templates) if templates.isEmpty =>
              Some(
                Filters(
                  Seq(
                    CumulativeFilter.defaultInstance
                      .withWildcardFilter(WildcardFilter(false))
                  )
                )
              )
            case Some(templates) => Some(filtersForTemplates(templates))
          }
          createdEvents <- ledger
            .activeContracts(
              GetActiveContractsRequest(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = filtersByParty,
                    filtersForAnyParty = filtersForAnyParty,
                    verbose = true,
                  )
                ),
                activeAtOffset = end,
              )
            )
            .map(_.filter(_.offset > offsetAtTestStart))
        } yield createdEvents.map(_.contractId)

      def testForFixture(actual: Vector[String], expected: Set[FilterCoord], hint: String): Unit = {
        val actualSet = actual.toSet
        assert(
          expected.forall(allContracts.contains),
          s"$hint expected FilterCoord(s) which do not exist(s): ${expected.filterNot(allContracts.contains)}",
        )
        assert(
          actualSet.size == actual.size,
          s"$hint ACS returned redundant entries ${actual.groupBy(identity).toList.filter(_._2.size > 1).map(_._1).mkString("\n")}",
        )
        val errors = allContracts.toList.flatMap {
          case (filterCoord, contracts) if expected(filterCoord) && contracts.forall(actualSet) =>
            Nil
          case (filterCoord, contracts)
              if expected(filterCoord) && contracts.forall(x => !actualSet(x)) =>
            List(s"$filterCoord is missing from result")
          case (filterCoord, _) if expected(filterCoord) =>
            List(s"$filterCoord is partially missing from result")
          case (filterCoord, contracts) if contracts.forall(actualSet) =>
            List(s"$filterCoord is present (too many contracts in result)")
          case (filterCoord, contracts) if contracts.exists(actualSet) =>
            List(s"$filterCoord is partially present (too many contracts in result)")
          case (_, _) => Nil
        }
        assert(errors == Nil, s"$hint ACS mismatch: ${errors.mkString(", ")}")
        val expectedContracts = expected.view.flatMap(allContracts).toSet
        // This extra, redundant test is to safeguard the above, more fine grained approach
        assert(
          expectedContracts == actualSet,
          s"$hint ACS mismatch\n Extra contracts: ${actualSet -- expectedContracts}\n Missing contracts: ${expectedContracts -- actualSet}",
        )
      }

      val testFs = fixtures.map { case (filter, expectedResultCoords) =>
        activeContractIdsFor(filter).map(
          testForFixture(_, expectedResultCoords, s"Filter: $filter")
        )
      }
      Future.sequence(testFs)
    }

    for {
      offsetAtTestStart <- ledger.currentEnd()
      allContracts <- createContracts
      _ <- testForFixtures(fixtures, allContracts, offsetAtTestStart)
    } yield ()
  })

  test(
    "ActiveAtOffsetInfluencesAcs",
    "Allow to specify optional active_at_offset",
    partyAllocation = allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val eventFormat =
      Some(
        EventFormat(
          filtersByParty = Map(party.getValue -> Filters(Nil)),
          filtersForAnyParty = None,
          verbose = false,
        )
      )
    for {
      c1 <- ledger.create(party, new Dummy(party))
      offset1 <- ledger.currentEnd()
      c2 <- ledger.create(party, new Dummy(party))
      offset2 <- ledger.currentEnd()
      // acs at offset1
      acs1 <- ledger
        .activeContractsIds(
          GetActiveContractsRequest(
            eventFormat = eventFormat,
            activeAtOffset = offset1,
          )
        )
      _ = assertEquals("acs1", acs1.toSet, Set(c1))
      // acs at offset2
      acs2 <- ledger
        .activeContractsIds(
          GetActiveContractsRequest(
            eventFormat = eventFormat,
            activeAtOffset = offset2,
          )
        )
      _ = assertEquals("ACS at the second offset", acs2.toSet, Set(c1, c2))
    } yield ()
  })

  test(
    "AcsAtPruningOffsetIsAllowed",
    "Allow requesting ACS at the pruning offset",
    partyAllocation = allocate(SingleParty),
    runConcurrently = false,
    limitation = TestConstraints.GrpcOnly("Pruning not available in JSON API"),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val eventFormat =
      Some(
        EventFormat(
          filtersByParty = Map(party.getValue -> Filters(Nil)),
          filtersForAnyParty = None,
          verbose = false,
        )
      )
    for {
      c1 <- ledger.create(party, new Dummy(party))
      anOffset <- ledger.currentEnd()
      _ <- ledger.create(party, new Dummy(party))
      _ <- ledger.pruneCantonSafe(
        pruneUpTo = anOffset,
        party = party,
        dummyCommand = p => new Dummy(p).create.commands,
      )
      acs <- ledger
        .activeContractsIds(
          GetActiveContractsRequest(
            eventFormat = eventFormat,
            activeAtOffset = anOffset,
          )
        )
      _ = assertEquals("acs valid_at at pruning offset", acs.toSet, Set(c1))
    } yield ()
  })

  test(
    "AcsBeforePruningOffsetIsDisallowed",
    "Fail when requesting ACS before the pruning offset",
    partyAllocation = allocate(SingleParty),
    runConcurrently = false,
    limitation = TestConstraints.GrpcOnly("Pruning not available in JSON API"),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val eventFormat =
      Some(
        EventFormat(
          filtersByParty = Map(party.getValue -> Filters(Nil)),
          filtersForAnyParty = None,
          verbose = false,
        )
      )
    for {
      offset1 <- ledger.currentEnd()
      _ <- ledger.create(party, new Dummy(party))
      offset2 <- ledger.currentEnd()
      _ <- ledger.create(party, new Dummy(party))
      _ <- ledger.pruneCantonSafe(
        pruneUpTo = offset2,
        party = party,
        p => new Dummy(p).create.commands,
      )
      _ <- ledger
        .activeContractsIds(
          GetActiveContractsRequest(
            eventFormat = eventFormat,
            activeAtOffset = offset1,
          )
        )
        .mustFailWith(
          "ACS before the pruning offset",
          RequestValidationErrors.ParticipantPrunedDataAccessed,
        )
    } yield ()
  })

  test(
    "ActiveAtOffsetInvalidInput",
    "Fail when active_at_offset has invalid input",
    partyAllocation = allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val eventFormat =
      Some(
        EventFormat(
          filtersByParty = Map(party.getValue -> Filters(Nil)),
          filtersForAnyParty = None,
          verbose = false,
        )
      )
    for {
      _ <- ledger
        .activeContracts(
          GetActiveContractsRequest(
            eventFormat = eventFormat,
            activeAtOffset = -1L,
          )
        )
        .mustFailWith(
          "invalid offset",
          RequestValidationErrors.NegativeOffset,
        )
    } yield ()
  })

  test(
    "ActiveAtOffsetAfterLedgerEnd",
    "Fail when active_at_offset is after the ledger end offset",
    partyAllocation = allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val eventFormat =
      Some(
        EventFormat(
          filtersByParty = Map(party.getValue -> Filters(Nil)),
          filtersForAnyParty = None,
          verbose = false,
        )
      )
    for {
      offsetBeyondLedgerEnd <- ledger.offsetBeyondLedgerEnd()
      _ <- ledger
        .activeContracts(
          GetActiveContractsRequest(
            eventFormat = eventFormat,
            activeAtOffset = offsetBeyondLedgerEnd,
          )
        )
        .mustFailWith(
          "offset after ledger end",
          RequestValidationErrors.OffsetAfterLedgerEnd,
        )
    } yield ()
  })

  private def createDummyContracts(party: Party, ledger: ParticipantTestContext)(implicit
      ec: ExecutionContext
  ): Future[
    (
        Dummy.ContractId,
        DummyWithParam.ContractId,
        DummyFactory.ContractId,
    )
  ] =
    for {
      dummy <- ledger.create(party, new Dummy(party))
      dummyWithParam <- ledger.create(party, new DummyWithParam(party))
      dummyFactory <- ledger.create(party, new DummyFactory(party))
    } yield (dummy, dummyWithParam, dummyFactory)

  private def assertTemplates(
      party: Seq[Party],
      events: Vector[CreatedEvent],
      templateId: JavaIdentifier,
      count: Int,
  ): Unit = {
    val templateEvents =
      events.count(_.getTemplateId == Identifier.fromJavaProto(templateId.toProto))
    assert(
      templateEvents == count,
      s"${party.mkString(" and ")} expected $count $templateId events, but received $templateEvents.",
    )
  }

  private def archive(
      ledger: ParticipantTestContext,
      party: Party,
  )(
      dummy: Dummy.ContractId,
      dummyWithParam: DummyWithParam.ContractId,
      dummyFactory: DummyFactory.ContractId,
  )(implicit ec: ExecutionContext) =
    for {
      _ <- ledger.exercise(party, dummy.exerciseArchive())
      _ <- ledger.exercise(party, dummyWithParam.exerciseArchive())
      _ <- ledger.exercise(party, dummyFactory.exerciseArchive())
    } yield ()

}
