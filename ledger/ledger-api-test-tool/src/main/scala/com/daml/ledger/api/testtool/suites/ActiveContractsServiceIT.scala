// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsRequest
import com.daml.ledger.api.v1.event.Event.Event.Created
import com.daml.ledger.api.v1.event.{CreatedEvent, Event}
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.binding.Primitive.{Party, TemplateId}
import com.daml.ledger.client.binding.Template
import com.daml.ledger.test.model.Test.Divulgence2._
import com.daml.ledger.test.model.Test.Dummy._
import com.daml.ledger.test.model.Test.Witnesses._
import com.daml.ledger.test.model.Test.{
  Divulgence1,
  Divulgence2,
  Dummy,
  DummyFactory,
  DummyWithParam,
  TriAgreement,
  TriProposal,
  WithObservers,
  Witnesses => TestWitnesses,
}
import io.grpc.Status
import scalaz.syntax.tag._

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}

class ActiveContractsServiceIT extends LedgerTestSuite {
  test(
    "ACSinvalidLedgerId",
    "The ActiveContractService should fail for requests with an invalid ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, parties @ _*)) =>
    val invalidLedgerId = "ACSinvalidLedgerId"
    val invalidRequest = ledger
      .activeContractsRequest(parties)
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger.activeContracts(invalidRequest).mustFail("retrieving active contracts")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, Some("not found. Actual Ledger ID"))
    }
  })

  test(
    "ACSemptyResponse",
    "The ActiveContractService should succeed with an empty response if no contracts have been created for a party",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      activeContracts <- ledger.activeContracts(party)
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
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      (dummy, dummyWithParam, dummyFactory) <- createDummyContracts(party, ledger)
      activeContracts <- ledger.activeContracts(party)
    } yield {
      assert(
        activeContracts.size == 3,
        s"Expected 3 contracts, but received ${activeContracts.size}.",
      )

      assert(
        activeContracts.exists(_.contractId == dummy),
        s"Didn't find Dummy contract with contractId $dummy.",
      )
      assert(
        activeContracts.exists(_.contractId == dummyWithParam),
        s"Didn't find DummyWithParam contract with contractId $dummy.",
      )
      assert(
        activeContracts.exists(_.contractId == dummyFactory),
        s"Didn't find DummyFactory contract with contractId $dummy.",
      )

      val invalidSignatories = activeContracts.filterNot(_.signatories == Seq(party.unwrap))
      assert(
        invalidSignatories.isEmpty,
        s"Found contracts with signatories other than $party: $invalidSignatories",
      )

      val invalidObservers = activeContracts.filterNot(_.observers.isEmpty)
      assert(
        invalidObservers.isEmpty,
        s"Found contracts with non-empty observers: $invalidObservers",
      )
    }
  })

  test(
    "ACSfilterContracts",
    "The ActiveContractService should return contracts filtered by templateId",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      (dummy, _, _) <- createDummyContracts(party, ledger)
      activeContracts <- ledger.activeContractsByTemplateId(Seq(Dummy.id.unwrap), party)
    } yield {
      assert(
        activeContracts.size == 1,
        s"Expected 1 contract, but received ${activeContracts.size}.",
      )

      assert(
        activeContracts.head.getTemplateId == Dummy.id.unwrap,
        s"Received contract is not of type Dummy, but ${activeContracts.head.templateId}.",
      )
      assert(
        activeContracts.head.contractId == dummy,
        s"Expected contract with contractId $dummy, but received ${activeContracts.head.contractId}.",
      )
    }
  })

  test(
    "ACSarchivedContracts",
    "The ActiveContractService does not return archived contracts",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      (dummy, _, _) <- createDummyContracts(party, ledger)
      contractsBeforeExercise <- ledger.activeContracts(party)
      _ <- ledger.exercise(party, dummy.exerciseDummyChoice1)
      contractsAfterExercise <- ledger.activeContracts(party)
    } yield {
      // check the contracts BEFORE the exercise
      assert(
        contractsBeforeExercise.size == 3,
        s"Expected 3 contracts, but received ${contractsBeforeExercise.size}.",
      )

      assert(
        contractsBeforeExercise.exists(_.contractId == dummy),
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
        !contractsAfterExercise.exists(_.contractId == dummy),
        s"Expected to not receive contract with contractId $dummy.",
      )
    }
  })

  test(
    "ACSusableOffset",
    "The ActiveContractService should return a usable offset to resume streaming transactions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      (Some(offset), onlyDummy) <- ledger.activeContracts(
        ledger.activeContractsRequest(Seq(party))
      )
      dummyWithParam <- ledger.create(party, DummyWithParam(party))
      request = ledger.getTransactionsRequest(Seq(party))
      fromOffset = request.update(_.begin := offset)
      transactions <- ledger.flatTransactions(fromOffset)
    } yield {
      assert(onlyDummy.size == 1)
      assert(
        onlyDummy.exists(_.contractId == dummy),
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
        createdEvent.exists(_.contractId == dummyWithParam),
        s"Expected a CreateEvent for $dummyWithParam, but received $createdEvent.",
      )
    }
  })

  test(
    "ACSverbosity",
    "The ActiveContractService should emit field names only if the verbose flag is set to true",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, Dummy(party))
      verboseRequest = ledger.activeContractsRequest(Seq(party)).update(_.verbose := true)
      nonVerboseRequest = verboseRequest.update(_.verbose := false)
      (_, verboseEvents) <- ledger.activeContracts(verboseRequest)
      (_, nonVerboseEvents) <- ledger.activeContracts(nonVerboseRequest)
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
    }
  })

  test(
    "ACSmultiParty",
    "The ActiveContractsService should return contracts for the requesting parties",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      _ <- createDummyContracts(alice, ledger)
      _ <- createDummyContracts(bob, ledger)
      allContractsForAlice <- ledger.activeContracts(alice)
      allContractsForBob <- ledger.activeContracts(bob)
      allContractsForAliceAndBob <- ledger.activeContracts(alice, bob)
      dummyContractsForAlice <- ledger.activeContractsByTemplateId(Seq(Dummy.id.unwrap), alice)
      dummyContractsForAliceAndBob <- ledger.activeContractsByTemplateId(
        Seq(Dummy.id.unwrap),
        alice,
        bob,
      )
    } yield {
      assert(
        allContractsForAlice.size == 3,
        s"$alice expected 3 events, but received ${allContractsForAlice.size}.",
      )
      assertTemplates(Seq(alice), allContractsForAlice, Dummy.id, 1)
      assertTemplates(Seq(alice), allContractsForAlice, DummyWithParam.id, 1)
      assertTemplates(Seq(alice), allContractsForAlice, DummyFactory.id, 1)

      assert(
        allContractsForBob.size == 3,
        s"$bob expected 3 events, but received ${allContractsForBob.size}.",
      )
      assertTemplates(Seq(bob), allContractsForBob, Dummy.id, 1)
      assertTemplates(Seq(bob), allContractsForBob, DummyWithParam.id, 1)
      assertTemplates(Seq(bob), allContractsForBob, DummyFactory.id, 1)

      assert(
        allContractsForAliceAndBob.size == 6,
        s"$alice and $bob expected 6 events, but received ${allContractsForAliceAndBob.size}.",
      )
      assertTemplates(Seq(alice, bob), allContractsForAliceAndBob, Dummy.id, 2)
      assertTemplates(Seq(alice, bob), allContractsForAliceAndBob, DummyWithParam.id, 2)
      assertTemplates(Seq(alice, bob), allContractsForAliceAndBob, DummyFactory.id, 2)

      assert(
        dummyContractsForAlice.size == 1,
        s"$alice expected 1 event, but received ${dummyContractsForAlice.size}.",
      )
      assertTemplates(Seq(alice), dummyContractsForAlice, Dummy.id, 1)

      assert(
        dummyContractsForAliceAndBob.size == 2,
        s"$alice and $bob expected 2 events, but received ${dummyContractsForAliceAndBob.size}.",
      )
      assertTemplates(Seq(alice, bob), dummyContractsForAliceAndBob, Dummy.id, 2)
    }
  })

  test(
    "ACSagreementText",
    "The ActiveContractService should properly fill the agreementText field",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummyCid <- ledger.create(party, Dummy(party))
      dummyWithParamCid <- ledger.create(party, DummyWithParam(party))
      contracts <- ledger.activeContracts(party)
    } yield {
      assert(contracts.size == 2, s"$party expected 2 contracts, but received ${contracts.size}.")
      val dummyAgreementText = contracts.collect {
        case ev if ev.contractId == dummyCid => ev.agreementText
      }
      val dummyWithParamAgreementText = contracts.collect {
        case ev if ev.contractId == dummyWithParamCid => ev.agreementText
      }

      assert(
        dummyAgreementText.exists(_.nonEmpty),
        s"$party expected a non-empty agreement text, but received $dummyAgreementText.",
      )
      assert(
        dummyWithParamAgreementText.exists(_.nonEmpty),
        s"$party expected an empty agreement text, but received $dummyWithParamAgreementText.",
      )
    }
  })

  test(
    "ACSeventId",
    "The ActiveContractService should properly fill the eventId field",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, Dummy(party))
      Vector(dummyEvent) <- ledger.activeContracts(party)
      flatTransaction <- ledger.flatTransactionByEventId(dummyEvent.eventId, party)
      transactionTree <- ledger.transactionTreeByEventId(dummyEvent.eventId, party)
    } yield {
      assert(
        flatTransaction.transactionId == transactionTree.transactionId,
        s"EventId ${dummyEvent.eventId} did not resolve to the same flat transaction (${flatTransaction.transactionId}) and transaction tree (${transactionTree.transactionId}).",
      )
    }
  })

  test(
    "ACSnoWitnessedContracts",
    "The ActiveContractService should not return witnessed contracts",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      witnesses <- ledger.create(alice, TestWitnesses(alice, bob, bob))
      _ <- ledger.exercise(bob, witnesses.exerciseWitnessesCreateNewWitnesses(_))
      bobContracts <- ledger.activeContracts(bob)
      aliceContracts <- ledger.activeContracts(alice)
    } yield {
      assert(
        bobContracts.size == 2,
        s"Expected to receive 2 active contracts for $bob, but received ${bobContracts.size}.",
      )
      assert(
        aliceContracts.size == 1,
        s"Expected to receive 1 active contracts for $alice, but received ${aliceContracts.size}.",
      )
    }
  })

  test(
    "ACSnoDivulgedContracts",
    "The ActiveContractService should not return divulged contracts",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      divulgence1 <- ledger.create(alice, Divulgence1(alice))
      divulgence2 <- ledger.create(bob, Divulgence2(bob, alice))
      _ <- ledger.exercise(alice, divulgence2.exerciseDivulgence2Fetch(_, divulgence1))
      bobContracts <- ledger.activeContracts(bob)
      aliceContracts <- ledger.activeContracts(alice)
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
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      _ <- ledger.create(alice, WithObservers(alice, Seq(alice, bob)))
      contracts <- ledger.activeContracts(alice)
      Seq(ce) = contracts
    } yield assert(
      ce.observers == Seq(bob),
      s"Expected observers to only contain $bob, but received ${ce.observers}",
    )
  })

  test(
    "ACSFilterCombinations",
    "Testing a few combinations of ACSs",
    allocate(Parties(3)),
  )(implicit ec => { case Participants(Participant(ledger, p1, p2, p3)) =>
    // Let we have 3 templates
    val templateIds: Vector[Identifier] =
      Vector(TriAgreement.id.unwrap, TriProposal.id.unwrap, WithObservers.id.unwrap)
    // Let we have 3 parties
    val parties: Vector[Party] = Vector(p1, p2, p3)
    // Let we have all combinations for the 3 parties
    val partyCombinations =
      Vector(Set(0), Set(1), Set(2), Set(0, 1), Set(1, 2), Set(0, 2), Set(0, 1, 2))
    case class FilterCoord(templateId: Int, stakeholders: Set[Int])
    // Let we populate 3 contracts for each template/partyCombination pair (see createContracts below)
    // Then we require the following Filter - Expectations to be upheld
    val * = Set.empty[Int]
    val allFilterCoords = Set(
      FilterCoord(0, Set(0)),
      FilterCoord(0, Set(1)),
      FilterCoord(0, Set(2)),
      FilterCoord(0, Set(0, 1)),
      FilterCoord(0, Set(0, 2)),
      FilterCoord(0, Set(1, 2)),
      FilterCoord(0, Set(0, 1, 2)),
      FilterCoord(1, Set(0)),
      FilterCoord(1, Set(1)),
      FilterCoord(1, Set(2)),
      FilterCoord(1, Set(0, 1)),
      FilterCoord(1, Set(0, 2)),
      FilterCoord(1, Set(1, 2)),
      FilterCoord(1, Set(0, 1, 2)),
      FilterCoord(2, Set(0)),
      FilterCoord(2, Set(1)),
      FilterCoord(2, Set(2)),
      FilterCoord(2, Set(0, 1)),
      FilterCoord(2, Set(0, 2)),
      FilterCoord(2, Set(1, 2)),
      FilterCoord(2, Set(0, 1, 2)),
    )
    val fixtures: Vector[(Map[Int, Set[Int]], Set[FilterCoord])] = Vector(
      // single filter
      Map(0 -> *) -> Set(
        FilterCoord(0, Set(0)),
        FilterCoord(0, Set(0, 1)),
        FilterCoord(0, Set(0, 2)),
        FilterCoord(0, Set(0, 1, 2)),
        FilterCoord(1, Set(0)),
        FilterCoord(1, Set(0, 1)),
        FilterCoord(1, Set(0, 2)),
        FilterCoord(1, Set(0, 1, 2)),
        FilterCoord(2, Set(0)),
        FilterCoord(2, Set(0, 1)),
        FilterCoord(2, Set(0, 2)),
        FilterCoord(2, Set(0, 1, 2)),
      ),
      Map(0 -> Set(0)) -> Set(
        FilterCoord(0, Set(0)),
        FilterCoord(0, Set(0, 1)),
        FilterCoord(0, Set(0, 2)),
        FilterCoord(0, Set(0, 1, 2)),
      ),
      Map(0 -> Set(1)) -> Set(
        FilterCoord(1, Set(0)),
        FilterCoord(1, Set(0, 1)),
        FilterCoord(1, Set(0, 2)),
        FilterCoord(1, Set(0, 1, 2)),
      ),
      Map(0 -> Set(2)) -> Set(
        FilterCoord(2, Set(0)),
        FilterCoord(2, Set(0, 1)),
        FilterCoord(2, Set(0, 2)),
        FilterCoord(2, Set(0, 1, 2)),
      ),
      Map(0 -> Set(0, 1)) -> Set(
        FilterCoord(0, Set(0)),
        FilterCoord(0, Set(0, 1)),
        FilterCoord(0, Set(0, 2)),
        FilterCoord(0, Set(0, 1, 2)),
        FilterCoord(1, Set(0)),
        FilterCoord(1, Set(0, 1)),
        FilterCoord(1, Set(0, 2)),
        FilterCoord(1, Set(0, 1, 2)),
      ),
      Map(0 -> Set(0, 2)) -> Set(
        FilterCoord(0, Set(0)),
        FilterCoord(0, Set(0, 1)),
        FilterCoord(0, Set(0, 2)),
        FilterCoord(0, Set(0, 1, 2)),
        FilterCoord(2, Set(0)),
        FilterCoord(2, Set(0, 1)),
        FilterCoord(2, Set(0, 2)),
        FilterCoord(2, Set(0, 1, 2)),
      ),
      Map(0 -> Set(1, 2)) -> Set(
        FilterCoord(1, Set(0)),
        FilterCoord(1, Set(0, 1)),
        FilterCoord(1, Set(0, 2)),
        FilterCoord(1, Set(0, 1, 2)),
        FilterCoord(2, Set(0)),
        FilterCoord(2, Set(0, 1)),
        FilterCoord(2, Set(0, 2)),
        FilterCoord(2, Set(0, 1, 2)),
      ),
      Map(0 -> Set(0, 1, 2)) -> Set(
        FilterCoord(0, Set(0)),
        FilterCoord(0, Set(0, 1)),
        FilterCoord(0, Set(0, 2)),
        FilterCoord(0, Set(0, 1, 2)),
        FilterCoord(1, Set(0)),
        FilterCoord(1, Set(0, 1)),
        FilterCoord(1, Set(0, 2)),
        FilterCoord(1, Set(0, 1, 2)),
        FilterCoord(2, Set(0)),
        FilterCoord(2, Set(0, 1)),
        FilterCoord(2, Set(0, 2)),
        FilterCoord(2, Set(0, 1, 2)),
      ),
      // multi filter
      Map(0 -> *, 1 -> *) -> (allFilterCoords -- Set(
        FilterCoord(0, Set(2)),
        FilterCoord(1, Set(2)),
        FilterCoord(2, Set(2)),
      )),
      Map(0 -> *, 2 -> *) -> (allFilterCoords -- Set(
        FilterCoord(0, Set(1)),
        FilterCoord(1, Set(1)),
        FilterCoord(2, Set(1)),
      )),
      Map(0 -> *, 1 -> *, 2 -> *) -> allFilterCoords,
      Map(0 -> Set(0), 1 -> Set(1)) -> Set(
        FilterCoord(0, Set(0)),
        FilterCoord(0, Set(0, 1)),
        FilterCoord(0, Set(0, 2)),
        FilterCoord(0, Set(0, 1, 2)),
        FilterCoord(1, Set(1)),
        FilterCoord(1, Set(0, 1)),
        FilterCoord(1, Set(1, 2)),
        FilterCoord(1, Set(0, 1, 2)),
      ),
      Map(0 -> Set(0), 1 -> Set(1), 2 -> Set(2)) -> Set(
        FilterCoord(0, Set(0)),
        FilterCoord(0, Set(0, 1)),
        FilterCoord(0, Set(0, 2)),
        FilterCoord(0, Set(0, 1, 2)),
        FilterCoord(1, Set(1)),
        FilterCoord(1, Set(0, 1)),
        FilterCoord(1, Set(1, 2)),
        FilterCoord(1, Set(0, 1, 2)),
        FilterCoord(2, Set(2)),
        FilterCoord(2, Set(0, 2)),
        FilterCoord(2, Set(1, 2)),
        FilterCoord(2, Set(0, 1, 2)),
      ),
      Map(0 -> Set(0, 1), 1 -> Set(0, 2)) -> Set(
        FilterCoord(0, Set(0)),
        FilterCoord(0, Set(1)),
        FilterCoord(0, Set(0, 1)),
        FilterCoord(0, Set(0, 2)),
        FilterCoord(0, Set(1, 2)),
        FilterCoord(0, Set(0, 1, 2)),
        FilterCoord(1, Set(0)),
        FilterCoord(1, Set(0, 1)),
        FilterCoord(1, Set(0, 2)),
        FilterCoord(1, Set(0, 1, 2)),
        FilterCoord(2, Set(1)),
        FilterCoord(2, Set(0, 1)),
        FilterCoord(2, Set(1, 2)),
        FilterCoord(2, Set(0, 1, 2)),
      ),
      Map(0 -> Set(0, 1), 1 -> Set(0, 2), 2 -> Set(1, 2)) -> Set(
        FilterCoord(0, Set(0)),
        FilterCoord(0, Set(1)),
        FilterCoord(0, Set(0, 1)),
        FilterCoord(0, Set(0, 2)),
        FilterCoord(0, Set(1, 2)),
        FilterCoord(0, Set(0, 1, 2)),
        FilterCoord(1, Set(0)),
        FilterCoord(1, Set(2)),
        FilterCoord(1, Set(0, 1)),
        FilterCoord(1, Set(0, 2)),
        FilterCoord(1, Set(1, 2)),
        FilterCoord(1, Set(0, 1, 2)),
        FilterCoord(2, Set(1)),
        FilterCoord(2, Set(2)),
        FilterCoord(2, Set(0, 1)),
        FilterCoord(2, Set(0, 2)),
        FilterCoord(2, Set(1, 2)),
        FilterCoord(2, Set(0, 1, 2)),
      ),
      Map(0 -> *, 1 -> Set(0)) -> Set(
        FilterCoord(0, Set(0)),
        FilterCoord(0, Set(1)),
        FilterCoord(0, Set(0, 1)),
        FilterCoord(0, Set(0, 2)),
        FilterCoord(0, Set(1, 2)),
        FilterCoord(0, Set(0, 1, 2)),
        FilterCoord(1, Set(0)),
        FilterCoord(1, Set(0, 1)),
        FilterCoord(1, Set(0, 2)),
        FilterCoord(1, Set(0, 1, 2)),
        FilterCoord(2, Set(0)),
        FilterCoord(2, Set(0, 1)),
        FilterCoord(2, Set(0, 2)),
        FilterCoord(2, Set(0, 1, 2)),
      ),
      Map(0 -> *, 1 -> Set(0), 2 -> Set(1, 2)) -> Set(
        FilterCoord(0, Set(0)),
        FilterCoord(0, Set(1)),
        FilterCoord(0, Set(0, 1)),
        FilterCoord(0, Set(0, 2)),
        FilterCoord(0, Set(1, 2)),
        FilterCoord(0, Set(0, 1, 2)),
        FilterCoord(1, Set(0)),
        FilterCoord(1, Set(2)),
        FilterCoord(1, Set(0, 1)),
        FilterCoord(1, Set(0, 2)),
        FilterCoord(1, Set(1, 2)),
        FilterCoord(1, Set(0, 1, 2)),
        FilterCoord(2, Set(0)),
        FilterCoord(2, Set(2)),
        FilterCoord(2, Set(0, 1)),
        FilterCoord(2, Set(0, 2)),
        FilterCoord(2, Set(1, 2)),
        FilterCoord(2, Set(0, 1, 2)),
      ),
    )

    def createContracts: Future[Map[FilterCoord, Set[String]]] = {
      def withThreeParties[T](f: (Party, Party, Party) => T)(partySet: Set[Party]): T =
        partySet.toList match {
          case a :: b :: c :: Nil => f(a, b, c)
          case a :: b :: Nil => f(a, b, b)
          case a :: Nil => f(a, a, a)
          case invalid =>
            throw new Exception(s"Invalid partySet, length must be 1 or 2 or 3 but it was $invalid")
        }
      val templateFactories: Vector[Set[Party] => Template[Template[Any]]] = Vector(
        withThreeParties(TriAgreement(_, _, _)),
        withThreeParties(TriProposal(_, _, _)),
        parties => WithObservers(parties.head, parties.toList),
      )
      def createContractFor(template: Int, partyCombination: Int) =
        ledger.create(
          partyCombinations(partyCombination).map(parties).toList,
          partyCombinations(partyCombination).map(parties).toList,
          templateFactories(template)(partyCombinations(partyCombination).map(parties)),
        )

      val createFs = for {
        partyCombinationIndex <- partyCombinations.indices
        templateIndex <- templateFactories.indices
        _ <- 1 to 3
      } yield createContractFor(templateIndex, partyCombinationIndex)
        .map((partyCombinationIndex, templateIndex) -> _)

      Future
        .sequence(createFs)
        .map(_.groupBy(_._1).map { case ((partyCombinationIndex, templateIndex), contractId) =>
          (
            FilterCoord(templateIndex, partyCombinations(partyCombinationIndex)),
            contractId.view.map(_._2.toString).toSet,
          )
        })
    }

    def testForFixtures(
        fixtures: Vector[(Map[Int, Set[Int]], Set[FilterCoord])],
        allContracts: Map[FilterCoord, Set[String]],
    ) = {
      def activeContractIdsFor(filters: Map[Int, Set[Int]]): Future[Vector[String]] =
        ledger
          .activeContracts(
            new GetActiveContractsRequest(
              ledgerId = ledger.ledgerId,
              filter = Some(
                new TransactionFilter(
                  filters.map {
                    case (party, templates) if templates.isEmpty =>
                      (parties(party).toString, new Filters(None))
                    case (party, templates) =>
                      (
                        parties(party).toString,
                        new Filters(Some(new InclusiveFilters(templates.toSeq.map(templateIds)))),
                      )
                  }
                )
              ),
              verbose = true,
            )
          )
          .map(_._2.map(_.contractId))

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
            List(s"$filterCoord is present (to many contracts in result)")
          case (filterCoord, contracts) if contracts.exists(actualSet) =>
            List(s"$filterCoord is partially present (to many contracts in result)")
          case (_, _) => Nil
        }
        assert(errors == Nil, s"$hint ACS mismatch: ${errors.mkString(", ")}")
      }

      val testFs = fixtures.map { case (filter, expectedResultCoords) =>
        activeContractIdsFor(filter).map(
          testForFixture(_, expectedResultCoords, s"Filter: $filter")
        )
      }
      Future.sequence(testFs)
    }

    for {
      allContracts <- createContracts
      _ <- testForFixtures(fixtures, allContracts)
    } yield ()
  })

  private def createDummyContracts(party: Party, ledger: ParticipantTestContext)(implicit
      ec: ExecutionContext
  ) = {
    for {
      dummy <- ledger.create(party, Dummy(party))
      dummyWithParam <- ledger.create(party, DummyWithParam(party))
      dummyFactory <- ledger.create(party, DummyFactory(party))
    } yield (dummy, dummyWithParam, dummyFactory)
  }

  private def assertTemplates[A](
      party: Seq[Party],
      events: Vector[CreatedEvent],
      templateId: TemplateId[A],
      count: Int,
  ): Unit = {
    val templateEvents = events.count(_.getTemplateId == templateId.unwrap)
    assert(
      templateEvents == count,
      s"${party.mkString(" and ")} expected $count $templateId events, but received $templateEvents.",
    )
  }
}
