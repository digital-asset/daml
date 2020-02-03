// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
import com.digitalasset.ledger.client.binding.Primitive.{Party, TemplateId}
import com.digitalasset.ledger.test_stable.Test.Dummy._
import com.digitalasset.ledger.test_stable.Test.{Dummy, DummyFactory, DummyWithParam}
import io.grpc.Status
import scalaz.syntax.tag._

class ActiveContractsService(session: LedgerSession) extends LedgerTestSuite(session) {
  test(
    "ACSinvalidLedgerId",
    "The ActiveContractService should fail for requests with an invalid ledger identifier",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, parties @ _*)) =>
      val invalidLedgerId = "ACSinvalidLedgerId"
      val invalidRequest = ledger
        .activeContractsRequest(parties)
        .update(_.ledgerId := invalidLedgerId)
      for {
        failure <- ledger.activeContracts(invalidRequest).failed
      } yield {
        assertGrpcError(failure, Status.Code.NOT_FOUND, "not found. Actual Ledger ID")
      }
  }

  test(
    "ACSemptyResponse",
    "The ActiveContractService should succeed with an empty response if no contracts have been created for a party",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        activeContracts <- ledger.activeContracts(party)
      } yield {
        assert(
          activeContracts.isEmpty,
          s"There should be no active contracts, but received ${activeContracts}",
        )
      }
  }

  test(
    "ACSallContracts",
    "The ActiveContractService should return all active contracts",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
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
          s"Didn't find Dummy contract with contractId ${dummy}.",
        )
        assert(
          activeContracts.exists(_.contractId == dummyWithParam),
          s"Didn't find DummyWithParam contract with contractId ${dummy}.",
        )
        assert(
          activeContracts.exists(_.contractId == dummyFactory),
          s"Didn't find DummyFactory contract with contractId ${dummy}.",
        )

        val invalidSignatories = activeContracts.filterNot(_.signatories == Seq(party.unwrap))
        assert(
          invalidSignatories.isEmpty,
          s"Found contracts with signatories other than ${party}: $invalidSignatories",
        )

        val invalidObservers = activeContracts.filterNot(_.observers.isEmpty)
        assert(
          invalidObservers.isEmpty,
          s"Found contracts with non-empty observers: $invalidObservers",
        )
      }
  }

  test(
    "ACSfilterContracts",
    "The ActiveContractService should return contracts filtered by templateId",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
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
          s"Expected contract with contractId ${dummy}, but received ${activeContracts.head.contractId}.",
        )
      }
  }

  test(
    "ACSarchivedContracts",
    "The ActiveContractService does not return archived contracts",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
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
          s"Expected to receive contract with contractId ${dummy}, but received ${contractsBeforeExercise
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
          s"Expected to not receive contract with contractId ${dummy}.",
        )
      }
  }

  test(
    "ACSusableOffset",
    "The ActiveContractService should return a usable offset to resume streaming transactions",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        dummy <- ledger.create(party, Dummy(party))
        (Some(offset), onlyDummy) <- ledger.activeContracts(
          ledger.activeContractsRequest(Seq(party)),
        )
        dummyWithParam <- ledger.create(party, DummyWithParam(party))
        request = ledger.getTransactionsRequest(Seq(party))
        fromOffset = request.update(_.begin := offset)
        transactions <- ledger.flatTransactions(fromOffset)
      } yield {
        assert(onlyDummy.size == 1)
        assert(
          onlyDummy.exists(_.contractId == dummy),
          s"Expected to receive ${dummy} in active contracts, but didn't receive it.",
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

        val createdEvent = transaction.events.collect {
          case Event(Created(createdEvent)) => createdEvent
        }
        assert(
          createdEvent.exists(_.contractId == dummyWithParam),
          s"Expected a CreateEvent for ${dummyWithParam}, but received ${createdEvent}.",
        )
      }
  }

  test(
    "ACSverbosity",
    "The ActiveContractService should emit field names only if the verbose flag is set to true",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
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
  }

  test(
    "ACSmultiParty",
    "The ActiveContractsService should return contracts for the requesting parties",
    allocate(TwoParties),
  ) {
    case Participants(Participant(ledger, alice, bob)) =>
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
        assertTemplates((Seq(alice)), dummyContractsForAlice, Dummy.id, 1)

        assert(
          dummyContractsForAliceAndBob.size == 2,
          s"$alice and $bob expected 2 events, but received ${dummyContractsForAliceAndBob.size}.",
        )
        assertTemplates((Seq(alice, bob)), dummyContractsForAliceAndBob, Dummy.id, 2)
      }
  }

  test(
    "ACSagreementText",
    "The ActiveContractService should properly fill the agreementText field",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        dummyCid <- ledger.create(party, Dummy(party))
        dummyWithParamCid <- ledger.create(party, DummyWithParam(party))
        contracts <- ledger.activeContracts(party)
      } yield {
        assert(contracts.size == 2, s"$party expected 1 event, but received ${contracts.size}.")
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
  }

  test(
    "ACSeventId",
    "The ActiveContractService should properly fill the eventId field",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
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
  }

  private def createDummyContracts(party: Party, ledger: ParticipantTestContext) = {
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
