// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
import com.digitalasset.ledger.client.binding.Primitive.{Party, TemplateId}
import com.digitalasset.ledger.test_stable.Test.Dummy._
import com.digitalasset.ledger.test_stable.Test.{Dummy, DummyFactory, DummyWithParam}
import io.grpc.Status
import scalaz.syntax.tag._

class ActiveContractsService(session: LedgerSession) extends LedgerTestSuite(session) {
  private val invalidLedgerId =
    LedgerTest(
      "ACSinvalidLedgerId",
      "The ActiveContractService should fail for requests with an invalid ledger identifier") {
      context =>
        val invalidLedgerId = "ACSinvalidLedgerId"
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          invalidRequest = ledger
            .activeContractsRequest(Seq(party))
            .update(_.ledgerId := invalidLedgerId)
          failure <- ledger.activeContracts(invalidRequest).failed
        } yield {
          assertGrpcError(failure, Status.Code.NOT_FOUND, "not found. Actual Ledger ID")
        }
    }

  private val emptyResponse = LedgerTest(
    "ACSemptyResponse",
    "The ActiveContractService should succeed with an empty response if no contracts have been created for a party") {
    context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        activeContracts <- ledger.activeContracts(party)
      } yield {
        assert(
          activeContracts.isEmpty,
          s"There should be no active contracts, but received ${activeContracts}")
      }
  }

  private def createDummyContracts(party: Party, ledger: ParticipantTestContext) = {
    for {
      dummy <- ledger.create(party, Dummy(party))
      dummyWithParam <- ledger.create(party, DummyWithParam(party))
      dummyFactory <- ledger.create(party, DummyFactory(party))
    } yield (dummy, dummyWithParam, dummyFactory)
  }

  private val returnAllContracts =
    LedgerTest("ACSallContracts", "The ActiveContractService should return all active contracts") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          (dummy, dummyWithParam, dummyFactory) <- createDummyContracts(party, ledger)
          activeContracts <- ledger.activeContracts(party)
        } yield {
          assert(
            activeContracts.size == 3,
            s"Expected 3 contracts, but received ${activeContracts.size}.")

          assert(
            activeContracts.exists(_.contractId == dummy),
            s"Didn't find Dummy contract with contractId ${dummy}.")
          assert(
            activeContracts.exists(_.contractId == dummyWithParam),
            s"Didn't find DummyWithParam contract with contractId ${dummy}.")
          assert(
            activeContracts.exists(_.contractId == dummyFactory),
            s"Didn't find DummyFactory contract with contractId ${dummy}.")

          assert(
            activeContracts.forall(_.signatories == Seq(party.unwrap)),
            s"Found contracts with signatories other than ${party}.")
          assert(
            activeContracts.forall(_.observers.isEmpty),
            s"Found contracts with signatories other than ${party}.")
        }
    }

  private val filterContracts =
    LedgerTest(
      "ACSfilterContracts",
      "The ActiveContractService should return contracts filtered by templateId") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        (dummy, _, _) <- createDummyContracts(party, ledger)
        activeContracts <- ledger.activeContractsByTemplateId(Seq(Dummy.id.unwrap), party)
      } yield {
        assert(
          activeContracts.size == 1,
          s"Expected 1 contract, but received ${activeContracts.size}.")

        assert(
          activeContracts.head.getTemplateId == Dummy.id.unwrap,
          s"Received contract is not of type Dummy, but ${activeContracts.head.templateId}.")
        assert(
          activeContracts.head.contractId == dummy,
          s"Expected contract with contractId ${dummy}, but received ${activeContracts.head.contractId}."
        )
      }
    }

  private val excludeArchivedContracts =
    LedgerTest(
      "ACSarchivedContracts",
      "The ActiveContractService does not return archived contracts") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        (dummy, _, _) <- createDummyContracts(party, ledger)
        contractsBeforeExercise <- ledger.activeContracts(party)
        _ <- ledger.exercise(party, dummy.exerciseDummyChoice1)
        contractsAfterExercise <- ledger.activeContracts(party)
      } yield {
        // check the contracts BEFORE the exercise
        assert(
          contractsBeforeExercise.size == 3,
          s"Expected 3 contracts, but received ${contractsBeforeExercise.size}.")

        assert(
          contractsBeforeExercise.exists(_.contractId == dummy),
          s"Expected to receive contract with contractId ${dummy}, but received ${contractsBeforeExercise
            .map(_.contractId)
            .mkString(", ")} instead."
        )

        // check the contracts AFTER the exercise
        assert(
          contractsAfterExercise.size == 2,
          s"Expected 2 contracts, but received ${contractsAfterExercise.size}")

        assert(
          !contractsAfterExercise.exists(_.contractId == dummy),
          s"Expected to not receive contract with contractId ${dummy}."
        )
      }
    }

  private val usableOffset = LedgerTest(
    "ACSusableOffset",
    "The ActiveContractService should return a usable offset to resume streaming transactions") {
    context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        dummy <- ledger.create(party, Dummy(party))
        (Some(offset), onlyDummy) <- ledger.activeContracts(
          ledger.activeContractsRequest(Seq(party)))
        dummyWithParam <- ledger.create(party, DummyWithParam(party))
        request = ledger.getTransactionsRequest(Seq(party))
        fromOffset = request.update(_.begin := offset)
        transactions <- ledger.flatTransactions(fromOffset)
      } yield {
        assert(onlyDummy.size == 1)
        assert(
          onlyDummy.exists(_.contractId == dummy),
          s"Expected to receive ${dummy} in active contracts, but didn't receive it.")

        assert(
          transactions.size == 1,
          s"Expected to receive only 1 transaction from offset $offset, but received ${transactions.size}.")

        val transaction = transactions.head
        assert(
          transaction.events.size == 1,
          s"Expected only 1 event in the transaction, but received ${transaction.events.size}.")

        val createdEvent = transaction.events.collect {
          case Event(Created(createdEvent)) => createdEvent
        }
        assert(
          createdEvent.exists(_.contractId == dummyWithParam),
          s"Expected a CreateEvent for ${dummyWithParam}, but received ${createdEvent}."
        )
      }
  }

  private val verbosityFlag = LedgerTest(
    "ACSverbosity",
    "The ActiveContractService should emit field names only if the verbose flag is set to true") {
    context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        _ <- ledger.create(party, Dummy(party))
        verboseRequest = ledger.activeContractsRequest(Seq(party)).update(_.verbose := true)
        nonVerboseRequest = verboseRequest.update(_.verbose := false)
        (_, verboseEvents) <- ledger.activeContracts(verboseRequest)
        (_, nonVerboseEvents) <- ledger.activeContracts(nonVerboseRequest)
      } yield {
        val verboseCreateArgs = verboseEvents.map(_.getCreateArguments).flatMap(_.fields)
        assert(
          verboseEvents.nonEmpty && verboseCreateArgs.forall(_.label.nonEmpty),
          s"$party expected a contract with labels, but received $verboseEvents.")

        val nonVerboseCreateArgs = nonVerboseEvents.map(_.getCreateArguments).flatMap(_.fields)
        assert(
          nonVerboseEvents.nonEmpty && nonVerboseCreateArgs.forall(_.label.isEmpty),
          s"$party expected a contract without labels, but received $nonVerboseEvents.")
      }
  }

  private def assertTemplates[A](
      party: Seq[Party],
      events: Vector[CreatedEvent],
      templateId: TemplateId[A],
      count: Int): Unit = {
    val templateEvents = events.count(_.getTemplateId == templateId.unwrap)
    assert(
      templateEvents == count,
      s"${party.mkString(" and ")} expected $count $templateId events, but received $templateEvents.")
  }

  private val multiPartyRequests = LedgerTest(
    "ACSmultiParty",
    "The ActiveContractsService should return contracts for the requesting parties") { context =>
    for {
      ledger <- context.participant()
      Vector(alice, bob) <- ledger.allocateParties(2)
      _ <- createDummyContracts(alice, ledger)
      _ <- createDummyContracts(bob, ledger)
      allContractsForAlice <- ledger.activeContracts(alice)
      allContractsForBob <- ledger.activeContracts(bob)
      allContractsForAliceAndBob <- ledger.activeContracts(alice, bob)
      dummyContractsForAlice <- ledger.activeContractsByTemplateId(Seq(Dummy.id.unwrap), alice)
      dummyContractsForAliceAndBob <- ledger.activeContractsByTemplateId(
        Seq(Dummy.id.unwrap),
        alice,
        bob)
    } yield {
      assert(
        allContractsForAlice.size == 3,
        s"$alice expected 3 events, but received ${allContractsForAlice.size}.")
      assertTemplates(Seq(alice), allContractsForAlice, Dummy.id, 1)
      assertTemplates(Seq(alice), allContractsForAlice, DummyWithParam.id, 1)
      assertTemplates(Seq(alice), allContractsForAlice, DummyFactory.id, 1)

      assert(
        allContractsForBob.size == 3,
        s"$bob expected 3 events, but received ${allContractsForBob.size}.")
      assertTemplates(Seq(bob), allContractsForBob, Dummy.id, 1)
      assertTemplates(Seq(bob), allContractsForBob, DummyWithParam.id, 1)
      assertTemplates(Seq(bob), allContractsForBob, DummyFactory.id, 1)

      assert(
        allContractsForAliceAndBob.size == 6,
        s"$alice and $bob expected 6 events, but received ${allContractsForAliceAndBob.size}.")
      assertTemplates(Seq(alice, bob), allContractsForAliceAndBob, Dummy.id, 2)
      assertTemplates(Seq(alice, bob), allContractsForAliceAndBob, DummyWithParam.id, 2)
      assertTemplates(Seq(alice, bob), allContractsForAliceAndBob, DummyFactory.id, 2)

      assert(
        dummyContractsForAlice.size == 1,
        s"$alice expected 1 event, but received ${dummyContractsForAlice.size}.")
      assertTemplates((Seq(alice)), dummyContractsForAlice, Dummy.id, 1)

      assert(
        dummyContractsForAliceAndBob.size == 2,
        s"$alice and $bob expected 2 events, but received ${dummyContractsForAliceAndBob.size}.")
      assertTemplates((Seq(alice, bob)), dummyContractsForAliceAndBob, Dummy.id, 2)
    }
  }

  private val agreementText =
    LedgerTest(
      "ACSagreementText",
      "The ActiveContractService should properly fill the agreementText field") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
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
          s"$party expected a non-empty agreement text, but received $dummyAgreementText.")
        assert(
          dummyWithParamAgreementText.exists(_.nonEmpty),
          s"$party expected an empty agreement text, but received $dummyWithParamAgreementText.")
      }
    }

  override val tests: Vector[LedgerTest] = Vector(
    invalidLedgerId,
    emptyResponse,
    returnAllContracts,
    filterContracts,
    excludeArchivedContracts,
    usableOffset,
    verbosityFlag,
    multiPartyRequests,
    agreementText
  )
}
