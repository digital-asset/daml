// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
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
import scalaz.syntax.tag._

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

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
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.LedgerIdMismatch,
        Some("not found. Actual Ledger ID"),
      )
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
      activeContracts <- ledger.activeContractsByTemplateId(Seq(Dummy.id), party)
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
      _ <- ledger.exercise(party, dummy.exerciseDummyChoice1())
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
      request = ledger.getTransactionsRequest(ledger.transactionFilter(Seq(party)))
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
      dummyContractsForAlice <- ledger.activeContractsByTemplateId(Seq(Dummy.id), alice)
      dummyContractsForAliceAndBob <- ledger.activeContractsByTemplateId(
        Seq(Dummy.id),
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
      _ <- ledger.exercise(bob, witnesses.exerciseWitnessesCreateNewWitnesses())
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
      _ <- ledger.exercise(alice, divulgence2.exerciseDivulgence2Fetch(divulgence1))
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
    } yield {
      contracts.foreach(ce =>
        assert(
          ce.observers == Seq(bob),
          s"Expected observers to only contain $bob, but received ${ce.observers}",
        )
      )
    }
  })

  test(
    "ACFilterWitnesses",
    "The ActiveContractService should filter witnesses by the transaction filter",
    allocate(Parties(3)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie)) =>
    for {
      _ <- ledger.create(alice, WithObservers(alice, List(bob, charlie)))
      bobContracts <- ledger.activeContracts(bob)
      aliceBobContracts <- ledger.activeContracts(alice, bob)
      bobCharlieContracts <- ledger.activeContracts(bob, charlie)
    } yield {
      def assertWitnesses(contracts: Vector[CreatedEvent], requesters: Set[Party]): Unit = {
        assert(
          contracts.size == 1,
          s"Expected to receive 1 active contracts for $requesters, but received ${contracts.size}.",
        )
        assert(
          contracts.head.witnessParties.toSet == requesters.map(_.toString),
          s"Expected witness parties to equal to $requesters, but received ${contracts.head.witnessParties}",
        )
      }

      assertWitnesses(bobContracts, Set(bob))
      assertWitnesses(aliceBobContracts, Set(alice, bob))
      assertWitnesses(bobCharlieContracts, Set(bob, charlie))
    }
  })

  test(
    "ACSFilterCombinations",
    "Testing ACS filter combinations",
    allocate(Parties(3)),
  )(implicit ec => { case Participants(Participant(ledger, p1, p2, p3)) =>
    // Let us have 3 templates
    val templateIds: Vector[Identifier] =
      Vector(TriAgreement.id.unwrap, TriProposal.id.unwrap, WithObservers.id.unwrap)
    // Let us have 3 parties
    val parties: Vector[Party] = Vector(p1, p2, p3)
    // Let us have all combinations for the 3 parties
    val partyCombinations =
      Vector(Set(0), Set(1), Set(2), Set(0, 1), Set(1, 2), Set(0, 2), Set(0, 1, 2))
    // Let us populate 3 contracts for each template/partyCombination pair (see createContracts below)
    // Then we require the following Filter - Expectations to be upheld

    // Key is the index of a test party (see parties)
    // Value is
    //   either empty, meaning a wildcard party filter
    //   or the Set of indices of a test template (see templateIds)
    val * = Set.empty[Int]
    type ACSFilter = Map[Int, Set[Int]]

    case class FilterCoord(templateId: Int, stakeholders: Set[Int])

    def filterCoordsForFilter(filter: ACSFilter): Set[FilterCoord] = {
      (for {
        (party, templates) <- filter
        templateId <- if (templates.isEmpty) templateIds.indices.toSet else templates
        allowedPartyCombination <- partyCombinations.filter(_(party))
      } yield FilterCoord(templateId, allowedPartyCombination)).toSet
    }

    val fixtures: Vector[(ACSFilter, Set[FilterCoord])] = Vector(
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
    ).map(filter => filter -> filterCoordsForFilter(filter))

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

    val random = new Random(System.nanoTime())

    def testForFixtures(
        fixtures: Vector[(ACSFilter, Set[FilterCoord])],
        allContracts: Map[FilterCoord, Set[String]],
    ) = {
      def activeContractIdsFor(filter: ACSFilter): Future[Vector[String]] =
        ledger
          .activeContracts(
            new GetActiveContractsRequest(
              ledgerId = ledger.ledgerId,
              filter = Some(
                new TransactionFilter(
                  filter.map {
                    case (party, templates) if templates.isEmpty =>
                      (parties(party).toString, new Filters(None))
                    case (party, templates) =>
                      (
                        parties(party).toString,
                        new Filters(
                          Some(
                            new InclusiveFilters(random.shuffle(templates.toSeq.map(templateIds)))
                          )
                        ),
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
