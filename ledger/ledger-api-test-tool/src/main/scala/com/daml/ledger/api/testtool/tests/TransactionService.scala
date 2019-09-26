// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import ai.x.diff.conversions._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.value.{RecordField, Value}
import com.digitalasset.ledger.client.binding.Primitive
import com.digitalasset.ledger.client.binding.Value.encode
import com.digitalasset.ledger.test_stable.Test.Agreement._
import com.digitalasset.ledger.test_stable.Test.AgreementFactory._
import com.digitalasset.ledger.test_stable.Test.Choice1._
import com.digitalasset.ledger.test_stable.Test.CreateAndFetch._
import com.digitalasset.ledger.test_stable.Test.Dummy._
import com.digitalasset.ledger.test_stable.Test.DummyFactory._
import com.digitalasset.ledger.test_stable.Test.ParameterShowcase._
import com.digitalasset.ledger.test_stable.Test.TriProposal._
import com.digitalasset.ledger.test_stable.Test._
import io.grpc.Status
import scalaz.Tag

import scala.concurrent.Future

class TransactionService(session: LedgerSession) extends LedgerTestSuite(session) {

  private[this] val beginToBeginShouldBeEmpty =
    LedgerTest(
      "TXBeginToBegin",
      "An empty stream should be served when getting transactions from and to the beginning of the ledger") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          request = ledger.getTransactionsRequest(Seq(party))
          fromAndToBegin = request.update(_.begin := ledger.begin, _.end := ledger.begin)
          transactions <- ledger.flatTransactions(fromAndToBegin)
        } yield {
          assert(
            transactions.isEmpty,
            s"Received a non-empty stream with ${transactions.size} transactions in it.")
        }
    }

  private[this] val treesBeginToBeginShouldBeEmpty =
    LedgerTest(
      "TXTreesBeginToBegin",
      "An empty stream of trees should be served when getting transactions from and to the beginning of the ledger") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          request = ledger.getTransactionsRequest(Seq(party))
          fromAndToBegin = request.update(_.begin := ledger.begin, _.end := ledger.begin)
          transactions <- ledger.transactionTrees(fromAndToBegin)
        } yield {
          assert(
            transactions.isEmpty,
            s"Received a non-empty stream with ${transactions.size} transactions in it.")
        }
    }

  private[this] val endToEndShouldBeEmpty =
    LedgerTest(
      "TXEndToEnd",
      "An empty stream should be served when getting transactions from and to the end of the ledger") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          _ <- ledger.create(party, Dummy(party))
          request = ledger.getTransactionsRequest(Seq(party))
          endToEnd = request.update(_.begin := ledger.end, _.end := ledger.end)
          transactions <- ledger.flatTransactions(endToEnd)
        } yield {
          assert(
            transactions.isEmpty,
            s"No transactions were expected but ${transactions.size} were read")
        }
    }

  private[this] val serveElementsUntilCancellation =
    LedgerTest("TXServeUntilCancellation", "Items should be served until the client cancels") {
      context =>
        val transactionsToSubmit = 14
        val transactionsToRead = 10
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          dummies <- Future.sequence(
            Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
          transactions <- ledger.flatTransactions(transactionsToRead, party)
        } yield {
          assert(
            dummies.size == transactionsToSubmit,
            s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead")
          assert(
            transactions.size == transactionsToRead,
            s"$transactionsToRead should have been received but ${transactions.size} were instead")
        }
    }

  private[this] val serveTreesUntilCancellation =
    LedgerTest("TXServeTreesUntilCancellation", "Trees should be served until the client cancels") {
      context =>
        val transactionsToSubmit = 14
        val treesToRead = 10
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          dummies <- Future.sequence(
            Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
          trees <- ledger.transactionTrees(treesToRead, party)
        } yield {
          assert(
            dummies.size == transactionsToSubmit,
            s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead")
          assert(
            trees.size == treesToRead,
            s"$treesToRead should have been received but ${trees.size} were instead")
        }
    }

  private[this] val deduplicateCommands =
    LedgerTest(
      "TXDeduplicateCommands",
      "Commands with identical submitter, command identifier, and application identifier should be accepted and deduplicated") {
      context =>
        for {
          ledger <- context.participant()
          alice <- ledger.allocateParty()
          bob <- ledger.allocateParty()
          aliceRequest <- ledger.submitAndWaitRequest(alice, Dummy(alice).create.command)
          _ <- ledger.submitAndWait(aliceRequest)
          _ <- ledger.submitAndWait(aliceRequest)
          aliceTransactions <- ledger.flatTransactions(alice)

          // now let's create another command that uses same applicationId and commandId, but submitted by Bob
          bobRequestTemplate <- ledger.submitAndWaitRequest(bob, Dummy(bob).create.command)
          bobRequest = bobRequestTemplate
            .update(_.commands.commandId := aliceRequest.getCommands.commandId)
            .update(_.commands.applicationId := aliceRequest.getCommands.applicationId)
          _ <- ledger.submitAndWait(bobRequest)
          bobTransactions <- ledger.flatTransactions(bob)
        } yield {
          assert(
            aliceTransactions.length == 1,
            s"Only one transaction was expected to be seen by $alice but ${aliceTransactions.length} appeared")

          assert(
            bobTransactions.length == 1,
            s"Expected a transaction to be seen by $bob but ${bobTransactions.length} appeared")
        }
    }

  private[this] val rejectEmptyFilter =
    LedgerTest(
      "TXRejectEmptyFilter",
      "A query with an empty transaction filter should be rejected with an INVALID_ARGUMENT status") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          request = ledger.getTransactionsRequest(Seq(party))
          requestWithEmptyFilter = request.update(_.filter.filtersByParty := Map.empty)
          failure <- ledger.flatTransactions(requestWithEmptyFilter).failed
        } yield {
          assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "filtersByParty cannot be empty")
        }
    }

  private[this] val completeOnLedgerEnd = LedgerTest(
    "TXCompleteOnLedgerEnd",
    "A stream should complete as soon as the ledger end is hit") { context =>
    val transactionsToSubmit = 14
    for {
      ledger <- context.participant()
      party <- ledger.allocateParty()
      transactionsFuture = ledger.flatTransactions(party)
      _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
      _ <- transactionsFuture
    } yield {
      // doing nothing: we are just checking that `transactionsFuture` completes successfully
    }
  }

  private[this] val completeTreesOnLedgerEnd = LedgerTest(
    "TXCompleteTreesOnLedgerEnd",
    "A stream of trees should complete as soon as the ledger end is hit") { context =>
    val transactionsToSubmit = 14
    for {
      ledger <- context.participant()
      party <- ledger.allocateParty()
      transactionsFuture = ledger.transactionTrees(party)
      _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
      _ <- transactionsFuture
    } yield {
      // doing nothing: we are just checking that `transactionsFuture` completes successfully
    }
  }

  private[this] val processInTwoChunks = LedgerTest(
    "TXProcessInTwoChunks",
    "Serve the complete sequence of transactions even if processing is stopped and resumed") {
    context =>
      val transactionsToSubmit = 5
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
        endAfterFirstSection <- ledger.currentEnd()
        firstSectionRequest = ledger
          .getTransactionsRequest(Seq(party))
          .update(_.end := endAfterFirstSection)
        firstSection <- ledger.flatTransactions(firstSectionRequest)
        _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
        endAfterSecondSection <- ledger.currentEnd()
        secondSectionRequest = ledger
          .getTransactionsRequest(Seq(party))
          .update(_.begin := endAfterFirstSection, _.end := endAfterSecondSection)
        secondSection <- ledger.flatTransactions(secondSectionRequest)
        fullSequence <- ledger.flatTransactions(party)
      } yield {
        val concatenation = Vector.concat(firstSection, secondSection)
        assert(
          fullSequence == concatenation,
          s"The result of processing items in two chunk should yield the same result as getting the overall stream of transactions in the end but there are differences. " +
            s"Full sequence: ${fullSequence.map(_.commandId).mkString(", ")}, " +
            s"first section: ${firstSection.map(_.commandId).mkString(", ")}, " +
            s"second section: ${secondSection.map(_.commandId).mkString(", ")}"
        )
      }
  }

  private[this] val identicalAndParallel = LedgerTest(
    "TXParallel",
    "The same data should be served for more than 1 identical, parallel requests") { context =>
    val transactionsToSubmit = 5
    val parallelRequests = 10
    for {
      ledger <- context.participant()
      party <- ledger.allocateParty()
      _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
      results <- Future.sequence(Vector.fill(parallelRequests)(ledger.flatTransactions(party)))
    } yield {
      assert(
        results.toSet.size == 1,
        s"All requests are supposed to return the same results but there " +
          s"where differences: ${results.map(_.map(_.commandId)).mkString(", ")}"
      )
    }
  }

  private[this] val notDivulgeToUnrelatedParties =
    LedgerTest("TXNotDivulge", "Data should not be exposed to parties unrelated to a transaction") {
      context =>
        for {
          Vector(alpha, beta) <- context.participants(2)
          alice <- alpha.allocateParty()
          bob <- beta.allocateParty()
          _ <- alpha.create(alice, Dummy(alice))
          bobsView <- alpha.flatTransactions(bob)
        } yield {
          assert(
            bobsView.isEmpty,
            s"After Alice create a contract, Bob sees one or more transaction he shouldn't, namely those created by commands ${bobsView.map(_.commandId).mkString(", ")}"
          )
        }
    }

  private[this] val rejectBeginAfterEnd =
    LedgerTest(
      "TXRejectBeginAfterEnd",
      "A request with the end before the begin should be rejected with INVALID_ARGUMENT") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          earlier <- ledger.currentEnd()
          _ <- ledger.create(party, Dummy(party))
          later <- ledger.currentEnd()
          request = ledger.getTransactionsRequest(Seq(party))
          invalidRequest = request.update(_.begin := later, _.end := earlier)
          failure <- ledger.flatTransactions(invalidRequest).failed
        } yield {
          assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "is before Begin offset")
        }
    }

  private[this] val hideCommandIdToNonSubmittingStakeholders =
    LedgerTest(
      "TXHideCommandIdToNonSubmittingStakeholders",
      "A transaction should be visible to a non-submitting stakeholder but its command identifier should be empty"
    ) { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        submitter <- alpha.allocateParty()
        listener <- beta.allocateParty()
        (id, _) <- alpha.createAndGetTransactionId(submitter, AgreementFactory(listener, submitter))
        tree <- eventually { beta.transactionTreeById(id, listener) }
      } yield {
        assert(
          tree.commandId.isEmpty,
          s"The command identifier was supposed to be empty but it's `${tree.commandId}` instead.")
      }
    }

  private[this] val filterByTemplate =
    LedgerTest(
      "TXFilterByTemplate",
      "The transaction service should correctly filter by template identifier") { context =>
      val filterBy = Dummy.id
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        create <- ledger.submitAndWaitRequest(
          party,
          Dummy(party).create.command,
          DummyFactory(party).create.command)
        _ <- ledger.submitAndWait(create)
        transactions <- ledger.flatTransactionsByTemplateId(filterBy, party)
      } yield {
        val contract = assertSingleton("FilterByTemplate", transactions.flatMap(createdEvents))
        assertEquals("FilterByTemplate", contract.getTemplateId, Tag.unwrap(filterBy))
      }
    }

  private[this] val useCreateToExercise =
    LedgerTest(
      "TXUseCreateToExercise",
      "Should be able to directly use a contract identifier to exercise a choice") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        dummyFactory <- ledger.create(party, DummyFactory(party))
        transactions <- ledger.exercise(party, dummyFactory.exerciseDummyFactoryCall)
      } yield {
        val events = transactions.rootEventIds.collect(transactions.eventsById)
        val exercised = events.filter(_.kind.isExercised)
        assert(exercised.size == 1, s"Only one exercise expected, got ${exercised.size}")
        assert(
          exercised.head.getExercised.contractId == Tag.unwrap(dummyFactory),
          s"The identifier of the exercised contract should have been ${Tag
            .unwrap(dummyFactory)} but instead it was ${exercised.head.getExercised.contractId}"
        )
      }
    }

  private[this] val contractIdFromExerciseWhenFilter =
    LedgerTest(
      "TXContractIdFromExerciseWhenFilter",
      "Expose contract identifiers that are results of exercising choices when filtering by template") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          factory <- ledger.create(party, DummyFactory(party))
          _ <- ledger.exercise(party, factory.exerciseDummyFactoryCall)
          dummyWithParam <- ledger.flatTransactionsByTemplateId(DummyWithParam.id, party)
          dummyFactory <- ledger.flatTransactionsByTemplateId(DummyFactory.id, party)
        } yield {
          val create = assertSingleton("GetCreate", dummyWithParam.flatMap(createdEvents))
          assertEquals(
            "Create should be of DummyWithParam",
            create.getTemplateId,
            Tag.unwrap(DummyWithParam.id))
          val archive = assertSingleton("GetArchive", dummyFactory.flatMap(archivedEvents))
          assertEquals(
            "Archive should be of DummyFactory",
            archive.getTemplateId,
            Tag.unwrap(DummyFactory.id))
          assertEquals(
            "Mismatching archived contract identifier",
            archive.contractId,
            Tag.unwrap(factory))
        }
    }

  private[this] val rejectOnFailingAssertion =
    LedgerTest("TXRejectOnFailingAssertion", "Reject a transaction on a failing assertion") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          dummy <- ledger.create(party, Dummy(party))
          failure <- ledger
            .exercise(
              party,
              dummy.exerciseConsumeIfTimeIsBetween(
                _,
                Primitive.Timestamp.MAX,
                Primitive.Timestamp.MAX))
            .failed
        } yield {
          assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Assertion failed")
        }
    }

  private[this] val createWithAnyType =
    LedgerTest(
      "TXCreateWithAnyType",
      "Creates should not have issues dealing with any type of argument") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        template = ParameterShowcase(
          party,
          42L,
          BigDecimal("47.0000000000"),
          "some text",
          true,
          Primitive.Timestamp.MIN,
          NestedOptionalInteger(OptionalInteger.SomeInteger(-1L)),
          Primitive.List(0L, 1L, 2L, 3L),
          Primitive.Optional("some optional text")
        )
        create <- ledger.submitAndWaitRequest(party, template.create.command)
        transaction <- ledger.submitAndWaitForTransaction(create)
      } yield {
        val contract = assertSingleton("CreateWithAnyType", createdEvents(transaction))
        assertEquals("CreateWithAnyType", contract.getCreateArguments, template.arguments)
      }
    }

  private[this] val exerciseWithAnyType =
    LedgerTest(
      "TXExerciseWithAnyType",
      "Exercise should not have issues dealing with any type of argument") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        template = ParameterShowcase(
          party,
          42L,
          BigDecimal("47.0000000000"),
          "some text",
          true,
          Primitive.Timestamp.MIN,
          NestedOptionalInteger(OptionalInteger.SomeInteger(-1L)),
          Primitive.List(0L, 1L, 2L, 3L),
          Primitive.Optional("some optional text")
        )
        choice1 = Choice1(
          template.integer,
          BigDecimal("37.0000000000"),
          template.text,
          template.bool,
          template.time,
          template.nestedOptionalInteger,
          template.integerList,
          template.optionalText
        )
        parameterShowcase <- ledger.create(
          party,
          template
        )
        tree <- ledger.exercise(party, parameterShowcase.exerciseChoice1(_, choice1))
      } yield {
        val contract = assertSingleton("ExerciseWithAnyType", exercisedEvents(tree))
        assertEquals("ExerciseWithAnyType", contract.getChoiceArgument, encode(choice1))
      }
    }

  private[this] val submitAVeryLongList =
    LedgerTest("TXVeryLongList", "Accept a submission with a very long list (10,000 items)") {
      context =>
        val n = 10000
        val veryLongList = Primitive.List(List.iterate(0L, n)(_ + 1): _*)
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          template = ParameterShowcase(
            party,
            42L,
            BigDecimal("47.0000000000"),
            "some text",
            true,
            Primitive.Timestamp.MIN,
            NestedOptionalInteger(OptionalInteger.SomeInteger(-1L)),
            veryLongList,
            Primitive.Optional("some optional text")
          )
          create <- ledger.submitAndWaitRequest(party, template.create.command)
          transaction <- ledger.submitAndWaitForTransaction(create)
        } yield {
          val contract = assertSingleton("VeryLongList", createdEvents(transaction))
          assertEquals("VeryLongList", contract.getCreateArguments, template.arguments)
        }
    }

  private[this] val notArchiveNonConsuming =
    LedgerTest(
      "TXNotArchiveNonConsuming",
      "Expressing a non-consuming choice on a contract should not result in its archival") {
      context =>
        for {
          Vector(alpha, beta) <- context.participants(2)
          receiver <- alpha.allocateParty()
          giver <- beta.allocateParty()
          agreementFactory <- beta.create(giver, AgreementFactory(receiver, giver))
          _ <- alpha.exercise(receiver, agreementFactory.exerciseCreateAgreement)
          _ <- synchronize(alpha, beta)
          transactions <- alpha.flatTransactions(receiver, giver)
        } yield {
          assert(
            !transactions.exists(_.events.exists(_.event.isArchived)),
            s"The transaction include an archival: ${transactions.flatMap(_.events).filter(_.event.isArchived)}"
          )
        }
    }

  private[this] val requireAuthorization =
    LedgerTest("TXRequireAuthorization", "Require only authorization of chosen branching signatory") {
      context =>
        for {
          Vector(alpha, beta) <- context.participants(2)
          alice <- alpha.allocateParty()
          bob <- beta.allocateParty()
          template = BranchingSignatories(true, alice, bob)
          _ <- alpha.create(alice, template)
          transactions <- alpha.flatTransactions(alice)
        } yield {
          assert(template.arguments == transactions.head.events.head.getCreated.getCreateArguments)
        }
    }

  private[this] val notDiscloseCreateToNonSignatory =
    LedgerTest(
      "TXNotDiscloseCreateToNonSignatory",
      "Not disclose create to non-chosen branching signatory") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        alice <- alpha.allocateParty()
        bob <- beta.allocateParty()
        template = BranchingSignatories(false, alice, bob)
        create <- beta.submitAndWaitRequest(bob, template.create.command)
        transaction <- beta.submitAndWaitForTransaction(create)
        _ <- synchronize(alpha, beta)
        transactions <- alpha.flatTransactions(alice)
      } yield {
        assert(
          !transactions.exists(_.transactionId != transaction.transactionId),
          s"The transaction ${transaction.transactionId} should not have been disclosed.")
      }
    }

  private[this] val discloseCreateToSignatory =
    LedgerTest("TXDiscloseCreateToSignatory", "Disclose create to the chosen branching controller") {
      context =>
        for {
          Vector(alpha, beta) <- context.participants(2)
          alice <- alpha.allocateParty()
          Vector(bob, eve) <- beta.allocateParties(2)
          template = BranchingControllers(alice, true, bob, eve)
          _ <- alpha.create(alice, template)
          _ <- eventually {
            for {
              aliceView <- alpha.flatTransactions(alice)
              bobView <- beta.flatTransactions(bob)
              evesView <- beta.flatTransactions(eve)
            } yield {
              val aliceCreate = assertSingleton(
                "Alice should see one transaction",
                aliceView.flatMap(createdEvents))
              assertEquals(
                "Alice arguments do not match",
                aliceCreate.getCreateArguments,
                template.arguments)
              val bobCreate =
                assertSingleton("Bob should see one transaction", bobView.flatMap(createdEvents))
              assertEquals(
                "Bob arguments do not match",
                bobCreate.getCreateArguments,
                template.arguments)
              assert(evesView.isEmpty, "Eve should not see any contract")
            }
          }
        } yield {
          // Checks performed in the `eventually` block
        }
    }

  private[this] val notDiscloseCreateToNonChosenBranchingController =
    LedgerTest(
      "TXNotDiscloseCreateToNonChosenBranchingController",
      "Not disclose create to non-chosen branching controller") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        alice <- alpha.allocateParty()
        Vector(bob, eve) <- beta.allocateParties(2)
        template = BranchingControllers(alice, false, bob, eve)
        create <- alpha.submitAndWaitRequest(alice, template.create.command)
        transaction <- alpha.submitAndWaitForTransaction(create)
        _ <- synchronize(alpha, beta)
        transactions <- beta.flatTransactions(bob)
      } yield {
        assert(
          !transactions.exists(_.transactionId != transaction.transactionId),
          s"The transaction ${transaction.transactionId} should not have been disclosed.")
      }
    }

  private[this] val discloseCreateToObservers =
    LedgerTest("TXDiscloseCreateToObservers", "Disclose create to observers") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        alice <- alpha.allocateParty()
        observers <- beta.allocateParties(2)
        template = WithObservers(alice, Primitive.List(observers: _*))
        create <- alpha.submitAndWaitRequest(alice, template.create.command)
        transactionId <- alpha.submitAndWaitForTransactionId(create)
        _ <- eventually {
          for {
            transactions <- beta.flatTransactions(observers: _*)
          } yield {
            assert(transactions.exists(_.transactionId == transactionId))
          }
        }
      } yield {
        // Checks performed in the `eventually` block
      }
    }

  private[this] val unitAsArgumentToNothing =
    LedgerTest("TXUnitAsArgumentToNothing", "DAML engine returns Unit as argument to Nothing") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          template = NothingArgument(party, Primitive.Optional.empty)
          create <- ledger.submitAndWaitRequest(party, template.create.command)
          transaction <- ledger.submitAndWaitForTransaction(create)
        } yield {
          val contract = assertSingleton("UnitAsArgumentToNothing", createdEvents(transaction))
          assertEquals("UnitAsArgumentToNothing", contract.getCreateArguments, template.arguments)
        }
    }

  private[this] val agreementText =
    LedgerTest(
      "TXAgreementText",
      "Expose the agreement text for templates with an explicit agreement text") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        _ <- ledger.create(party, Dummy(party))
        transactions <- ledger.flatTransactionsByTemplateId(Dummy.id, party)
      } yield {
        val contract = assertSingleton("AgreementText", transactions.flatMap(createdEvents))
        assertEquals("AgreementText", contract.getAgreementText, s"'$party' operates a dummy.")
      }
    }

  private[this] val agreementTextDefault =
    LedgerTest(
      "TXAgreementTextDefault",
      "Expose the default text for templates without an agreement text") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        _ <- ledger.create(party, DummyWithParam(party))
        transactions <- ledger.flatTransactions(party)
      } yield {
        val contract = assertSingleton("AgreementTextDefault", transactions.flatMap(createdEvents))
        assertEquals("AgreementTextDefault", contract.getAgreementText, "")
      }
    }

  private[this] val stakeholders =
    LedgerTest("TXStakeholders", "Expose the correct stakeholders") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        receiver <- alpha.allocateParty()
        giver <- beta.allocateParty()
        _ <- beta.create(giver, CallablePayout(giver, receiver))
        transactions <- beta.flatTransactions(giver, receiver)
      } yield {
        val contract = assertSingleton("Stakeholders", transactions.flatMap(createdEvents))
        assertEquals("Signatories", contract.signatories, Seq(Tag.unwrap(giver)))
        assertEquals("Observers", contract.observers, Seq(Tag.unwrap(receiver)))
      }
    }

  private[this] val noContractKey =
    LedgerTest(
      "TXNoContractKey",
      "There should be no contract key if the template does not specify one") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        receiver <- alpha.allocateParty()
        giver <- beta.allocateParty()
        _ <- beta.create(giver, CallablePayout(giver, receiver))
        transactions <- beta.flatTransactions(giver, receiver)
      } yield {
        val contract = assertSingleton("NoContractKey", transactions.flatMap(createdEvents))
        assert(
          contract.getContractKey.sum.isEmpty,
          s"The key is not empty: ${contract.getContractKey}")
      }
    }

  private[this] val contractKey =
    LedgerTest("TXContractKey", "The contract key should be exposed if the template specifies one") {
      context =>
        val expectedKey = "some-fancy-key"
        for {
          ledger <- context.participant()
          tkParty <- ledger.allocateParty()
          _ <- ledger.create(tkParty, TextKey(tkParty, expectedKey, Primitive.List.empty))
          transactions <- ledger.flatTransactions(tkParty)
        } yield {
          val contract = assertSingleton(s"ContractKey", transactions.flatMap(createdEvents))
          assertEquals(
            "ContractKey",
            contract.getContractKey.getRecord.fields,
            Seq(
              RecordField("_1", Some(Value(Value.Sum.Party(Tag.unwrap(tkParty))))),
              RecordField("_2", Some(Value(Value.Sum.Text(expectedKey))))
            )
          )
        }
    }

  private[this] val multiActorChoiceOk =
    LedgerTest("TXMultiActorChoiceOk", "Accept exercising a well-authorized multi-actor choice") {
      context =>
        for {
          Vector(alpha, beta) <- context.participants(2)
          Vector(operator, receiver) <- alpha.allocateParties(2)
          giver <- beta.allocateParty()
          agreementFactory <- beta.create(giver, AgreementFactory(receiver, giver))
          agreement <- alpha.exerciseAndGetContract[Agreement](
            receiver,
            agreementFactory.exerciseAgreementFactoryAccept)
          triProposalTemplate = TriProposal(operator, receiver, giver)
          triProposal <- alpha.create(operator, triProposalTemplate)
          tree <- eventually {
            beta.exercise(giver, agreement.exerciseAcceptTriProposal(_, triProposal))
          }
        } yield {
          val contract = assertSingleton("AcceptTriProposal", createdEvents(tree))
          assertEquals(
            "AcceptTriProposal",
            contract.getCreateArguments.fields,
            triProposalTemplate.arguments.fields)
        }
    }

  private[this] val multiActorChoiceOkCoincidingControllers =
    LedgerTest(
      "TXMultiActorChoiceOkCoincidingControllers",
      "Accept exercising a well-authorized multi-actor choice with coinciding controllers") {
      context =>
        for {
          Vector(alpha, beta) <- context.participants(2)
          operator <- alpha.allocateParty()
          giver <- beta.allocateParty()
          agreementFactory <- beta.create(giver, AgreementFactory(giver, giver))
          agreement <- beta.exerciseAndGetContract[Agreement](
            giver,
            agreementFactory.exerciseAgreementFactoryAccept)
          triProposalTemplate = TriProposal(operator, giver, giver)
          triProposal <- alpha.create(operator, triProposalTemplate)
          tree <- eventually {
            beta.exercise(giver, agreement.exerciseAcceptTriProposal(_, triProposal))
          }
        } yield {
          val contract = assertSingleton("AcceptTriProposalCoinciding", createdEvents(tree))
          assertEquals(
            "AcceptTriProposalCoinciding",
            contract.getCreateArguments.fields,
            triProposalTemplate.arguments.fields)
        }
    }

  private[this] val rejectMultiActorMissingAuth =
    LedgerTest(
      "TXRejectMultiActorMissingAuth",
      "Reject exercising a multi-actor choice with missing authorizers") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        Vector(operator, receiver) <- alpha.allocateParties(2)
        giver <- beta.allocateParty()
        triProposal <- alpha.create(operator, TriProposal(operator, receiver, giver))
        _ <- eventually {
          for {
            failure <- beta.exercise(giver, triProposal.exerciseTriProposalAccept).failed
          } yield {
            assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "requires authorizers")
          }
        }
      } yield {
        // Check performed in the `eventually` block
      }
    }

  // This is the current, most conservative semantics of multi-actor choice authorization.
  // It is likely that this will change in the future. Should we delete this test, we should
  // also remove the 'UnrestrictedAcceptTriProposal' choice from the 'Agreement' template.
  private[this] val rejectMultiActorExcessiveAuth =
    LedgerTest(
      "TXRejectMultiActorExcessiveAuth",
      "Reject exercising a multi-actor choice with too many authorizers") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        Vector(operator, receiver) <- alpha.allocateParties(2)
        giver <- beta.allocateParty()
        agreementFactory <- beta.create(giver, AgreementFactory(receiver, giver))
        agreement <- alpha
          .exerciseAndGetContract[Agreement](
            receiver,
            agreementFactory.exerciseAgreementFactoryAccept)
        triProposalTemplate = TriProposal(operator, giver, giver)
        triProposal <- alpha.create(operator, triProposalTemplate)
        _ <- eventually {
          for {
            failure <- beta
              .exercise(giver, agreement.exerciseAcceptTriProposal(_, triProposal))
              .failed
          } yield {
            assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Assertion failed")
          }
        }
      } yield {
        // Check performed in the `eventually` block
      }
    }

  private[this] val noReorder =
    LedgerTest("TXNoReorder", "Don't reorder fields in data structures of choices") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        dummy <- ledger.create(party, Dummy(party))
        tree <- ledger.exercise(
          party,
          dummy.exerciseWrapWithAddress(_, Address("street", "city", "state", "zip")))
      } yield {
        val contract = assertSingleton("Contract in transaction", createdEvents(tree))
        val fields = assertLength("Fields in contract", 2, contract.getCreateArguments.fields)
        assertEquals(
          "NoReorder",
          fields.flatMap(_.getValue.getRecord.fields).map(_.getValue.getText).zipWithIndex,
          Seq("street" -> 0, "city" -> 1, "state" -> 2, "zip" -> 3))
      }
    }

  private[this] val largeCommand =
    LedgerTest("TXLargeCommand", "Accept huge submissions with a large number of commands") {
      context =>
        val targetNumberOfSubCommands = 1500
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          request <- ledger.submitAndWaitRequest(
            party,
            List.fill(targetNumberOfSubCommands)(Dummy(party).create.command): _*)
          result <- ledger.submitAndWaitForTransaction(request)
        } yield {
          val _ = assertLength("LargeCommand", targetNumberOfSubCommands, result.events)
        }
    }

  private[this] val singleMultiSame =
    LedgerTest(
      "TXSingleMultiSame",
      "The same transaction should be served regardless of subscribing as one or multiple parties") {
      context =>
        for {
          Vector(alpha, beta) <- context.participants(2)
          alice <- alpha.allocateParty()
          bob <- beta.allocateParty()
          _ <- alpha.create(alice, Dummy(alice))
          _ <- beta.create(bob, Dummy(bob))
          aliceView <- alpha.flatTransactions(alice)
          bobView <- beta.flatTransactions(bob)
          _ <- synchronize(alpha, beta)
          alphaView <- alpha.flatTransactions(alice, bob)
          betaView <- beta.flatTransactions(alice, bob)
        } yield {
          val jointView = aliceView ++ bobView
          assertEquals(
            "Single- and multi-party subscription yield different results",
            jointView,
            alphaView)
          assertEquals(
            "Single- and multi-party subscription yield different results",
            jointView,
            betaView)
        }
    }

  private[this] val singleMultiSameTrees =
    LedgerTest(
      "TXSingleMultiSameTrees",
      "The same transaction trees should be served regardless of subscribing as one or multiple parties") {
      context =>
        for {
          Vector(alpha, beta) <- context.participants(2)
          alice <- alpha.allocateParty()
          bob <- beta.allocateParty()
          _ <- alpha.create(alice, Dummy(alice))
          _ <- beta.create(bob, Dummy(bob))
          aliceView <- alpha.transactionTrees(alice)
          bobView <- beta.transactionTrees(bob)
          _ <- synchronize(alpha, beta)
          alphaView <- alpha.transactionTrees(alice, bob)
          betaView <- beta.transactionTrees(alice, bob)
        } yield {
          val jointView = aliceView ++ bobView
          assertEquals(
            "Single- and multi-party subscription yield different results",
            jointView,
            alphaView)
          assertEquals(
            "Single- and multi-party subscription yield different results",
            jointView,
            betaView)
        }
    }

  private[this] val fetchContractCreatedInTransaction =
    LedgerTest(
      "TXFetchContractCreatedInTransaction",
      "It should be possible to fetch a contract created within a transaction") { context =>
      for {
        ledger <- context.participant()
        alice <- ledger.allocateParty()
        createAndFetch <- ledger.create(alice, CreateAndFetch(alice))
        transaction <- ledger.exerciseForFlatTransaction(
          alice,
          createAndFetch.exerciseCreateAndFetch_Run)
      } yield {
        val _ = assertSingleton("There should be only one create", createdEvents(transaction))
        val exercise =
          assertSingleton("There should be only one archive", archivedEvents(transaction))
        assertEquals(
          "The contract identifier of the exercise does not match",
          Tag.unwrap(createAndFetch),
          exercise.contractId)
      }
    }

  private[this] val flatTransactionsWrongLedgerId =
    LedgerTest(
      "TXFlatTransactionsWrongLedgerId",
      "The getTransactions endpoint should reject calls with the wrong ledger identifier") {
      context =>
        val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          invalidRequest = ledger
            .getTransactionsRequest(Seq(party))
            .update(_.ledgerId := invalidLedgerId)
          failure <- ledger.flatTransactions(invalidRequest).failed
        } yield {
          assertGrpcError(
            failure,
            Status.Code.NOT_FOUND,
            s"Ledger ID '$invalidLedgerId' not found.")
        }
    }

  private[this] val transactionTreesWrongLedgerId =
    LedgerTest(
      "TXTransactionTreesWrongLedgerId",
      "The getTransactionTrees endpoint should reject calls with the wrong ledger identifier") {
      context =>
        val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          invalidRequest = ledger
            .getTransactionsRequest(Seq(party))
            .update(_.ledgerId := invalidLedgerId)
          failure <- ledger.transactionTrees(invalidRequest).failed
        } yield {
          assertGrpcError(
            failure,
            Status.Code.NOT_FOUND,
            s"Ledger ID '$invalidLedgerId' not found.")
        }
    }

  private[this] val transactionTreeByIdWrongLedgerId =
    LedgerTest(
      "TXTransactionTreeByIdWrongLedgerId",
      "The getTransactionTreeById endpoint should reject calls with the wrong ledger identifier") {
      context =>
        val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          invalidRequest = ledger
            .getTransactionByIdRequest("not-relevant", Seq(party))
            .update(_.ledgerId := invalidLedgerId)
          failure <- ledger.transactionTreeById(invalidRequest).failed
        } yield {
          assertGrpcError(
            failure,
            Status.Code.NOT_FOUND,
            s"Ledger ID '$invalidLedgerId' not found.")
        }
    }

  private[this] val flatTransactionByIdWrongLedgerId =
    LedgerTest(
      "TXFlatTransactionByIdWrongLedgerId",
      "The getFlatTransactionById endpoint should reject calls with the wrong ledger identifier") {
      context =>
        val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          invalidRequest = ledger
            .getTransactionByIdRequest("not-relevant", Seq(party))
            .update(_.ledgerId := invalidLedgerId)
          failure <- ledger.flatTransactionById(invalidRequest).failed
        } yield {
          assertGrpcError(
            failure,
            Status.Code.NOT_FOUND,
            s"Ledger ID '$invalidLedgerId' not found.")
        }
    }

  private[this] val transactionTreeByEventIdWrongLedgerId =
    LedgerTest(
      "TXTransactionTreeByEventIdWrongLedgerId",
      "The getTransactionTreeByEventId endpoint should reject calls with the wrong ledger identifier") {
      context =>
        val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          invalidRequest = ledger
            .getTransactionByEventIdRequest("not-relevant", Seq(party))
            .update(_.ledgerId := invalidLedgerId)
          failure <- ledger.transactionTreeByEventId(invalidRequest).failed
        } yield {
          assertGrpcError(
            failure,
            Status.Code.NOT_FOUND,
            s"Ledger ID '$invalidLedgerId' not found.")
        }
    }

  private[this] val flatTransactionByEventIdWrongLedgerId =
    LedgerTest(
      "TXFlatTransactionByEventIdWrongLedgerId",
      "The getFlatTransactionByEventId endpoint should reject calls with the wrong ledger identifier") {
      context =>
        val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          invalidRequest = ledger
            .getTransactionByEventIdRequest("not-relevant", Seq(party))
            .update(_.ledgerId := invalidLedgerId)
          failure <- ledger.flatTransactionByEventId(invalidRequest).failed
        } yield {
          assertGrpcError(
            failure,
            Status.Code.NOT_FOUND,
            s"Ledger ID '$invalidLedgerId' not found.")
        }
    }

  private[this] val ledgerEndWrongLedgerId =
    LedgerTest(
      "TXTransactionTreeByIdWrongLedgerId",
      "The ledgerEnd endpoint should reject calls with the wrong ledger identifier") { context =>
      val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
      for {
        ledger <- context.participant()
        failure <- ledger.currentEnd(invalidLedgerId).failed
      } yield {
        assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
      }
    }

  private[this] val transactionTreeById =
    LedgerTest("TXTransactionTreeById", "Expose a visible transaction tree by identifier") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          dummy <- ledger.create(party, Dummy(party))
          tree <- ledger.exercise(party, dummy.exerciseDummyChoice1)
          byId <- ledger.transactionTreeById(tree.transactionId, party)
        } yield {
          assertEquals("The transaction fetched by identifier does not match", tree, byId)
        }
    }

  private[this] val invisibleTransactionTreeById =
    LedgerTest(
      "TXInvisibleTransactionTreeById",
      "Do not expose an invisible transaction tree by identifier") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        party <- alpha.allocateParty()
        intruder <- beta.allocateParty()
        dummy <- alpha.create(party, Dummy(party))
        tree <- alpha.exercise(party, dummy.exerciseDummyChoice1)
        _ <- synchronize(alpha, beta)
        failure <- beta.transactionTreeById(tree.transactionId, intruder).failed
      } yield {
        assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
      }
    }

  private[this] val transactionTreeByIdNotFound =
    LedgerTest(
      "TXTransactionTreeByIdNotFound",
      "Return NOT_FOUND when looking up an inexistent transaction tree by identifier") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        failure <- ledger.transactionTreeById("a" * 60, party).failed
      } yield {
        assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
      }
    }

  private[this] val transactionTreeByIdNoParty =
    LedgerTest(
      "TXTransactionTreeByIdNotFound",
      "Return INVALID_ARGUMENT when looking up a transaction tree by identifier without specifying a party") {
      context =>
        for {
          ledger <- context.participant()
          failure <- ledger.transactionTreeById("not-relevant").failed
        } yield {
          assertGrpcError(
            failure,
            Status.Code.INVALID_ARGUMENT,
            "Missing field: requesting_parties")
        }
    }

  private[this] val transactionTreeByIdSameAsTransactionStream =
    LedgerTest(
      "TXTransactionTreeByIdSameAsTransactionStream",
      "Expose the same events for each transaction as the output of getTransactionTrees") {
      context =>
        for {
          Vector(alpha, beta) <- context.participants(2)
          submitter <- alpha.allocateParty()
          listener <- beta.allocateParty()
          _ <- alpha.create(submitter, AgreementFactory(listener, submitter))
          _ <- synchronize(alpha, beta)
          trees <- alpha.transactionTrees(listener, submitter)
          byId <- Future.sequence(
            trees.map(t => beta.transactionTreeById(t.transactionId, listener, submitter)))
        } yield {
          assertEquals(
            "The events fetched by identifier did not match with the ones on the transaction stream",
            trees,
            byId)
        }
    }

  private[this] val flatTransactionById =
    LedgerTest("TXFlatTransactionById", "Expose a visible transaction by identifier") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        dummy <- ledger.create(party, Dummy(party))
        transaction <- ledger.exerciseForFlatTransaction(party, dummy.exerciseDummyChoice1)
        byId <- ledger.flatTransactionById(transaction.transactionId, party)
      } yield {
        assertEquals("The transaction fetched by identifier does not match", transaction, byId)
      }
    }

  private[this] val invisibleFlatTransactionById =
    LedgerTest(
      "TXInvisibleFlatTransactionById",
      "Do not expose an invisible flat transaction by identifier") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        party <- alpha.allocateParty()
        intruder <- beta.allocateParty()
        dummy <- alpha.create(party, Dummy(party))
        tree <- alpha.exercise(party, dummy.exerciseDummyChoice1)
        _ <- synchronize(alpha, beta)
        failure <- beta.flatTransactionById(tree.transactionId, intruder).failed
      } yield {
        assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
      }
    }

  private[this] val flatTransactionByIdNotFound =
    LedgerTest(
      "TXFlatTransactionByIdNotFound",
      "Return NOT_FOUND when looking up an inexistent flat transaction by identifier") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        failure <- ledger.flatTransactionById("a" * 60, party).failed
      } yield {
        assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
      }
    }

  private[this] val flatTransactionByIdNoParty =
    LedgerTest(
      "TXFlatTransactionByIdNotFound",
      "Return INVALID_ARGUMENT when looking up a flat transaction by identifier without specifying a party") {
      context =>
        for {
          ledger <- context.participant()
          failure <- ledger.flatTransactionById("not-relevant").failed
        } yield {
          assertGrpcError(
            failure,
            Status.Code.INVALID_ARGUMENT,
            "Missing field: requesting_parties")
        }
    }

  private[this] val flatTransactionByIdSameAsTransactionStream =
    LedgerTest(
      "TXFlatTransactionByIdSameAsTransactionStream",
      "Expose the same events for each transaction as the output of getTransactions") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        submitter <- alpha.allocateParty()
        listener <- beta.allocateParty()
        _ <- alpha.create(submitter, AgreementFactory(listener, submitter))
        _ <- synchronize(alpha, beta)
        transactions <- alpha.flatTransactions(listener, submitter)
        byId <- Future.sequence(
          transactions.map(t => beta.flatTransactionById(t.transactionId, listener, submitter)))
      } yield {
        assertEquals(
          "The events fetched by identifier did not match with the ones on the transaction stream",
          transactions,
          byId)
      }
    }

  private[this] val transactionTreeByEventId =
    LedgerTest(
      "TXTransactionTreeByEventId",
      "Expose a visible transaction tree by event identifier") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        dummy <- ledger.create(party, Dummy(party))
        tree <- ledger.exercise(party, dummy.exerciseDummyChoice1)
        byId <- ledger.transactionTreeByEventId(tree.rootEventIds.head, party)
      } yield {
        assertEquals("The transaction fetched by identifier does not match", tree, byId)
      }
    }

  private[this] val invisibleTransactionTreeByEventId =
    LedgerTest(
      "TXInvisibleTransactionTreeByEventId",
      "Do not expose an invisible transaction tree by event identifier") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        party <- alpha.allocateParty()
        intruder <- beta.allocateParty()
        dummy <- alpha.create(party, Dummy(party))
        tree <- alpha.exercise(party, dummy.exerciseDummyChoice1)
        _ <- synchronize(alpha, beta)
        failure <- beta.transactionTreeByEventId(tree.rootEventIds.head, intruder).failed
      } yield {
        assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
      }
    }

  private[this] val transactionTreeByEventIdInvalid =
    LedgerTest(
      "TXTransactionTreeByEventIdInvalid",
      "Return INVALID when looking up an invalid transaction tree by event identifier") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        failure <- ledger.transactionTreeByEventId("dont' worry, be happy", party).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Invalid field event_id")
      }
    }

  private[this] val transactionTreeByEventIdNotFound =
    LedgerTest(
      "TXTransactionTreeByEventIdNotFound",
      "Return NOT_FOUND when looking up an inexistent transaction tree by event identifier") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          failure <- ledger.transactionTreeByEventId(s"#${"a" * 60}:000", party).failed
        } yield {
          assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
        }
    }

  private[this] val transactionTreeByEventIdNoParty =
    LedgerTest(
      "TXTransactionTreeByEventIdNotFound",
      "Return INVALID_ARGUMENT when looking up a transaction tree by event identifier without specifying a party") {
      context =>
        for {
          ledger <- context.participant()
          failure <- ledger.transactionTreeByEventId("not-relevant").failed
        } yield {
          assertGrpcError(
            failure,
            Status.Code.INVALID_ARGUMENT,
            "Missing field: requesting_parties")
        }
    }

  private[this] val flatTransactionByEventId =
    LedgerTest(
      "TXFlatTransactionByEventId",
      "Expose a visible flat transaction by event identifier") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        dummy <- ledger.create(party, Dummy(party))
        transaction <- ledger.exerciseForFlatTransaction(party, dummy.exerciseDummyChoice1)
        event = transaction.events.head.event
        eventId = event.archived.map(_.eventId).get
        byId <- ledger.flatTransactionByEventId(eventId, party)
      } yield {
        assertEquals("The transaction fetched by identifier does not match", transaction, byId)
      }
    }

  private[this] val invisibleFlatTransactionByEventId =
    LedgerTest(
      "TXInvisibleFlatTransactionByEventId",
      "Do not expose an invisible flat transaction by event identifier") { context =>
      for {
        Vector(alpha, beta) <- context.participants(2)
        party <- alpha.allocateParty()
        intruder <- beta.allocateParty()
        dummy <- alpha.create(party, Dummy(party))
        tree <- alpha.exercise(party, dummy.exerciseDummyChoice1)
        _ <- synchronize(alpha, beta)
        failure <- beta.flatTransactionByEventId(tree.rootEventIds.head, intruder).failed
      } yield {
        assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
      }
    }

  private[this] val flatTransactionByEventIdInvalid =
    LedgerTest(
      "TXFlatTransactionByEventIdInvalid",
      "Return INVALID when looking up a flat transaction by an invalid event identifier") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          failure <- ledger.flatTransactionByEventId("dont' worry, be happy", party).failed
        } yield {
          assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Invalid field event_id")
        }
    }

  private[this] val flatTransactionByEventIdNotFound =
    LedgerTest(
      "TXFlatTransactionByEventIdNotFound",
      "Return NOT_FOUND when looking up an inexistent flat transaction by event identifier") {
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          failure <- ledger.flatTransactionByEventId(s"#${"a" * 60}:000", party).failed
        } yield {
          assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
        }
    }

  private[this] val flatTransactionByEventIdNoParty =
    LedgerTest(
      "TXFlatTransactionByEventIdNotFound",
      "Return INVALID_ARGUMENT when looking up a flat transaction by event identifier without specifying a party") {
      context =>
        for {
          ledger <- context.participant()
          failure <- ledger.flatTransactionByEventId("not-relevant").failed
        } yield {
          assertGrpcError(
            failure,
            Status.Code.INVALID_ARGUMENT,
            "Missing field: requesting_parties")
        }
    }

  private def checkTransactionsOrder(
      context: String,
      transactions: Vector[Transaction],
      contracts: Int): Unit = {
    val (cs, as) =
      transactions.flatMap(_.events).zipWithIndex.partition {
        case (e, _) => e.event.isCreated
      }
    val creations = cs.map { case (e, i) => e.getCreated.contractId -> i }
    val archivals = as.map { case (e, i) => e.getArchived.contractId -> i }
    assert(
      creations.size == contracts && archivals.size == contracts,
      s"$context: either the number of archive events (${archivals.size}) or the number of create events (${creations.size}) doesn't match the expected number of $contracts."
    )
    val createdContracts = creations.iterator.map(_._1).toSet
    val archivedContracts = archivals.iterator.map(_._1).toSet
    assert(
      createdContracts.size == creations.size,
      s"$context: there are duplicate contract identifiers in the create events"
    )
    assert(
      archivedContracts.size == archivals.size,
      s"$context: there are duplicate contract identifiers in the archive events"
    )
    assert(
      createdContracts == archivedContracts,
      s"$context: the contract identifiers for created and archived contracts differ: ${createdContracts
        .diff(archivedContracts)}"
    )
    val sortedCreations = creations.sortBy(_._1)
    val sortedArchivals = archivals.sortBy(_._1)
    for (i <- 0 until contracts) {
      val (createdContract, creationIndex) = sortedCreations(i)
      val (archivedContract, archivalIndex) = sortedArchivals(i)
      assert(
        createdContract == archivedContract,
        s"$context: unexpected discrepancy between the created and archived events")
      assert(
        creationIndex < archivalIndex,
        s"$context: the creation of $createdContract did not appear in the stream before it's archival")
    }
  }

  private[this] val singleSubscriptionInOrder =
    LedgerTest(
      "TXSingleSubscriptionInOrder",
      "Archives should always come after creations when subscribing as a single party") {
      val contracts = 50
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          _ <- Future.sequence(
            Vector.fill(contracts)(
              ledger
                .create(party, Dummy(party))
                .flatMap(contract => ledger.exercise(party, contract.exerciseDummyChoice1))))
          transactions <- ledger.flatTransactions(party)
        } yield {
          checkTransactionsOrder("Ledger", transactions, contracts)
        }
    }

  private[this] val multiSubscriptionInOrder =
    LedgerTest(
      "TXMultiSubscriptionInOrder",
      "Archives should always come after creations when subscribing as more than on party") {
      val contracts = 50
      context =>
        for {
          Vector(alpha, beta) <- context.participants(2)
          alice <- alpha.allocateParty()
          bob <- beta.allocateParty()
          _ <- Future.sequence(
            Vector.tabulate(contracts)(
              n =>
                if (n % 2 == 0)
                  alpha
                    .create(alice, Dummy(alice))
                    .flatMap(contract => alpha.exercise(alice, contract.exerciseDummyChoice1))
                else
                  beta
                    .create(bob, Dummy(bob))
                    .flatMap(contract => beta.exercise(bob, contract.exerciseDummyChoice1)))
          )
          _ <- synchronize(alpha, beta)
          aliceView <- alpha.flatTransactions(alice, bob)
          bobView <- beta.flatTransactions(alice, bob)
        } yield {
          checkTransactionsOrder("Alpha", aliceView, contracts)
          checkTransactionsOrder("Beta", bobView, contracts)
        }
    }

  private[this] val flatSubsetOfTrees =
    LedgerTest(
      "TXFlatSubsetOfTrees",
      "The event identifiers in the flat stream should be a subset of those in the trees stream") {
      val contracts = 50
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          _ <- Future.sequence(
            Vector.fill(contracts)(
              ledger
                .create(party, Dummy(party))
                .flatMap(contract => ledger.exercise(party, contract.exerciseDummyChoice1))))
          transactions <- ledger.flatTransactions(party)
          trees <- ledger.transactionTrees(party)
        } yield {
          assert(
            transactions
              .flatMap(_.events.map(e =>
                e.event.archived.map(_.eventId).orElse(e.event.created.map(_.eventId)).get))
              .toSet
              .subsetOf(trees.flatMap(_.eventsById.keys).toSet))
        }
    }

  private[this] val flatWitnessesSubsetOfTrees =
    LedgerTest(
      "TXFlatWitnessesSubsetOfTrees",
      "The witnesses in the flat stream should be a subset of those in the trees stream") {
      val contracts = 50
      context =>
        for {
          ledger <- context.participant()
          party <- ledger.allocateParty()
          _ <- Future.sequence(
            Vector.fill(contracts)(
              ledger
                .create(party, Dummy(party))
                .flatMap(contract => ledger.exercise(party, contract.exerciseDummyChoice1))))
          transactions <- ledger.flatTransactions(party)
          trees <- ledger.transactionTrees(party)
        } yield {
          val witnessesByEventIdInTreesStream =
            trees.iterator
              .flatMap(_.eventsById)
              .map {
                case (id, event) =>
                  id -> event.kind.exercised
                    .map(_.witnessParties.toSet)
                    .orElse(event.kind.created.map(_.witnessParties.toSet))
                    .get
              }
              .toMap
          val witnessesByEventIdInFlatStream =
            transactions
              .flatMap(
                _.events.map(
                  e =>
                    e.event.archived
                      .map(a => a.eventId -> a.witnessParties.toSet)
                      .orElse(e.event.created.map(c => c.eventId -> c.witnessParties.toSet))
                      .get))
          for ((event, witnesses) <- witnessesByEventIdInFlatStream) {
            assert(witnesses.subsetOf(witnessesByEventIdInTreesStream(event)))
          }
        }
    }

  override val tests: Vector[LedgerTest] = Vector(
    beginToBeginShouldBeEmpty,
    treesBeginToBeginShouldBeEmpty,
    endToEndShouldBeEmpty,
    serveElementsUntilCancellation,
    serveTreesUntilCancellation,
    deduplicateCommands,
    rejectEmptyFilter,
    completeOnLedgerEnd,
    completeTreesOnLedgerEnd,
    processInTwoChunks,
    identicalAndParallel,
    notDivulgeToUnrelatedParties,
    rejectBeginAfterEnd,
    hideCommandIdToNonSubmittingStakeholders,
    filterByTemplate,
    useCreateToExercise,
    contractIdFromExerciseWhenFilter,
    rejectOnFailingAssertion,
    createWithAnyType,
    exerciseWithAnyType,
    submitAVeryLongList,
    notArchiveNonConsuming,
    requireAuthorization,
    notDiscloseCreateToNonSignatory,
    discloseCreateToSignatory,
    notDiscloseCreateToNonChosenBranchingController,
    discloseCreateToObservers,
    unitAsArgumentToNothing,
    agreementText,
    agreementTextDefault,
    stakeholders,
    noContractKey,
    contractKey,
    multiActorChoiceOk,
    multiActorChoiceOkCoincidingControllers,
    rejectMultiActorMissingAuth,
    rejectMultiActorExcessiveAuth,
    noReorder,
    largeCommand,
    singleMultiSame,
    singleMultiSameTrees,
    fetchContractCreatedInTransaction,
    flatTransactionsWrongLedgerId,
    transactionTreesWrongLedgerId,
    flatTransactionByIdWrongLedgerId,
    transactionTreeByIdWrongLedgerId,
    flatTransactionByEventIdWrongLedgerId,
    transactionTreeByEventIdWrongLedgerId,
    ledgerEndWrongLedgerId,
    transactionTreeById,
    invisibleTransactionTreeById,
    transactionTreeByIdNotFound,
    transactionTreeByIdNoParty,
    transactionTreeByIdSameAsTransactionStream,
    flatTransactionById,
    invisibleFlatTransactionById,
    flatTransactionByIdNotFound,
    flatTransactionByIdNoParty,
    flatTransactionByIdSameAsTransactionStream,
    transactionTreeByEventId,
    invisibleTransactionTreeByEventId,
    transactionTreeByEventIdInvalid,
    transactionTreeByEventIdNotFound,
    transactionTreeByEventIdNoParty,
    flatTransactionByEventId,
    invisibleFlatTransactionByEventId,
    flatTransactionByEventIdInvalid,
    flatTransactionByEventIdNotFound,
    flatTransactionByEventIdNoParty,
    singleSubscriptionInOrder,
    multiSubscriptionInOrder,
    flatSubsetOfTrees,
    flatWitnessesSubsetOfTrees
  )
}
