// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.testtool.suites.TransactionServiceIT.{
  comparableTransactionTrees,
  comparableTransactions,
}
import com.daml.ledger.api.v1.event.Event.Event
import com.daml.ledger.api.v1.transaction.TreeEvent.Kind.Exercised
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree, TreeEvent}
import com.daml.ledger.client.binding
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.client.binding.Value.encode
import com.daml.ledger.test.model.Iou.Iou
import com.daml.ledger.test.model.Iou.Iou._
import com.daml.ledger.test.model.Iou.IouTransfer._
import com.daml.ledger.test.model.IouTrade.IouTrade
import com.daml.ledger.test.model.IouTrade.IouTrade._
import com.daml.ledger.test.model.Test.Agreement._
import com.daml.ledger.test.model.Test.AgreementFactory._
import com.daml.ledger.test.model.Test.Choice1._
import com.daml.ledger.test.model.Test.CreateAndFetch._
import com.daml.ledger.test.model.Test.Dummy._
import com.daml.ledger.test.model.Test.DummyFactory._
import com.daml.ledger.test.model.Test.ParameterShowcase._
import com.daml.ledger.test.model.Test.TriProposal._
import com.daml.ledger.test.model.Test._
import com.daml.platform.api.v1.event.EventOps.{EventOps, TreeEventOps}
import io.grpc.Status
import scalaz.Tag

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent.Future

class TransactionServiceIT extends LedgerTestSuite {
  test(
    "TXBeginToBegin",
    "An empty stream should be served when getting transactions from and to the beginning of the ledger",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.getTransactionsRequest(Seq(party))
    val fromAndToBegin = request.update(_.begin := ledger.begin, _.end := ledger.begin)
    for {
      transactions <- ledger.flatTransactions(fromAndToBegin)
    } yield {
      assert(
        transactions.isEmpty,
        s"Received a non-empty stream with ${transactions.size} transactions in it.",
      )
    }
  })

  test(
    "TXTreesBeginToBegin",
    "An empty stream of trees should be served when getting transactions from and to the beginning of the ledger",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.getTransactionsRequest(Seq(party))
    val fromAndToBegin = request.update(_.begin := ledger.begin, _.end := ledger.begin)
    for {
      transactions <- ledger.transactionTrees(fromAndToBegin)
    } yield {
      assert(
        transactions.isEmpty,
        s"Received a non-empty stream with ${transactions.size} transactions in it.",
      )
    }
  })

  test(
    "TXEndToEnd",
    "An empty stream should be served when getting transactions from and to the end of the ledger",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, Dummy(party))
      request = ledger.getTransactionsRequest(Seq(party))
      endToEnd = request.update(_.begin := ledger.end, _.end := ledger.end)
      transactions <- ledger.flatTransactions(endToEnd)
    } yield {
      assert(
        transactions.isEmpty,
        s"No transactions were expected but ${transactions.size} were read",
      )
    }
  })

  test(
    "TXAfterEnd",
    "An OUT_OF_RANGE error should be returned when subscribing past the ledger end",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, Dummy(party))
      futureOffset <- ledger.offsetBeyondLedgerEnd()
      request = ledger.getTransactionsRequest(Seq(party))
      beyondEnd = request.update(_.begin := futureOffset, _.optionalEnd := None)
      failure <- ledger.flatTransactions(beyondEnd).mustFail("subscribing past the ledger end")
    } yield {
      assertGrpcError(failure, Status.Code.OUT_OF_RANGE, "is after ledger end")
    }
  })

  test(
    "TXTreesAfterEnd",
    "An OUT_OF_RANGE error should be returned when subscribing to trees past the ledger end",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, Dummy(party))
      futureOffset <- ledger.offsetBeyondLedgerEnd()
      request = ledger.getTransactionsRequest(Seq(party))
      beyondEnd = request.update(_.begin := futureOffset, _.optionalEnd := None)
      failure <- ledger.transactionTrees(beyondEnd).mustFail("subscribing past the ledger end")
    } yield {
      assertGrpcError(failure, Status.Code.OUT_OF_RANGE, "is after ledger end")
    }
  })

  test(
    "TXServeUntilCancellation",
    "Items should be served until the client cancels",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val transactionsToSubmit = 14
    val transactionsToRead = 10
    for {
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party)))
      )
      transactions <- ledger.flatTransactions(transactionsToRead, party)
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        transactions.size == transactionsToRead,
        s"$transactionsToRead should have been received but ${transactions.size} were instead",
      )
    }
  })

  test(
    "TXServeTreesUntilCancellation",
    "Trees should be served until the client cancels",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val transactionsToSubmit = 14
    val treesToRead = 10
    for {
      dummies <- Future.sequence(
        Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party)))
      )
      trees <- ledger.transactionTrees(treesToRead, party)
    } yield {
      assert(
        dummies.size == transactionsToSubmit,
        s"$transactionsToSubmit should have been submitted but ${dummies.size} were instead",
      )
      assert(
        trees.size == treesToRead,
        s"$treesToRead should have been received but ${trees.size} were instead",
      )
    }
  })

  test(
    "TXTreeBlinding",
    "Trees should be served according to the blinding/projection rules",
    allocate(TwoParties, SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(
          Participant(alpha, alice, gbp_bank),
          Participant(beta, bob),
          Participant(delta, dkk_bank),
        ) =>
      for {
        gbpIouIssue <- alpha.create(gbp_bank, Iou(gbp_bank, gbp_bank, "GBP", 100, Nil))
        gbpTransfer <-
          alpha.exerciseAndGetContract(gbp_bank, gbpIouIssue.exerciseIou_Transfer(_, alice))
        dkkIouIssue <- delta.create(dkk_bank, Iou(dkk_bank, dkk_bank, "DKK", 110, Nil))
        dkkTransfer <-
          delta.exerciseAndGetContract(dkk_bank, dkkIouIssue.exerciseIou_Transfer(_, bob))

        aliceIou1 <- eventually {
          alpha.exerciseAndGetContract(alice, gbpTransfer.exerciseIouTransfer_Accept(_))
        }
        aliceIou <- eventually {
          alpha.exerciseAndGetContract(alice, aliceIou1.exerciseIou_AddObserver(_, bob))
        }
        bobIou <- eventually {
          beta.exerciseAndGetContract(bob, dkkTransfer.exerciseIouTransfer_Accept(_))
        }

        trade <- eventually {
          alpha.create(
            alice,
            IouTrade(alice, bob, aliceIou, gbp_bank, "GBP", 100, dkk_bank, "DKK", 110),
          )
        }
        tree <- eventually { beta.exercise(bob, trade.exerciseIouTrade_Accept(_, bobIou)) }

        aliceTree <- eventually { alpha.transactionTreeById(tree.transactionId, alice) }
        bobTree <- beta.transactionTreeById(tree.transactionId, bob)
        gbpTree <- eventually { alpha.transactionTreeById(tree.transactionId, gbp_bank) }
        dkkTree <- eventually { delta.transactionTreeById(tree.transactionId, dkk_bank) }

      } yield {
        def treeIsWellformed(tree: TransactionTree): Unit = {
          val eventsToObserve = mutable.Map.empty[String, TreeEvent] ++= tree.eventsById

          def go(eventId: String): Unit = {
            eventsToObserve.remove(eventId) match {
              case Some(TreeEvent(Exercised(exercisedEvent))) =>
                exercisedEvent.childEventIds.foreach(go)
              case Some(TreeEvent(_)) =>
                ()
              case None =>
                throw new AssertionError(
                  s"Referenced eventId $eventId is not available as node in the transaction."
                )
            }
          }
          tree.rootEventIds.foreach(go)
          assert(
            eventsToObserve.isEmpty,
            s"After traversing the transaction, there are still unvisited nodes: $eventsToObserve",
          )
        }

        treeIsWellformed(aliceTree)
        treeIsWellformed(bobTree)
        treeIsWellformed(gbpTree)
        treeIsWellformed(dkkTree)

        // both bob and alice see the entire transaction:
        // 1x Exercise IouTrade.IouTrade_Accept
        // 2 x Iou transfer with 4 nodes each (see below)
        assert(aliceTree.eventsById.size == 9)
        assert(bobTree.eventsById.size == 9)

        assert(aliceTree.rootEventIds.size == 1)
        assert(bobTree.rootEventIds.size == 1)

        // banks only see the transfer of their issued Iou:
        // Exercise Iou.Iou_Transfer -> Create IouTransfer
        // Exercise IouTransfer.IouTransfer_Accept -> Create Iou
        assert(gbpTree.eventsById.size == 4)
        assert(dkkTree.eventsById.size == 4)

        // the exercises are the root nodes
        assert(gbpTree.rootEventIds.size == 2)
        assert(dkkTree.rootEventIds.size == 2)

      }
  })

  test(
    "TXRejectEmptyFilter",
    "A query with an empty transaction filter should be rejected with an INVALID_ARGUMENT status",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val request = ledger.getTransactionsRequest(Seq(party))
    val requestWithEmptyFilter = request.update(_.filter.filtersByParty := Map.empty)
    for {
      failure <- ledger
        .flatTransactions(requestWithEmptyFilter)
        .mustFail("subscribing with an empty filter")
    } yield {
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "filtersByParty cannot be empty")
    }
  })

  test(
    "TXCompleteOnLedgerEnd",
    "A stream should complete as soon as the ledger end is hit",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val transactionsToSubmit = 14
    val transactionsFuture = ledger.flatTransactions(party)
    for {
      _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
      _ <- transactionsFuture
    } yield {
      // doing nothing: we are just checking that `transactionsFuture` completes successfully
    }
  })

  test(
    "TXCompleteTreesOnLedgerEnd",
    "A stream of trees should complete as soon as the ledger end is hit",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val transactionsToSubmit = 14
    val transactionsFuture = ledger.transactionTrees(party)
    for {
      _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
      _ <- transactionsFuture
    } yield {
      // doing nothing: we are just checking that `transactionsFuture` completes successfully
    }
  })

  test(
    "TXProcessInTwoChunks",
    "Serve the complete sequence of transactions even if processing is stopped and resumed",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val transactionsToSubmit = 5
    for {
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
          s"second section: ${secondSection.map(_.commandId).mkString(", ")}",
      )
    }
  })

  test(
    "TXParallel",
    "The same data should be served for more than 1 identical, parallel requests",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val transactionsToSubmit = 5
    val parallelRequests = 10
    for {
      _ <- Future.sequence(Vector.fill(transactionsToSubmit)(ledger.create(party, Dummy(party))))
      results <- Future.sequence(Vector.fill(parallelRequests)(ledger.flatTransactions(party)))
    } yield {
      assert(
        results.toSet.size == 1,
        s"All requests are supposed to return the same results but there " +
          s"where differences: ${results.map(_.map(_.commandId)).mkString(", ")}",
      )
    }
  })

  test(
    "TXNotDivulge",
    "Data should not be exposed to parties unrelated to a transaction",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(_, bob)) =>
    for {
      _ <- alpha.create(alice, Dummy(alice))
      bobsView <- alpha.flatTransactions(bob)
    } yield {
      assert(
        bobsView.isEmpty,
        s"After Alice create a contract, Bob sees one or more transaction he shouldn't, namely those created by commands ${bobsView.map(_.commandId).mkString(", ")}",
      )
    }
  })

  test(
    "TXRejectBeginAfterEnd",
    "A request with the end before the begin should be rejected with INVALID_ARGUMENT",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      earlier <- ledger.currentEnd()
      _ <- ledger.create(party, Dummy(party))
      later <- ledger.currentEnd()
      request = ledger.getTransactionsRequest(Seq(party))
      invalidRequest = request.update(_.begin := later, _.end := earlier)
      failure <- ledger
        .flatTransactions(invalidRequest)
        .mustFail("subscribing with the end before the begin")
    } yield {
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "is before Begin offset")
    }
  })

  test(
    "TXTreeHideCommandIdToNonSubmittingStakeholders",
    "A transaction tree should be visible to a non-submitting stakeholder but its command identifier should be empty",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, submitter), Participant(beta, listener)) =>
    for {
      (id, _) <- alpha.createAndGetTransactionId(submitter, AgreementFactory(listener, submitter))
      _ <- synchronize(alpha, beta)
      tree <- beta.transactionTreeById(id, listener)
      treesFromStream <- beta.transactionTrees(listener)
    } yield {
      assert(
        tree.commandId.isEmpty,
        s"The command identifier for the transaction tree was supposed to be empty but it's `${tree.commandId}` instead.",
      )

      assert(
        treesFromStream.size == 1,
        s"One transaction tree expected but got ${treesFromStream.size} instead.",
      )

      val treeFromStreamCommandId = treesFromStream.head.commandId
      assert(
        treeFromStreamCommandId.isEmpty,
        s"The command identifier for the transaction tree was supposed to be empty but it's `$treeFromStreamCommandId` instead.",
      )
    }
  })

  test(
    "TXHideCommandIdToNonSubmittingStakeholders",
    "A flat transaction should be visible to a non-submitting stakeholder but its command identifier should be empty",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, submitter), Participant(beta, listener)) =>
      for {
        (id, _) <- alpha.createAndGetTransactionId(submitter, AgreementFactory(listener, submitter))
        _ <- synchronize(alpha, beta)
        flatTx <- beta.flatTransactionById(id, listener)
        flatsFromStream <- beta.flatTransactions(listener)
      } yield {

        assert(
          flatTx.commandId.isEmpty,
          s"The command identifier for the flat transaction was supposed to be empty but it's `${flatTx.commandId}` instead.",
        )

        assert(
          flatsFromStream.size == 1,
          s"One flat transaction expected but got ${flatsFromStream.size} instead.",
        )

        val flatTxFromStreamCommandId = flatsFromStream.head.commandId
        assert(
          flatTxFromStreamCommandId.isEmpty,
          s"The command identifier for the flat transaction was supposed to be empty but it's `$flatTxFromStreamCommandId` instead.",
        )
      }
  })

  test(
    "TXFilterByTemplate",
    "The transaction service should correctly filter by template identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val filterBy = Dummy.id
    val create = ledger.submitAndWaitRequest(
      party,
      Dummy(party).create.command,
      DummyFactory(party).create.command,
    )
    for {
      _ <- ledger.submitAndWait(create)
      transactions <- ledger.flatTransactionsByTemplateId(filterBy, party)
    } yield {
      val contract = assertSingleton("FilterByTemplate", transactions.flatMap(createdEvents))
      assertEquals("FilterByTemplate", contract.getTemplateId, Tag.unwrap(filterBy))
    }
  })

  test(
    "TXUseCreateToExercise",
    "Should be able to directly use a contract identifier to exercise a choice",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummyFactory <- ledger.create(party, DummyFactory(party))
      transactions <- ledger.exercise(party, dummyFactory.exerciseDummyFactoryCall)
    } yield {
      val events = transactions.rootEventIds.collect(transactions.eventsById)
      val exercised = events.filter(_.kind.isExercised)
      assert(exercised.size == 1, s"Only one exercise expected, got ${exercised.size}")
      assert(
        exercised.head.getExercised.contractId == Tag.unwrap(dummyFactory),
        s"The identifier of the exercised contract should have been ${Tag
          .unwrap(dummyFactory)} but instead it was ${exercised.head.getExercised.contractId}",
      )
    }
  })

  test(
    "TXContractIdFromExerciseWhenFilter",
    "Expose contract identifiers that are results of exercising choices when filtering by template",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      factory <- ledger.create(party, DummyFactory(party))
      _ <- ledger.exercise(party, factory.exerciseDummyFactoryCall)
      dummyWithParam <- ledger.flatTransactionsByTemplateId(DummyWithParam.id, party)
      dummyFactory <- ledger.flatTransactionsByTemplateId(DummyFactory.id, party)
    } yield {
      val create = assertSingleton("GetCreate", dummyWithParam.flatMap(createdEvents))
      assertEquals(
        "Create should be of DummyWithParam",
        create.getTemplateId,
        Tag.unwrap(DummyWithParam.id),
      )
      val archive = assertSingleton("GetArchive", dummyFactory.flatMap(archivedEvents))
      assertEquals(
        "Archive should be of DummyFactory",
        archive.getTemplateId,
        Tag.unwrap(DummyFactory.id),
      )
      assertEquals(
        "Mismatching archived contract identifier",
        archive.contractId,
        Tag.unwrap(factory),
      )
    }
  })

  test(
    "TXRejectOnFailingAssertion",
    "Reject a transaction on a failing assertion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      failure <- ledger
        .exercise(
          party,
          dummy
            .exerciseConsumeIfTimeIsBetween(_, Primitive.Timestamp.MAX, Primitive.Timestamp.MAX),
        )
        .mustFail("exercising with a failing assertion")
    } yield {
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Assertion failed")
    }
  })

  test(
    "TXCreateWithAnyType",
    "Creates should not have issues dealing with any type of argument",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val template = ParameterShowcase(
      operator = party,
      integer = 42L,
      decimal = BigDecimal("47.0000000000"),
      text = "some text",
      bool = true,
      time = Primitive.Timestamp.MIN,
      nestedOptionalInteger = NestedOptionalInteger(OptionalInteger.SomeInteger(-1L)),
      integerList = Primitive.List(0L, 1L, 2L, 3L),
      optionalText = Primitive.Optional("some optional text"),
    )
    val create = ledger.submitAndWaitRequest(party, template.create.command)
    for {
      transaction <- ledger.submitAndWaitForTransaction(create)
    } yield {
      val contract = assertSingleton("CreateWithAnyType", createdEvents(transaction))
      assertEquals("CreateWithAnyType", contract.getCreateArguments, template.arguments)
    }
  })

  test(
    "TXExerciseWithAnyType",
    "Exercise should not have issues dealing with any type of argument",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val template = ParameterShowcase(
      operator = party,
      integer = 42L,
      decimal = BigDecimal("47.0000000000"),
      text = "some text",
      bool = true,
      time = Primitive.Timestamp.MIN,
      nestedOptionalInteger = NestedOptionalInteger(OptionalInteger.SomeInteger(-1L)),
      integerList = Primitive.List(0L, 1L, 2L, 3L),
      optionalText = Primitive.Optional("some optional text"),
    )
    val choice1 = Choice1(
      template.integer,
      BigDecimal("37.0000000000"),
      template.text,
      template.bool,
      template.time,
      template.nestedOptionalInteger,
      template.integerList,
      template.optionalText,
    )
    for {
      parameterShowcase <- ledger.create(
        party,
        template,
      )
      tree <- ledger.exercise(party, parameterShowcase.exerciseChoice1(_, choice1))
    } yield {
      val contract = assertSingleton("ExerciseWithAnyType", exercisedEvents(tree))
      assertEquals("ExerciseWithAnyType", contract.getChoiceArgument, encode(choice1))
    }
  })

  test(
    "TXVeryLongList",
    "Accept a submission with a very long list (10,000 items)",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val n = 10000
    val veryLongList = Primitive.List(List.iterate(0L, n)(_ + 1): _*)
    val template = ParameterShowcase(
      operator = party,
      integer = 42L,
      decimal = BigDecimal("47.0000000000"),
      text = "some text",
      bool = true,
      time = Primitive.Timestamp.MIN,
      nestedOptionalInteger = NestedOptionalInteger(OptionalInteger.SomeInteger(-1L)),
      integerList = veryLongList,
      optionalText = Primitive.Optional("some optional text"),
    )
    val create = ledger.submitAndWaitRequest(party, template.create.command)
    for {
      transaction <- ledger.submitAndWaitForTransaction(create)
    } yield {
      val contract = assertSingleton("VeryLongList", createdEvents(transaction))
      assertEquals("VeryLongList", contract.getCreateArguments, template.arguments)
    }
  })

  test(
    "TXNotArchiveNonConsuming",
    "Expressing a non-consuming choice on a contract should not result in its archival",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, receiver), Participant(beta, giver)) =>
    for {
      agreementFactory <- beta.create(giver, AgreementFactory(receiver, giver))
      _ <- eventually { alpha.exercise(receiver, agreementFactory.exerciseCreateAgreement) }
      _ <- synchronize(alpha, beta)
      transactions <- alpha.flatTransactions(receiver, giver)
    } yield {
      assert(
        !transactions.exists(_.events.exists(_.event.isArchived)),
        s"The transaction include an archival: ${transactions.flatMap(_.events).filter(_.event.isArchived)}",
      )
    }
  })

  test(
    "TXRequireAuthorization",
    "Require only authorization of chosen branching signatory",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(_, bob)) =>
    val template = BranchingSignatories(whichSign = true, signTrue = alice, signFalse = bob)
    for {
      _ <- alpha.create(alice, template)
      transactions <- alpha.flatTransactions(alice)
    } yield {
      assert(template.arguments == transactions.head.events.head.getCreated.getCreateArguments)
    }
  })

  test(
    "TXNotDiscloseCreateToNonSignatory",
    "Not disclose create to non-chosen branching signatory",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    val template = BranchingSignatories(whichSign = false, signTrue = alice, signFalse = bob)
    val create = beta.submitAndWaitRequest(bob, template.create.command)
    for {
      transaction <- beta.submitAndWaitForTransaction(create)
      _ <- synchronize(alpha, beta)
      aliceTransactions <- alpha.flatTransactions(alice)
    } yield {
      val branchingContractId = createdEvents(transaction)
        .map(_.contractId)
        .headOption
        .getOrElse(fail(s"Expected single create event"))
      val contractsVisibleByAlice = aliceTransactions.flatMap(createdEvents).map(_.contractId)
      assert(
        !contractsVisibleByAlice.contains(branchingContractId),
        s"The transaction ${transaction.transactionId} should not have been disclosed.",
      )
    }
  })

  test(
    "TXDiscloseCreateToSignatory",
    "Disclose create to the chosen branching controller",
    allocate(SingleParty, TwoParties),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob, eve)) =>
    val template =
      BranchingControllers(giver = alice, whichCtrl = true, ctrlTrue = bob, ctrlFalse = eve)
    for {
      _ <- alpha.create(alice, template)
      _ <- eventually {
        for {
          aliceView <- alpha.flatTransactions(alice)
          bobView <- beta.flatTransactions(bob)
          evesView <- beta.flatTransactions(eve)
        } yield {
          val aliceCreate =
            assertSingleton("Alice should see one transaction", aliceView.flatMap(createdEvents))
          assertEquals(
            "Alice arguments do not match",
            aliceCreate.getCreateArguments,
            template.arguments,
          )
          val bobCreate =
            assertSingleton("Bob should see one transaction", bobView.flatMap(createdEvents))
          assertEquals(
            "Bob arguments do not match",
            bobCreate.getCreateArguments,
            template.arguments,
          )
          assert(evesView.isEmpty, "Eve should not see any contract")
        }
      }
    } yield {
      // Checks performed in the `eventually` block
    }
  })

  test(
    "TXNotDiscloseCreateToNonChosenBranchingController",
    "Not disclose create to non-chosen branching controller",
    allocate(SingleParty, TwoParties),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob, eve)) =>
    val template =
      BranchingControllers(giver = alice, whichCtrl = false, ctrlTrue = bob, ctrlFalse = eve)
    val create = alpha.submitAndWaitRequest(alice, template.create.command)
    for {
      transaction <- alpha.submitAndWaitForTransaction(create)
      _ <- synchronize(alpha, beta)
      transactions <- beta.flatTransactions(bob)
    } yield {
      assert(
        !transactions.exists(_.transactionId != transaction.transactionId),
        s"The transaction ${transaction.transactionId} should not have been disclosed.",
      )
    }
  })

  test(
    "TXDiscloseCreateToObservers",
    "Disclose create to observers",
    allocate(SingleParty, TwoParties),
  )(implicit ec => {
    case Participants(Participant(alpha, alice), Participant(beta, observers @ _*)) =>
      val template = WithObservers(alice, Primitive.List(observers: _*))
      val create = alpha.submitAndWaitRequest(alice, template.create.command)
      for {
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
  })

  test(
    "TXUnitAsArgumentToNothing",
    "Daml engine returns Unit as argument to Nothing",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val template = NothingArgument(party, Primitive.Optional.empty)
    val create = ledger.submitAndWaitRequest(party, template.create.command)
    for {
      transaction <- ledger.submitAndWaitForTransaction(create)
    } yield {
      val contract = assertSingleton("UnitAsArgumentToNothing", createdEvents(transaction))
      assertEquals("UnitAsArgumentToNothing", contract.getCreateArguments, template.arguments)
    }
  })

  test(
    "TXAgreementTextExplicit",
    "Expose the agreement text for templates with an explicit agreement text",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, Dummy(party))
      transactions <- ledger.flatTransactionsByTemplateId(Dummy.id, party)
    } yield {
      val contract = assertSingleton("AgreementText", transactions.flatMap(createdEvents))
      assertEquals("AgreementText", contract.getAgreementText, s"'$party' operates a dummy.")
    }
  })

  test(
    "TXAgreementTextDefault",
    "Expose the default text for templates without an agreement text",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      _ <- ledger.create(party, DummyWithParam(party))
      transactions <- ledger.flatTransactions(party)
    } yield {
      val contract = assertSingleton("AgreementTextDefault", transactions.flatMap(createdEvents))
      assertEquals("AgreementTextDefault", contract.getAgreementText, "")
    }
  })

  test("TXStakeholders", "Expose the correct stakeholders", allocate(SingleParty, SingleParty))(
    implicit ec => {
      case Participants(Participant(alpha @ _, receiver), Participant(beta, giver)) =>
        for {
          _ <- beta.create(giver, CallablePayout(giver, receiver))
          transactions <- beta.flatTransactions(giver, receiver)
        } yield {
          val contract = assertSingleton("Stakeholders", transactions.flatMap(createdEvents))
          assertEquals("Signatories", contract.signatories, Seq(Tag.unwrap(giver)))
          assertEquals("Observers", contract.observers, Seq(Tag.unwrap(receiver)))
        }
    }
  )

  test(
    "TXNoContractKey",
    "There should be no contract key if the template does not specify one",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha @ _, receiver), Participant(beta, giver)) =>
      for {
        _ <- beta.create(giver, CallablePayout(giver, receiver))
        transactions <- beta.flatTransactions(giver, receiver)
      } yield {
        val contract = assertSingleton("NoContractKey", transactions.flatMap(createdEvents))
        assert(
          contract.getContractKey.sum.isEmpty,
          s"The key is not empty: ${contract.getContractKey}",
        )
      }
  })

  test(
    "TXMultiActorChoiceOkBasic",
    "Accept exercising a well-authorized multi-actor choice",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, operator, receiver), Participant(beta, giver)) =>
      for {
        agreementFactory <- beta.create(giver, AgreementFactory(receiver, giver))
        agreement <- eventually {
          alpha.exerciseAndGetContract(receiver, agreementFactory.exerciseAgreementFactoryAccept)
        }
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
          triProposalTemplate.arguments.fields,
        )
      }
  })

  test(
    "TXMultiActorChoiceOkCoincidingControllers",
    "Accept exercising a well-authorized multi-actor choice with coinciding controllers",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, operator), Participant(beta, giver)) =>
    for {
      agreementFactory <- beta.create(giver, AgreementFactory(giver, giver))
      agreement <-
        beta.exerciseAndGetContract(giver, agreementFactory.exerciseAgreementFactoryAccept)
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
        triProposalTemplate.arguments.fields,
      )
    }
  })

  test(
    "TXRejectMultiActorMissingAuth",
    "Reject exercising a multi-actor choice with missing authorizers",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, operator, receiver), Participant(beta, giver)) =>
      for {
        triProposal <- alpha.create(operator, TriProposal(operator, receiver, giver))
        _ <- eventually {
          for {
            failure <- beta
              .exercise(giver, triProposal.exerciseTriProposalAccept)
              .mustFail("exercising with missing authorizers")
          } yield {
            assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "requires authorizers")
          }
        }
      } yield {
        // Check performed in the `eventually` block
      }
  })

  // This is the current, most conservative semantics of multi-actor choice authorization.
  // It is likely that this will change in the future. Should we delete this test, we should
  // also remove the 'UnrestrictedAcceptTriProposal' choice from the 'Agreement' template.
  test(
    "TXRejectMultiActorExcessiveAuth",
    "Reject exercising a multi-actor choice with too many authorizers",
    allocate(TwoParties, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, operator, receiver), Participant(beta, giver)) =>
      for {
        agreementFactory <- beta.create(giver, AgreementFactory(receiver, giver))
        // TODO eventually is a temporary workaround. It should take into account
        // TODO that the contract needs to hit the target node before a choice
        // TODO is executed on it.
        agreement <- eventually {
          alpha.exerciseAndGetContract(receiver, agreementFactory.exerciseAgreementFactoryAccept)
        }
        triProposalTemplate = TriProposal(operator, giver, giver)
        triProposal <- alpha.create(operator, triProposalTemplate)
        _ <- eventually {
          for {
            failure <- beta
              .exercise(giver, agreement.exerciseAcceptTriProposal(_, triProposal))
              .mustFail("exercising with failing assertion")
          } yield {
            assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Assertion failed")
          }
        }
      } yield {
        // Check performed in the `eventually` block
      }
  })

  test("TXNoReorder", "Don't reorder fields in data structures of choices", allocate(SingleParty))(
    implicit ec => { case Participants(Participant(ledger, party)) =>
      for {
        dummy <- ledger.create(party, Dummy(party))
        tree <- ledger.exercise(
          party,
          dummy.exerciseWrapWithAddress(_, Address("street", "city", "state", "zip")),
        )
      } yield {
        val contract = assertSingleton("Contract in transaction", createdEvents(tree))
        val fields = assertLength("Fields in contract", 2, contract.getCreateArguments.fields)
        assertEquals(
          "NoReorder",
          fields.flatMap(_.getValue.getRecord.fields).map(_.getValue.getText).zipWithIndex,
          Seq("street" -> 0, "city" -> 1, "state" -> 2, "zip" -> 3),
        )
      }
    }
  )

  test(
    "TXSingleMultiSameBasic",
    "The same transaction should be served regardless of subscribing as one or multiple parties",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      _ <- ledger.create(alice, Dummy(alice))
      _ <- ledger.create(bob, Dummy(bob))
      aliceView <- ledger.flatTransactions(alice)
      bobView <- ledger.flatTransactions(bob)
      multiSubscriptionView <- ledger.flatTransactions(alice, bob)
    } yield {
      val jointView = aliceView ++ bobView
      assertEquals(
        "Single- and multi-party subscription yield different results",
        jointView,
        multiSubscriptionView,
      )
    }
  })

  test(
    "TXSingleMultiSameTreesBasic",
    "The same transaction trees should be served regardless of subscribing as one or multiple parties",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      _ <- ledger.create(alice, Dummy(alice))
      _ <- ledger.create(bob, Dummy(bob))
      aliceView <- ledger.transactionTrees(alice)
      bobView <- ledger.transactionTrees(bob)
      multiSubscriptionView <- ledger.transactionTrees(alice, bob)
    } yield {
      val jointView = aliceView ++ bobView
      assertEquals(
        "Single- and multi-party subscription yield different results",
        jointView,
        multiSubscriptionView,
      )
    }
  })

  test(
    "TXSingleMultiSameStakeholders",
    "The same transaction should be served to all stakeholders",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      _ <- alpha.create(alice, AgreementFactory(bob, alice))
      _ <- beta.create(bob, AgreementFactory(alice, bob))
      _ <- synchronize(alpha, beta)
      alphaView <- alpha.flatTransactions(alice, bob)
      betaView <- beta.flatTransactions(alice, bob)
    } yield {
      assertEquals(
        "Single- and multi-party subscription yield different results",
        comparableTransactions(alphaView),
        comparableTransactions(betaView),
      )
    }
  })

  test(
    "TXSingleMultiSameTreesStakeholders",
    "The same transaction trees should be served to all stakeholders",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      _ <- alpha.create(alice, AgreementFactory(bob, alice))
      _ <- beta.create(bob, AgreementFactory(alice, bob))
      _ <- synchronize(alpha, beta)
      alphaView <- alpha.transactionTrees(alice, bob)
      betaView <- beta.transactionTrees(alice, bob)
    } yield {
      assertEquals(
        "Single- and multi-party subscription yield different results",
        comparableTransactionTrees(alphaView),
        comparableTransactionTrees(betaView),
      )
    }
  })

  test(
    "TXFetchContractCreatedInTransaction",
    "It should be possible to fetch a contract created within a transaction",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      createAndFetch <- ledger.create(party, CreateAndFetch(party))
      transaction <- ledger.exerciseForFlatTransaction(
        party,
        createAndFetch.exerciseCreateAndFetch_Run,
      )
    } yield {
      val _ = assertSingleton("There should be only one create", createdEvents(transaction))
      val exercise =
        assertSingleton("There should be only one archive", archivedEvents(transaction))
      assertEquals(
        "The contract identifier of the exercise does not match",
        Tag.unwrap(createAndFetch),
        exercise.contractId,
      )
    }
  })

  test(
    "TXnoSignatoryObservers",
    "transactions' created events should not return overlapping signatories and observers",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      _ <- ledger.create(alice, WithObservers(alice, Seq(alice, bob)))
      flat <- ledger.flatTransactions(alice)
      Seq(flatTx) = flat
      Seq(flatWo) = createdEvents(flatTx)
      tree <- ledger.transactionTrees(alice)
      Seq(treeTx) = tree
      Seq(treeWo) = createdEvents(treeTx)
    } yield {
      assert(
        flatWo.observers == Seq(bob),
        s"Expected observers to only contain $bob, but received ${flatWo.observers}",
      )
      assert(
        treeWo.observers == Seq(bob),
        s"Expected observers to only contain $bob, but received ${treeWo.observers}",
      )
    }
  })

  test(
    "TXFlatTransactionsWrongLedgerId",
    "The getTransactions endpoint should reject calls with the wrong ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    val invalidRequest = ledger
      .getTransactionsRequest(Seq(party))
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .flatTransactions(invalidRequest)
        .mustFail("subscribing with the wrong ledger ID")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
    }
  })

  test(
    "TXTransactionTreesWrongLedgerId",
    "The getTransactionTrees endpoint should reject calls with the wrong ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    val invalidRequest = ledger
      .getTransactionsRequest(Seq(party))
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .transactionTrees(invalidRequest)
        .mustFail("subscribing with the wrong ledger ID")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
    }
  })

  test(
    "TXTransactionTreeByIdWrongLedgerId",
    "The getTransactionTreeById endpoint should reject calls with the wrong ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    val invalidRequest = ledger
      .getTransactionByIdRequest("not-relevant", Seq(party))
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .transactionTreeById(invalidRequest)
        .mustFail("subscribing with the wrong ledger ID")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
    }
  })

  test(
    "TXFlatTransactionByIdWrongLedgerId",
    "The getFlatTransactionById endpoint should reject calls with the wrong ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    val invalidRequest = ledger
      .getTransactionByIdRequest("not-relevant", Seq(party))
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .flatTransactionById(invalidRequest)
        .mustFail("subscribing with the wrong ledger ID")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
    }
  })

  test(
    "TXTransactionTreeByEventIdWrongLedgerId",
    "The getTransactionTreeByEventId endpoint should reject calls with the wrong ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    val invalidRequest = ledger
      .getTransactionByEventIdRequest("not-relevant", Seq(party))
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .transactionTreeByEventId(invalidRequest)
        .mustFail("subscribing with the wrong ledger ID")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
    }
  })

  test(
    "TXFlatTransactionByEventIdWrongLedgerId",
    "The getFlatTransactionByEventId endpoint should reject calls with the wrong ledger identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    val invalidRequest = ledger
      .getTransactionByEventIdRequest("not-relevant", Seq(party))
      .update(_.ledgerId := invalidLedgerId)
    for {
      failure <- ledger
        .flatTransactionByEventId(invalidRequest)
        .mustFail("subscribing with the wrong ledger ID")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
    }
  })

  test(
    "TXLedgerEndWrongLedgerId",
    "The ledgerEnd endpoint should reject calls with the wrong ledger identifier",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val invalidLedgerId = "DEFINITELY_NOT_A_VALID_LEDGER_IDENTIFIER"
    for {
      failure <- ledger.currentEnd(invalidLedgerId).mustFail("requesting with the wrong ledger ID")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
    }
  })

  test(
    "TXTransactionTreeByIdBasic",
    "Expose a visible transaction tree by identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1)
      byId <- ledger.transactionTreeById(tree.transactionId, party)
    } yield {
      assertEquals("The transaction fetched by identifier does not match", tree, byId)
    }
  })

  test(
    "TXInvisibleTransactionTreeById",
    "Do not expose an invisible transaction tree by identifier",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, party), Participant(beta, intruder)) =>
    for {
      dummy <- alpha.create(party, Dummy(party))
      tree <- alpha.exercise(party, dummy.exerciseDummyChoice1)
      _ <- synchronize(alpha, beta)
      failure <- beta
        .transactionTreeById(tree.transactionId, intruder)
        .mustFail("subscribing to an invisible transaction tree")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
    }
  })

  test(
    "TXTransactionTreeByIdNotFound",
    "Return NOT_FOUND when looking up a non-existent transaction tree by identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      failure <- ledger
        .transactionTreeById("a" * 60, party)
        .mustFail("looking up an non-existent transaction tree")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
    }
  })

  test(
    "TXTransactionTreeByIdWithoutParty",
    "Return INVALID_ARGUMENT when looking up a transaction tree by identifier without specifying a party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      failure <- ledger
        .transactionTreeById("not-relevant")
        .mustFail("looking up a transaction tree without specifying a party")
    } yield {
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Missing field: requesting_parties")
    }
  })

  test(
    "TXTransactionTreeByIdSameAsTransactionStream",
    "Expose the same events for each transaction as the output of getTransactionTrees",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, submitter), Participant(beta, listener)) =>
      for {
        _ <- alpha.create(submitter, AgreementFactory(listener, submitter))
        _ <- synchronize(alpha, beta)
        trees <- alpha.transactionTrees(listener, submitter)
        byId <- Future.sequence(
          trees.map(t => beta.transactionTreeById(t.transactionId, listener, submitter))
        )
      } yield {
        assertEquals(
          "The events fetched by identifier did not match the ones on the transaction stream",
          comparableTransactionTrees(trees),
          comparableTransactionTrees(byId),
        )
      }
  })

  test(
    "TXFlatTransactionByIdBasic",
    "Expose a visible transaction by identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      transaction <- ledger.exerciseForFlatTransaction(party, dummy.exerciseDummyChoice1)
      byId <- ledger.flatTransactionById(transaction.transactionId, party)
    } yield {
      assertEquals("The transaction fetched by identifier does not match", transaction, byId)
    }
  })

  test(
    "TXInvisibleFlatTransactionById",
    "Do not expose an invisible flat transaction by identifier",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, party, intruder)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1)
      failure <- ledger
        .flatTransactionById(tree.transactionId, intruder)
        .mustFail("looking up an invisible flat transaction")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
    }
  })

  test(
    "TXFlatTransactionByIdNotFound",
    "Return NOT_FOUND when looking up a non-existent flat transaction by identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      failure <- ledger
        .flatTransactionById("a" * 60, party)
        .mustFail("looking up a non-existent flat transaction")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
    }
  })

  test(
    "TXFlatTransactionByIdWithoutParty",
    "Return INVALID_ARGUMENT when looking up a flat transaction by identifier without specifying a party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      failure <- ledger
        .flatTransactionById("not-relevant")
        .mustFail("looking up a flat transaction without specifying a party")
    } yield {
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Missing field: requesting_parties")
    }
  })

  test(
    "TXFlatTransactionByIdSameAsTransactionStream",
    "Expose the same events for each transaction as the output of getTransactions",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case Participants(Participant(alpha, submitter), Participant(beta, listener)) =>
      for {
        _ <- alpha.create(submitter, AgreementFactory(listener, submitter))
        _ <- synchronize(alpha, beta)
        transactions <- alpha.flatTransactions(listener, submitter)
        byId <- Future.sequence(
          transactions.map(t => beta.flatTransactionById(t.transactionId, listener, submitter))
        )
      } yield {
        assertEquals(
          "The events fetched by identifier did not match the ones on the transaction stream",
          comparableTransactions(transactions),
          comparableTransactions(byId),
        )
      }
  })

  test(
    "TXTransactionTreeByEventIdBasic",
    "Expose a visible transaction tree by event identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1)
      byId <- ledger.transactionTreeByEventId(tree.rootEventIds.head, party)
    } yield {
      assertEquals("The transaction fetched by identifier does not match", tree, byId)
    }
  })

  test(
    "TXInvisibleTransactionTreeByEventId",
    "Do not expose an invisible transaction tree by event identifier",
    allocate(SingleParty, SingleParty),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(alpha, party), Participant(beta, intruder)) =>
    for {
      dummy <- alpha.create(party, Dummy(party))
      tree <- alpha.exercise(party, dummy.exerciseDummyChoice1)
      _ <- synchronize(alpha, beta)
      failure <- beta
        .transactionTreeByEventId(tree.rootEventIds.head, intruder)
        .mustFail("looking up an invisible transaction tree")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
    }
  })

  test(
    "TXTransactionTreeByEventIdInvalid",
    "Return INVALID when looking up a transaction tree using an invalid event identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      failure <- ledger
        .transactionTreeByEventId("dont' worry, be happy", party)
        .mustFail("looking up an transaction tree using an invalid event ID")
    } yield {
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Invalid field event_id")
    }
  })

  test(
    "TXTransactionTreeByEventIdNotFound",
    "Return NOT_FOUND when looking up a non-existent transaction tree by event identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      failure <- ledger
        .transactionTreeByEventId(s"#${"a" * 60}:000", party)
        .mustFail("looking up a non-existent transaction tree")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
    }
  })

  test(
    "TXTransactionTreeByEventIdWithoutParty",
    "Return INVALID_ARGUMENT when looking up a transaction tree by event identifier without specifying a party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      failure <- ledger
        .transactionTreeByEventId("not-relevant")
        .mustFail("looking up a transaction tree without specifying a party")
    } yield {
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Missing field: requesting_parties")
    }
  })

  test(
    "TXFlatTransactionByEventIdBasic",
    "Expose a visible flat transaction by event identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      transaction <- ledger.exerciseForFlatTransaction(party, dummy.exerciseDummyChoice1)
      event = transaction.events.head.event
      eventId = event.archived.map(_.eventId).get
      byId <- ledger.flatTransactionByEventId(eventId, party)
    } yield {
      assertEquals("The transaction fetched by identifier does not match", transaction, byId)
    }
  })

  test(
    "TXInvisibleFlatTransactionByEventId",
    "Do not expose an invisible flat transaction by event identifier",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, party, intruder)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1)
      failure <- ledger
        .flatTransactionByEventId(tree.rootEventIds.head, intruder)
        .mustFail("looking up an invisible flat transaction")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
    }
  })

  test(
    "TXFlatTransactionByEventIdInvalid",
    "Return INVALID when looking up a flat transaction using an invalid event identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      failure <- ledger
        .flatTransactionByEventId("dont' worry, be happy", party)
        .mustFail("looking up a flat transaction using an invalid event ID")
    } yield {
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Invalid field event_id")
    }
  })

  test(
    "TXFlatTransactionByEventIdNotFound",
    "Return NOT_FOUND when looking up a non-existent flat transaction by event identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      failure <- ledger
        .flatTransactionByEventId(s"#${"a" * 60}:000", party)
        .mustFail("looking up a non-existent flat transaction")
    } yield {
      assertGrpcError(failure, Status.Code.NOT_FOUND, "Transaction not found, or not visible.")
    }
  })

  test(
    "TXFlatTransactionByEventIdWithoutParty",
    "Return INVALID_ARGUMENT when looking up a flat transaction by event identifier without specifying a party",
    allocate(NoParties),
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      failure <- ledger
        .flatTransactionByEventId("not-relevant")
        .mustFail("looking up a flat transaction without specifying a party")
    } yield {
      assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "Missing field: requesting_parties")
    }
  })

  private def checkTransactionsOrder(
      context: String,
      transactions: Vector[Transaction],
      contracts: Int,
  ): Unit = {
    val (cs, as) =
      transactions.flatMap(_.events).zipWithIndex.partition { case (e, _) =>
        e.event.isCreated
      }
    val creations = cs.map { case (e, i) => e.getCreated.contractId -> i }
    val archivals = as.map { case (e, i) => e.getArchived.contractId -> i }
    assert(
      creations.size == contracts && archivals.size == contracts,
      s"$context: either the number of archive events (${archivals.size}) or the number of create events (${creations.size}) doesn't match the expected number of $contracts.",
    )
    val createdContracts = creations.iterator.map(_._1).toSet
    val archivedContracts = archivals.iterator.map(_._1).toSet
    assert(
      createdContracts.size == creations.size,
      s"$context: there are duplicate contract identifiers in the create events",
    )
    assert(
      archivedContracts.size == archivals.size,
      s"$context: there are duplicate contract identifiers in the archive events",
    )
    assert(
      createdContracts == archivedContracts,
      s"$context: the contract identifiers for created and archived contracts differ: ${createdContracts
        .diff(archivedContracts)}",
    )
    val sortedCreations = creations.sortBy(_._1)
    val sortedArchivals = archivals.sortBy(_._1)
    for (i <- 0 until contracts) {
      val (createdContract, creationIndex) = sortedCreations(i)
      val (archivedContract, archivalIndex) = sortedArchivals(i)
      assert(
        createdContract == archivedContract,
        s"$context: unexpected discrepancy between the created and archived events",
      )
      assert(
        creationIndex < archivalIndex,
        s"$context: the creation of $createdContract did not appear in the stream before it's archival",
      )
    }
  }

  test(
    "TXSingleSubscriptionInOrder",
    "Archives should always come after creations when subscribing as a single party",
    allocate(SingleParty),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val contracts = 50
    for {
      _ <- Future.sequence(
        Vector.fill(contracts)(
          ledger
            .create(party, Dummy(party))
            .flatMap(contract => ledger.exercise(party, contract.exerciseDummyChoice1))
        )
      )
      transactions <- ledger.flatTransactions(party)
    } yield {
      checkTransactionsOrder("Ledger", transactions, contracts)
    }
  })

  test(
    "TXMultiSubscriptionInOrder",
    "Archives should always come after creations when subscribing as more than on party",
    allocate(TwoParties),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    val contracts = 50
    for {
      _ <- Future.sequence(Vector.tabulate(contracts) { n =>
        val party = if (n % 2 == 0) alice else bob
        ledger
          .create(party, Dummy(party))
          .flatMap(contract => ledger.exercise(party, contract.exerciseDummyChoice1))
      })
      transactions <- ledger.flatTransactions(alice, bob)
    } yield {
      checkTransactionsOrder("Ledger", transactions, contracts)
    }
  })

  test(
    "TXFlatSubsetOfTrees",
    "The event identifiers in the flat stream should be a subset of those in the trees stream",
    allocate(SingleParty),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val contracts = 50
    for {
      _ <- Future.sequence(
        Vector.fill(contracts)(
          ledger
            .create(party, Dummy(party))
            .flatMap(contract => ledger.exercise(party, contract.exerciseDummyChoice1))
        )
      )
      transactions <- ledger.flatTransactions(party)
      trees <- ledger.transactionTrees(party)
    } yield {
      assert(
        transactions
          .flatMap(
            _.events.map(e =>
              e.event.archived.map(_.eventId).orElse(e.event.created.map(_.eventId)).get
            )
          )
          .toSet
          .subsetOf(trees.flatMap(_.eventsById.keys).toSet)
      )
    }
  })

  test(
    "TXFlatWitnessesSubsetOfTrees",
    "The witnesses in the flat stream should be a subset of those in the trees stream",
    allocate(SingleParty),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val contracts = 50
    for {
      _ <- Future.sequence(
        Vector.fill(contracts)(
          ledger
            .create(party, Dummy(party))
            .flatMap(contract => ledger.exercise(party, contract.exerciseDummyChoice1))
        )
      )
      transactions <- ledger.flatTransactions(party)
      trees <- ledger.transactionTrees(party)
    } yield {
      val witnessesByEventIdInTreesStream =
        trees.iterator
          .flatMap(_.eventsById)
          .map { case (id, event) =>
            id -> event.kind.exercised
              .map(_.witnessParties.toSet)
              .orElse(event.kind.created.map(_.witnessParties.toSet))
              .get
          }
          .toMap
      val witnessesByEventIdInFlatStream =
        transactions
          .flatMap(
            _.events.map(e =>
              e.event.archived
                .map(a => a.eventId -> a.witnessParties.toSet)
                .orElse(e.event.created.map(c => c.eventId -> c.witnessParties.toSet))
                .get
            )
          )
      for ((event, witnesses) <- witnessesByEventIdInFlatStream) {
        assert(witnesses.subsetOf(witnessesByEventIdInTreesStream(event)))
      }
    }
  })

  test(
    "TXFlatTransactionsVisibility",
    "Transactions in the flat transactions stream should be disclosed only to the stakeholders",
    allocate(Parties(3)),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, bank, alice, bob)) =>
    for {
      gbpIouIssue <- ledger.create(bank, Iou(bank, bank, "GBP", 100, Nil))
      gbpTransfer <- ledger.exerciseAndGetContract(bank, gbpIouIssue.exerciseIou_Transfer(_, alice))
      dkkIouIssue <- ledger.create(bank, Iou(bank, bank, "DKK", 110, Nil))
      dkkTransfer <- ledger.exerciseAndGetContract(bank, dkkIouIssue.exerciseIou_Transfer(_, bob))

      aliceIou1 <- eventually {
        ledger.exerciseAndGetContract(alice, gbpTransfer.exerciseIouTransfer_Accept(_))
      }
      aliceIou <- eventually {
        ledger.exerciseAndGetContract(alice, aliceIou1.exerciseIou_AddObserver(_, bob))
      }
      bobIou <- eventually {
        ledger.exerciseAndGetContract(bob, dkkTransfer.exerciseIouTransfer_Accept(_))
      }
      trade <- eventually {
        ledger.create(
          alice,
          IouTrade(alice, bob, aliceIou, bank, "GBP", 100, bank, "DKK", 110),
        )
      }
      tree <- eventually { ledger.exercise(bob, trade.exerciseIouTrade_Accept(_, bobIou)) }
      aliceTransactions <- ledger.flatTransactions(alice)
      bobTransactions <- ledger.flatTransactions(bob)
    } yield {
      val newIouList = createdEvents(tree)
        .filter(event => event.templateId.exists(_.entityName == "Iou"))

      assert(
        newIouList.length == 2,
        s"Expected 2 new IOUs created, found: ${newIouList.length}",
      )

      val newAliceIou = newIouList
        .find(iou => iou.signatories.contains(alice) && iou.signatories.contains(bank))
        .map(_.contractId)
        .getOrElse {
          fail(s"Not found an IOU owned by $alice")
        }

      val newBobIou = newIouList
        .find(iou => iou.signatories.contains(bob) && iou.signatories.contains(bank))
        .map(_.contractId)
        .getOrElse {
          fail(s"Not found an IOU owned by $bob")
        }

      assert(
        aliceTransactions.flatMap(createdEvents).map(_.contractId).contains(newAliceIou),
        "Alice's flat transaction stream does not contain the new IOU",
      )
      assert(
        !aliceTransactions.flatMap(createdEvents).map(_.contractId).contains(newBobIou),
        "Alice's flat transaction stream contains Bob's new IOU",
      )
      assert(
        bobTransactions.flatMap(createdEvents).map(_.contractId).contains(newBobIou),
        "Bob's flat transaction stream does not contain the new IOU",
      )
      assert(
        !bobTransactions.flatMap(createdEvents).map(_.contractId).contains(newAliceIou),
        "Bob's flat transaction stream contains Alice's new IOU",
      )
    }
  })

  test(
    "TXRequestingPartiesWitnessVisibility",
    "Transactions in the flat transactions stream should not leak witnesses",
    allocate(Parties(3)),
  )(implicit ec => { case Participants(Participant(ledger, bank, alice, bob)) =>
    for {
      iouIssue <- ledger.create(bank, Iou(bank, bank, "GBP", 100, Nil))
      transfer <- ledger.exerciseAndGetContract(bank, iouIssue.exerciseIou_Transfer(_, alice))

      aliceIou <- eventually {
        ledger.exerciseAndGetContract(alice, transfer.exerciseIouTransfer_Accept(_))
      }
      _ <- eventually {
        ledger.exerciseAndGetContract(alice, aliceIou.exerciseIou_AddObserver(_, bob))
      }

      aliceFlatTransactions <- ledger.flatTransactions(alice)
      bobFlatTransactions <- ledger.flatTransactions(bob)
      aliceBankFlatTransactions <- ledger.flatTransactions(alice, bank)
    } yield {
      onlyRequestingPartiesAsWitnesses(allTxWitnesses(aliceFlatTransactions), alice)(
        "Alice's flat transactions contain other parties as witnesses"
      )

      onlyRequestingPartiesAsWitnesses(allTxWitnesses(bobFlatTransactions), bob)(
        "Bob's flat transactions contain other parties as witnesses"
      )

      onlyRequestingPartiesAsWitnesses(allTxWitnesses(aliceBankFlatTransactions), alice, bank)(
        "Alice's and Bank's flat transactions contain other parties as witnesses"
      )
    }
  })

  test(
    "TXTreesRequestingPartiesWitnessVisibility",
    "Transactions in the transaction trees stream should not leak witnesses",
    allocate(Parties(3)),
  )(implicit ec => { case Participants(Participant(ledger, bank, alice, bob)) =>
    for {
      iouIssue <- ledger.create(bank, Iou(bank, bank, "GBP", 100, Nil))
      transfer <- ledger.exerciseAndGetContract(bank, iouIssue.exerciseIou_Transfer(_, alice))

      aliceIou <- eventually {
        ledger.exerciseAndGetContract(alice, transfer.exerciseIouTransfer_Accept(_))
      }
      _ <- eventually {
        ledger.exerciseAndGetContract(alice, aliceIou.exerciseIou_AddObserver(_, bob))
      }

      aliceTransactionTrees <- ledger.transactionTrees(alice)
      bobTransactionTrees <- ledger.transactionTrees(bob)
      aliceBankTransactionTrees <- ledger.transactionTrees(alice, bank)
    } yield {
      onlyRequestingPartiesAsWitnesses(allTxTreesWitnesses(aliceTransactionTrees), alice)(
        "Alice's transaction trees contain other parties as witnesses"
      )

      onlyRequestingPartiesAsWitnesses(allTxTreesWitnesses(bobTransactionTrees), bob)(
        "Bob's transaction trees contain other parties as witnesses"
      )

      onlyRequestingPartiesAsWitnesses(allTxTreesWitnesses(aliceBankTransactionTrees), alice, bank)(
        "Alice's and Bank's transaction trees contain other parties as witnesses"
      )
    }
  })

  private def onlyRequestingPartiesAsWitnesses(
      allWitnesses: Set[String],
      requestingParties: binding.Primitive.Party*
  )(msg: String): Unit = {
    val nonRequestingWitnesses = allWitnesses.diff(requestingParties.map(_.toString).toSet)
    assert(
      nonRequestingWitnesses.isEmpty,
      s"$msg: ${nonRequestingWitnesses.mkString("[", ",", "]")}",
    )
  }

  private def allTxWitnesses(transactions: Vector[Transaction]): Set[String] =
    transactions
      .flatMap(_.events.map(_.event).flatMap {
        case Event.Empty => Seq.empty
        case Event.Created(createdEvent) => createdEvent.witnessParties
        case Event.Archived(archivedEvent) => archivedEvent.witnessParties
      })
      .toSet

  private def allTxTreesWitnesses(transactionTrees: Vector[TransactionTree]): Set[String] =
    transactionTrees
      .flatMap(_.eventsById.valuesIterator.flatMap {
        case event if event.kind.isCreated => event.getCreated.witnessParties
        case event if event.kind.isExercised => event.getExercised.witnessParties
        case _ => Seq.empty
      })
      .toSet
}

object TransactionServiceIT {

  // Strip command id and offset to yield a transaction comparable across participant
  // Furthermore, makes sure that the order is not relevant for witness parties
  // Sort by transactionId as on distributed ledgers updates can occur in different orders
  private def comparableTransactions(transactions: Vector[Transaction]): Vector[Transaction] =
    transactions
      .map(t =>
        t.copy(
          commandId = "commandId",
          offset = "offset",
          events = t.events.map(_.modifyWitnessParties(_.sorted)),
        )
      )
      .sortBy(_.transactionId)

  // Strip command id and offset to yield a transaction comparable across participant
  // Furthermore, makes sure that the order is not relevant for witness parties
  // Sort by transactionId as on distributed ledgers updates can occur in different orders
  private def comparableTransactionTrees(
      transactionTrees: Vector[TransactionTree]
  ): Vector[TransactionTree] =
    transactionTrees
      .map(t =>
        t.copy(
          commandId = "commandId",
          offset = "offset",
          eventsById = t.eventsById.view.mapValues(_.modifyWitnessParties(_.sorted)).toMap,
        )
      )
      .sortBy(_.transactionId)

}
