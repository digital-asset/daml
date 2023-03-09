// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.Eventually
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.testtool.suites.v1_8.TransactionServiceVisibilityIT._
import com.daml.ledger.api.v1.event.Event.Event
import com.daml.ledger.api.v1.transaction.TreeEvent.Kind.Exercised
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree, TreeEvent}
import com.daml.ledger.client.binding
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Iou.Iou
import com.daml.ledger.test.model.Iou.Iou._
import com.daml.ledger.test.model.Iou.IouTransfer._
import com.daml.ledger.test.model.IouTrade.IouTrade
import com.daml.ledger.test.model.IouTrade.IouTrade._
import com.daml.ledger.test.model.Test._
import com.daml.lf.ledger.EventId
import com.daml.test.evidence.tag.EvidenceTag
import com.daml.test.evidence.tag.Security.SecurityTest
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Privacy

import scala.annotation.nowarn
import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class TransactionServiceVisibilityIT extends LedgerTestSuite {

  // quadruple the eventually wait duration compared to default to avoid database timeouts
  // when running against Oracle in enterprise mode
  def eventually[A](
      assertionName: String
  )(runAssertion: => Future[A])(implicit ec: ExecutionContext): Future[A] =
    Eventually.eventually(
      assertionName,
      attempts =
        12, // compared to the default of 10; 4x comes from exponential 2x backoff on each attempt
    )(runAssertion)

  def privacyHappyCase(asset: String, happyCase: String)(implicit
      lineNo: sourcecode.Line,
      fileName: sourcecode.File,
  ): List[EvidenceTag] =
    List(SecurityTest(property = Privacy, asset = asset, happyCase))

  test(
    "TXTreeBlinding",
    "Trees should be served according to the blinding/projection rules",
    allocate(TwoParties, SingleParty, SingleParty),
    enabled = _.committerEventLog.eventLogType.isCentralized,
    tags = privacyHappyCase(
      asset = "Transaction Tree",
      happyCase = "Transaction trees are served according to the blinding/projection rules",
    ),
  )(implicit ec => {
    case Participants(
          Participant(alpha, alice, gbp_bank),
          Participant(beta, bob),
          Participant(delta, dkk_bank),
        ) =>
      for {
        gbpIouIssue <- alpha.create(gbp_bank, Iou(gbp_bank, gbp_bank, "GBP", 100, Nil))
        gbpTransfer <-
          alpha.exerciseAndGetContract(gbp_bank, gbpIouIssue.exerciseIou_Transfer(alice))
        dkkIouIssue <- delta.create(dkk_bank, Iou(dkk_bank, dkk_bank, "DKK", 110, Nil))
        dkkTransfer <-
          delta.exerciseAndGetContract(dkk_bank, dkkIouIssue.exerciseIou_Transfer(bob))

        aliceIou1 <- eventually("exerciseIouTransfer_Accept") {
          alpha.exerciseAndGetContract(alice, gbpTransfer.exerciseIouTransfer_Accept())
        }
        aliceIou <- eventually("exerciseIou_AddObserver") {
          alpha.exerciseAndGetContract(alice, aliceIou1.exerciseIou_AddObserver(bob))
        }
        bobIou <- eventually("exerciseIouTransfer_Accept") {
          beta.exerciseAndGetContract(bob, dkkTransfer.exerciseIouTransfer_Accept())
        }

        trade <- eventually("create") {
          alpha.create(
            alice,
            IouTrade(alice, bob, aliceIou, gbp_bank, "GBP", 100, dkk_bank, "DKK", 110),
          )
        }
        tree <- eventually("exerciseIouTrade_Accept") {
          beta.exercise(bob, trade.exerciseIouTrade_Accept(bobIou))
        }

        aliceTree <- eventually("transactionTreeById1") {
          alpha.transactionTreeById(tree.transactionId, alice)
        }
        bobTree <- beta.transactionTreeById(tree.transactionId, bob)
        gbpTree <- eventually("transactionTreeById2") {
          alpha.transactionTreeById(tree.transactionId, gbp_bank)
        }
        dkkTree <- eventually("transactionTreeById3") {
          delta.transactionTreeById(tree.transactionId, dkk_bank)
        }
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
    "TXTreeChildOrder",
    "Trees formed by childEventIds should be maintaining Transaction event order",
    allocate(TwoParties, SingleParty, SingleParty),
    enabled = _.committerEventLog.eventLogType.isCentralized,
    tags = privacyHappyCase(
      asset = "Transaction Tree",
      happyCase = "Trees formed by childEventIds should be maintaining Transaction event order",
    ),
  )(implicit ec => {
    case Participants(
          Participant(alpha, alice, gbp_bank),
          Participant(beta, bob),
          Participant(delta, dkk_bank),
        ) =>
      for {
        gbpIouIssue <- alpha.create(gbp_bank, Iou(gbp_bank, gbp_bank, "GBP", 100, Nil))
        gbpTransfer <-
          alpha.exerciseAndGetContract(gbp_bank, gbpIouIssue.exerciseIou_Transfer(alice))
        dkkIouIssue <- delta.create(dkk_bank, Iou(dkk_bank, dkk_bank, "DKK", 110, Nil))
        dkkTransfer <-
          delta.exerciseAndGetContract(dkk_bank, dkkIouIssue.exerciseIou_Transfer(bob))

        aliceIou1 <- eventually("exerciseIouTransfer_Accept") {
          alpha.exerciseAndGetContract(alice, gbpTransfer.exerciseIouTransfer_Accept())
        }
        aliceIou <- eventually("exerciseIou_AddObserver") {
          alpha.exerciseAndGetContract(alice, aliceIou1.exerciseIou_AddObserver(bob))
        }
        bobIou <- eventually("exerciseIouTransfer_Accept") {
          beta.exerciseAndGetContract(bob, dkkTransfer.exerciseIouTransfer_Accept())
        }

        trade <- eventually("create") {
          alpha.create(
            alice,
            IouTrade(alice, bob, aliceIou, gbp_bank, "GBP", 100, dkk_bank, "DKK", 110),
          )
        }
        tree <- eventually("exerciseIouTrade_Accept") {
          beta.exercise(bob, trade.exerciseIouTrade_Accept(bobIou))
        }

        aliceTree <- eventually("transactionTreeById1") {
          alpha.transactionTreeById(tree.transactionId, alice)
        }
        bobTree <- beta.transactionTreeById(tree.transactionId, bob)
        gbpTree <- eventually("transactionTreeById2") {
          alpha.transactionTreeById(tree.transactionId, gbp_bank)
        }
        dkkTree <- eventually("transactionTreeById3") {
          delta.transactionTreeById(tree.transactionId, dkk_bank)
        }
        aliceTrees <- alpha.transactionTrees(alice)
        bobTrees <- alpha.transactionTrees(bob)
        gbpTrees <- alpha.transactionTrees(gbp_bank)
        dkkTrees <- alpha.transactionTrees(dkk_bank)
      } yield {
        def treeIsWellformed(tree: TransactionTree): Unit = {
          val eventsToObserve = mutable.Map.empty[String, TreeEvent] ++= tree.eventsById

          def go(eventId: String): Unit = {
            eventsToObserve.remove(eventId) match {
              case Some(TreeEvent(Exercised(exercisedEvent))) =>
                val expected = exercisedEvent.childEventIds.sortBy(stringEventId =>
                  EventId.assertFromString(stringEventId).nodeId.index
                )
                val actual = exercisedEvent.childEventIds
                assertEquals(
                  context = s"childEventIds are out of order. Expected: $expected, actual: $actual",
                  actual = actual,
                  expected = expected,
                )
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

        Iterator(
          aliceTrees,
          bobTrees,
          gbpTrees,
          dkkTrees,
        ).flatten.foreach(treeIsWellformed)
      }
  })

  test(
    "TXNotDivulge",
    "Data should not be exposed to parties unrelated to a transaction",
    allocate(SingleParty, SingleParty),
    tags = privacyHappyCase(
      asset = "Transaction",
      happyCase = "Transactions are not exposed to parties unrelated to a transaction",
    ),
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
    "TXTreeHideCommandIdToNonSubmittingStakeholders",
    "A transaction tree should be visible to a non-submitting stakeholder but its command identifier should be empty",
    allocate(SingleParty, SingleParty),
    enabled = _.committerEventLog.eventLogType.isCentralized,
    tags = privacyHappyCase(
      asset = "Transaction Tree",
      happyCase =
        "Transaction tree is visible to a non-submitting stakeholder but its command identifier should be empty",
    ),
  )(implicit ec => { case Participants(Participant(alpha, submitter), Participant(beta, listener)) =>
    for {
      (id, _) <- alpha.createAndGetTransactionId(
        submitter,
        AgreementFactory(listener, submitter),
      )
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
    enabled = _.committerEventLog.eventLogType.isCentralized,
    tags = privacyHappyCase(
      asset = "Flat Transaction",
      happyCase =
        "A flat transaction is visible to a non-submitting stakeholder but its command identifier is empty",
    ),
  )(implicit ec => {
    case Participants(Participant(alpha, submitter), Participant(beta, listener)) =>
      for {
        (id, _) <- alpha.createAndGetTransactionId(
          submitter,
          AgreementFactory(listener, submitter),
        )
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
    "TXNotDiscloseCreateToNonSignatory",
    "Not disclose create to non-chosen branching signatory",
    allocate(SingleParty, SingleParty),
    tags = privacyHappyCase(
      asset = "Flat Transaction",
      happyCase =
        "Transaction with a create event is not disclosed to non-chosen branching signatory",
    ),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    val template = BranchingSignatories(whichSign = false, signTrue = alice, signFalse = bob)
    val create = beta.submitAndWaitRequest(bob, template.create.command)
    for {
      transactionResponse <- beta.submitAndWaitForTransaction(create)
      _ <- synchronize(alpha, beta)
      aliceTransactions <- alpha.flatTransactions(alice)
    } yield {
      val transaction = transactionResponse.getTransaction
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
    tags = privacyHappyCase(
      asset = "Flat Transaction",
      happyCase = "Transaction with a create event is disclosed to chosen branching controller",
    ),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob, eve)) =>
    val template =
      BranchingControllers(giver = alice, whichCtrl = true, ctrlTrue = bob, ctrlFalse = eve)
    for {
      _ <- alpha.create(alice, template)
      _ <- eventually("flatTransactions") {
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
    tags = privacyHappyCase(
      asset = "Flat Transaction",
      happyCase =
        "Transaction with a create event is not disclosed to non-chosen branching controller",
    ),
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob, eve)) =>
    val template =
      BranchingControllers(giver = alice, whichCtrl = false, ctrlTrue = bob, ctrlFalse = eve)
    val create = alpha.submitAndWaitRequest(alice, template.create.command)
    for {
      transactionResponse <- alpha.submitAndWaitForTransaction(create)
      _ <- synchronize(alpha, beta)
      transactions <- beta.flatTransactions(bob)
    } yield {
      val transaction = transactionResponse.getTransaction
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
    enabled = _.committerEventLog.eventLogType.isCentralized,
    tags = privacyHappyCase(
      asset = "Flat Transaction",
      happyCase = "Transaction with a create event is disclosed to observers",
    ),
  )(implicit ec => {
    case Participants(Participant(alpha, alice), Participant(beta, observers @ _*)) =>
      val template = WithObservers(alice, Primitive.List(observers: _*))
      val create = alpha.submitAndWaitRequest(alice, template.create.command)
      for {
        transactionId <- alpha.submitAndWaitForTransactionId(create).map(_.transactionId)
        _ <- eventually("flatTransactions") {
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
    "TXFlatTransactionsVisibility",
    "Transactions in the flat transactions stream should be disclosed only to the stakeholders",
    allocate(Parties(3)),
    timeoutScale = 2.0,
    tags = privacyHappyCase(
      asset = "Transaction",
      happyCase =
        "Transactions in the flat transactions stream are disclosed only to the stakeholders",
    ),
  )(implicit ec => { case Participants(Participant(ledger, bank, alice, bob)) =>
    for {
      gbpIouIssue <- ledger.create(bank, Iou(bank, bank, "GBP", 100, Nil))
      gbpTransfer <- ledger.exerciseAndGetContract(
        bank,
        gbpIouIssue.exerciseIou_Transfer(alice),
      )
      dkkIouIssue <- ledger.create(bank, Iou(bank, bank, "DKK", 110, Nil))
      dkkTransfer <- ledger.exerciseAndGetContract(bank, dkkIouIssue.exerciseIou_Transfer(bob))
      aliceIou1 <- ledger.exerciseAndGetContract(alice, gbpTransfer.exerciseIouTransfer_Accept())
      aliceIou <- ledger.exerciseAndGetContract(alice, aliceIou1.exerciseIou_AddObserver(bob))
      bobIou <- ledger.exerciseAndGetContract(bob, dkkTransfer.exerciseIouTransfer_Accept())

      trade <- ledger.create(
        alice,
        IouTrade(alice, bob, aliceIou, bank, "GBP", 100, bank, "DKK", 110),
      )

      tree <- ledger.exercise(bob, trade.exerciseIouTrade_Accept(bobIou))

      aliceTransactions <- ledger.flatTransactions(alice)
      bobTransactions <- ledger.flatTransactions(bob)
    } yield {
      val newIouList = createdEvents(tree)
        .filter(event => event.templateId.exists(_.entityName == "Iou"))

      assert(
        newIouList.length == 2,
        s"Expected 2 new IOUs created, found: ${newIouList.length}",
      )

      @nowarn("cat=lint-infer-any")
      val newAliceIou = newIouList
        .find(iou => iou.signatories.contains(alice) && iou.signatories.contains(bank))
        .map(_.contractId)
        .getOrElse {
          fail(s"Not found an IOU owned by $alice")
        }

      @nowarn("cat=lint-infer-any")
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
    tags = privacyHappyCase(
      asset = "Transaction",
      happyCase = "Transactions in the flat transactions stream are not leaking witnesses",
    ),
  )(implicit ec => { case Participants(Participant(ledger, bank, alice, bob)) =>
    for {
      iouIssue <- ledger.create(bank, Iou(bank, bank, "GBP", 100, Nil))
      transfer <- ledger.exerciseAndGetContract(bank, iouIssue.exerciseIou_Transfer(alice))
      aliceIou <- ledger.exerciseAndGetContract(alice, transfer.exerciseIouTransfer_Accept())
      _ <- ledger.exerciseAndGetContract(alice, aliceIou.exerciseIou_AddObserver(bob))
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
    tags = privacyHappyCase(
      asset = "Transaction",
      happyCase = "Transactions in the transaction trees stream are not leaking witnesses",
    ),
  )(implicit ec => { case Participants(Participant(ledger, bank, alice, bob)) =>
    for {
      iouIssue <- ledger.create(bank, Iou(bank, bank, "GBP", 100, Nil))
      transfer <- ledger.exerciseAndGetContract(bank, iouIssue.exerciseIou_Transfer(alice))
      aliceIou <- ledger.exerciseAndGetContract(alice, transfer.exerciseIouTransfer_Accept())
      _ <- ledger.exerciseAndGetContract(alice, aliceIou.exerciseIou_AddObserver(bob))
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

      onlyRequestingPartiesAsWitnesses(
        allTxTreesWitnesses(aliceBankTransactionTrees),
        alice,
        bank,
      )(
        "Alice's and Bank's transaction trees contain other parties as witnesses"
      )
    }
  })
}

object TransactionServiceVisibilityIT {
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
