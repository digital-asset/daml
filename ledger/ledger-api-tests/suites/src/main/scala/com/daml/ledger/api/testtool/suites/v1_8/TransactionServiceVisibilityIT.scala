// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.Eventually.eventually
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

import scala.collection.immutable.Seq
import scala.collection.mutable

class TransactionServiceVisibilityIT extends LedgerTestSuite {
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
        tree <- eventually {
          beta.exercise(bob, trade.exerciseIouTrade_Accept(_, bobIou))
        }

        aliceTree <- eventually {
          alpha.transactionTreeById(tree.transactionId, alice)
        }
        bobTree <- beta.transactionTreeById(tree.transactionId, bob)
        gbpTree <- eventually {
          alpha.transactionTreeById(tree.transactionId, gbp_bank)
        }
        dkkTree <- eventually {
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
    "TXTreeHideCommandIdToNonSubmittingStakeholders",
    "A transaction tree should be visible to a non-submitting stakeholder but its command identifier should be empty",
    allocate(SingleParty, SingleParty),
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
  )(implicit ec => {
    case Participants(Participant(alpha, alice), Participant(beta, observers @ _*)) =>
      val template = WithObservers(alice, Primitive.List(observers: _*))
      val create = alpha.submitAndWaitRequest(alice, template.create.command)
      for {
        transactionId <- alpha.submitAndWaitForTransactionId(create).map(_.transactionId)
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
    "TXFlatTransactionsVisibility",
    "Transactions in the flat transactions stream should be disclosed only to the stakeholders",
    allocate(Parties(3)),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(ledger, bank, alice, bob)) =>
    for {
      gbpIouIssue <- ledger.create(bank, Iou(bank, bank, "GBP", 100, Nil))
      gbpTransfer <- ledger.exerciseAndGetContract(
        bank,
        gbpIouIssue.exerciseIou_Transfer(_, alice),
      )
      dkkIouIssue <- ledger.create(bank, Iou(bank, bank, "DKK", 110, Nil))
      dkkTransfer <- ledger.exerciseAndGetContract(bank, dkkIouIssue.exerciseIou_Transfer(_, bob))
      aliceIou1 <- ledger.exerciseAndGetContract(alice, gbpTransfer.exerciseIouTransfer_Accept(_))
      aliceIou <- ledger.exerciseAndGetContract(alice, aliceIou1.exerciseIou_AddObserver(_, bob))
      bobIou <- ledger.exerciseAndGetContract(bob, dkkTransfer.exerciseIouTransfer_Accept(_))

      trade <- ledger.create(
        alice,
        IouTrade(alice, bob, aliceIou, bank, "GBP", 100, bank, "DKK", 110),
      )

      tree <- ledger.exercise(bob, trade.exerciseIouTrade_Accept(_, bobIou))

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
      aliceIou <- ledger.exerciseAndGetContract(alice, transfer.exerciseIouTransfer_Accept(_))
      _ <- ledger.exerciseAndGetContract(alice, aliceIou.exerciseIou_AddObserver(_, bob))
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
      aliceIou <- ledger.exerciseAndGetContract(alice, transfer.exerciseIouTransfer_Accept(_))
      _ <- ledger.exerciseAndGetContract(alice, aliceIou.exerciseIou_AddObserver(_, bob))
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
