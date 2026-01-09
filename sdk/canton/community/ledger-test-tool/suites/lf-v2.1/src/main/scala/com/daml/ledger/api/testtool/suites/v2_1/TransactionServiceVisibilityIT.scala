// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.testtool.infrastructure.TransactionOps.*
import com.daml.ledger.api.testtool.infrastructure.{Eventually, LedgerTestSuite, Party}
import com.daml.ledger.api.testtool.suites.v2_1.TransactionServiceVisibilityIT.*
import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.event.Event.Event.Exercised
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.value.Record
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.model.iou.{Iou, IouTransfer}
import com.daml.ledger.test.java.model.ioutrade.IouTrade
import com.daml.ledger.test.java.model.test.{
  AgreementFactory,
  BranchingControllers,
  BranchingSignatories,
  Dummy,
  WithObservers,
}
import com.daml.test.evidence.tag.EvidenceTag
import com.daml.test.evidence.tag.Security.SecurityTest
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Privacy
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.platform.store.utils.EventOps.EventOps

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

class TransactionServiceVisibilityIT extends LedgerTestSuite {

  import com.digitalasset.canton.BigDecimalImplicits.*
  import CompanionImplicits.*

  implicit val iouTransferCompanion
      : ContractCompanion.WithoutKey[IouTransfer.Contract, IouTransfer.ContractId, IouTransfer] =
    IouTransfer.COMPANION
  implicit val iouTradeCompanion
      : ContractCompanion.WithoutKey[IouTrade.Contract, IouTrade.ContractId, IouTrade] =
    IouTrade.COMPANION

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
    "TXLedgerEffectsBlinding",
    "LedgerEffects transactions should be served according to the blinding/projection rules",
    allocate(TwoParties, SingleParty, SingleParty),
    tags = privacyHappyCase(
      asset = "LedgerEffects Transaction",
      happyCase = "LedgerEffects transactions are served according to the blinding/projection rules",
    ),
  )(implicit ec => {
    case Participants(
          Participant(alpha, Seq(alice, gbp_bank)),
          Participant(beta, Seq(bob)),
          Participant(delta, Seq(dkk_bank)),
        ) =>
      for {
        gbpIouIssue <- alpha.create(
          gbp_bank,
          new Iou(gbp_bank, gbp_bank, "GBP", 100.toBigDecimal, Nil.asJava),
        )
        gbpTransfer <-
          alpha.exerciseAndGetContract[IouTransfer.ContractId, IouTransfer](
            gbp_bank,
            gbpIouIssue.exerciseIou_Transfer(alice),
          )
        dkkIouIssue <- delta.create(
          dkk_bank,
          new Iou(dkk_bank, dkk_bank, "DKK", 110.toBigDecimal, Nil.asJava),
        )
        dkkTransfer <-
          delta.exerciseAndGetContract[IouTransfer.ContractId, IouTransfer](
            dkk_bank,
            dkkIouIssue.exerciseIou_Transfer(bob),
          )

        aliceIou1 <- eventually("exerciseIouTransfer_Accept") {
          alpha.exerciseAndGetContract[Iou.ContractId, Iou](
            alice,
            gbpTransfer.exerciseIouTransfer_Accept(),
          )
        }
        aliceIou <- eventually("exerciseIou_AddObserver") {
          alpha.exerciseAndGetContract[Iou.ContractId, Iou](
            alice,
            aliceIou1.exerciseIou_AddObserver(bob),
          )
        }
        bobIou <- eventually("exerciseIouTransfer_Accept") {
          beta.exerciseAndGetContract[Iou.ContractId, Iou](
            bob,
            dkkTransfer.exerciseIouTransfer_Accept(),
          )
        }

        trade <- eventually("create") {
          alpha.create(
            alice,
            new IouTrade(
              alice,
              bob,
              aliceIou,
              gbp_bank,
              "GBP",
              100.toBigDecimal,
              dkk_bank,
              "DKK",
              110.toBigDecimal,
            ),
          )
        }
        tx <- eventually("exerciseIouTrade_Accept") {
          beta.exercise(bob, trade.exerciseIouTrade_Accept(bobIou))
        }

        aliceTree <- eventually("transactionTreeById1") {
          alpha.transactionById(tx.updateId, Seq(alice), LedgerEffects)
        }
        bobTree <- beta.transactionById(tx.updateId, Seq(bob), LedgerEffects)
        gbpTree <- eventually("transactionTreeById2") {
          alpha.transactionById(tx.updateId, Seq(gbp_bank), LedgerEffects)
        }
        dkkTree <- eventually("transactionTreeById3") {
          delta.transactionById(tx.updateId, Seq(dkk_bank), LedgerEffects)
        }
      } yield {
        def treeIsWellformed(tx: Transaction): Unit = {
          val eventsToObserve =
            mutable.Map.empty[Int, Event] ++= tx.events.map(e => e.nodeId -> e)

          def go(nodeId: Int): Unit = {
            val lastDescendantNodeId = eventsToObserve
              .getOrElse(
                nodeId,
                throw new AssertionError(
                  s"Referenced nodeId $nodeId is not available as node in the transaction."
                ),
              )
              .event
              .exercised
              .fold(nodeId)(_.lastDescendantNodeId)
            val descendantNodeIds = // including itself
              eventsToObserve.view.keys.filter(id => nodeId <= id && id <= lastDescendantNodeId)

            descendantNodeIds.foreach(id =>
              eventsToObserve.remove(id) match {
                case Some(_) => ()
                case None =>
                  throw new AssertionError(
                    s"Referenced nodeId $id is not available as node in the transaction."
                  )
              }
            )
          }

          tx.rootNodeIds().foreach(go)
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
        assert(aliceTree.events.sizeIs == 9)
        assert(bobTree.events.sizeIs == 9)

        assert(aliceTree.rootNodeIds().sizeIs == 1)
        assert(bobTree.rootNodeIds().sizeIs == 1)

        // banks only see the transfer of their issued Iou:
        // Exercise Iou.Iou_Transfer -> Create IouTransfer
        // Exercise IouTransfer.IouTransfer_Accept -> Create Iou
        assert(gbpTree.events.sizeIs == 4)
        assert(dkkTree.events.sizeIs == 4)

        // the exercises are the root nodes
        assert(gbpTree.rootNodeIds().sizeIs == 2)
        assert(dkkTree.rootNodeIds().sizeIs == 2)

      }
  })

  test(
    "TXTreeLastDescendant",
    "Trees formed by lastDescendantNodeIds should be maintaining Transaction event order",
    allocate(TwoParties, SingleParty, SingleParty),
    tags = privacyHappyCase(
      asset = "Transaction Tree",
      happyCase =
        "Trees formed by lastDescendantNodeIds should be maintaining Transaction event order",
    ),
  )(implicit ec => {
    case Participants(
          Participant(alpha, Seq(alice, gbp_bank)),
          Participant(beta, Seq(bob)),
          Participant(delta, Seq(dkk_bank)),
        ) =>
      for {
        gbpIouIssue <- alpha.create(
          gbp_bank,
          new Iou(gbp_bank, gbp_bank, "GBP", 100.toBigDecimal, Nil.asJava),
        )
        gbpTransfer <-
          alpha.exerciseAndGetContract[IouTransfer.ContractId, IouTransfer](
            gbp_bank,
            gbpIouIssue.exerciseIou_Transfer(alice),
          )
        dkkIouIssue <- delta.create(
          dkk_bank,
          new Iou(dkk_bank, dkk_bank, "DKK", 110.toBigDecimal, Nil.asJava),
        )
        dkkTransfer <-
          delta.exerciseAndGetContract[IouTransfer.ContractId, IouTransfer](
            dkk_bank,
            dkkIouIssue.exerciseIou_Transfer(bob),
          )

        aliceIou1 <- eventually("exerciseIouTransfer_Accept") {
          alpha.exerciseAndGetContract[Iou.ContractId, Iou](
            alice,
            gbpTransfer.exerciseIouTransfer_Accept(),
          )
        }
        aliceIou <- eventually("exerciseIou_AddObserver") {
          alpha.exerciseAndGetContract[Iou.ContractId, Iou](
            alice,
            aliceIou1.exerciseIou_AddObserver(bob),
          )
        }
        bobIou <- eventually("exerciseIouTransfer_Accept") {
          beta.exerciseAndGetContract[Iou.ContractId, Iou](
            bob,
            dkkTransfer.exerciseIouTransfer_Accept(),
          )
        }

        trade <- eventually("create") {
          alpha.create(
            alice,
            new IouTrade(
              alice,
              bob,
              aliceIou,
              gbp_bank,
              "GBP",
              100.toBigDecimal,
              dkk_bank,
              "DKK",
              110.toBigDecimal,
            ),
          )
        }
        tree <- eventually("exerciseIouTrade_Accept") {
          beta.exercise(bob, trade.exerciseIouTrade_Accept(bobIou))
        }

        aliceTree <- eventually("transactionTreeById1") {
          alpha.transactionById(tree.updateId, Seq(alice), LedgerEffects)
        }
        bobTree <- beta.transactionById(tree.updateId, Seq(bob), LedgerEffects)
        gbpTree <- eventually("transactionTreeById2") {
          alpha.transactionById(tree.updateId, Seq(gbp_bank), LedgerEffects)
        }
        dkkTree <- eventually("transactionTreeById3") {
          delta.transactionById(tree.updateId, Seq(dkk_bank), LedgerEffects)
        }
        aliceTrees <- alpha.transactions(LedgerEffects, alice)
        bobTrees <- alpha.transactions(LedgerEffects, bob)
        gbpTrees <- alpha.transactions(LedgerEffects, gbp_bank)
        dkkTrees <- alpha.transactions(LedgerEffects, dkk_bank)
      } yield {
        def treeIsWellformed(tree: Transaction): Unit = {
          val eventsToObserve =
            mutable.Map
              .empty[Int, Event.Event] ++= tree.events.view.map(e => e.nodeId -> e.event).toMap

          def go(nodeId: Int): Unit =
            eventsToObserve.remove(nodeId) match {
              case Some(Exercised(exercisedEvent)) =>
                val lastDescendantNodeId = exercisedEvent.lastDescendantNodeId
                assertGreaterOrEquals(
                  context =
                    s"lastDescendantNodeId is less than the node id. Expected: $lastDescendantNodeId >= ${exercisedEvent.nodeId}",
                  a = lastDescendantNodeId,
                  b = exercisedEvent.nodeId,
                )
                val descendantNodeIds =
                  eventsToObserve.view.keys.filter(id => nodeId < id && id <= lastDescendantNodeId)
                descendantNodeIds.foreach(eventsToObserve.remove(_).discard)
              case Some(_) => ()
              case None =>
                throw new AssertionError(
                  s"Referenced nodeId $nodeId is not available as node in the transaction."
                )
            }

          tree.rootNodeIds().foreach(go)
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
  )(implicit ec => { case Participants(Participant(alpha, Seq(alice)), Participant(_, Seq(bob))) =>
    for {
      _ <- alpha.create(alice, new Dummy(alice))
      bobsView <- alpha.transactions(AcsDelta, bob)
    } yield {
      assert(
        bobsView.isEmpty,
        s"After Alice create a contract, Bob sees one or more transaction he shouldn't, namely those created by commands ${bobsView.map(_.commandId).mkString(", ")}",
      )
    }
  })

  test(
    "TXLedgerEffectsHideCommandIdToNonSubmittingStakeholders",
    "A ledger effects transaction should be visible to a non-submitting stakeholder but its command identifier should be empty",
    allocate(SingleParty, SingleParty),
    tags = privacyHappyCase(
      asset = "Ledger Effects Transaction",
      happyCase =
        "A ledger effects transaction is visible to a non-submitting stakeholder but its command identifier should be empty",
    ),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(submitter)), Participant(beta, Seq(listener))) =>
      for {
        (id, _) <- alpha.createAndGetUpdateId(
          submitter,
          new AgreementFactory(listener, submitter),
        )
        _ <- p.synchronize
        tx <- beta.transactionById(id, Seq(listener), LedgerEffects)
        txsFromStream <- beta.transactions(LedgerEffects, listener)
      } yield {
        assert(
          tx.commandId.isEmpty,
          s"The command identifier for the transaction was supposed to be empty but it's `${tx.commandId}` instead.",
        )

        assert(
          txsFromStream.sizeIs == 1,
          s"One transaction expected but got ${txsFromStream.size} instead.",
        )

        val txFromStreamCommandId = txsFromStream.headOption.value.commandId
        assert(
          txFromStreamCommandId.isEmpty,
          s"The command identifier for the transaction was supposed to be empty but it's `$txFromStreamCommandId` instead.",
        )
      }
  })

  test(
    "TXHideCommandIdToNonSubmittingStakeholders",
    "An acs delta transaction should be visible to a non-submitting stakeholder but its command identifier should be empty",
    allocate(SingleParty, SingleParty),
    tags = privacyHappyCase(
      asset = "Acs Delta Transaction",
      happyCase =
        "An acs delta transaction is visible to a non-submitting stakeholder but its command identifier is empty",
    ),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(submitter)), Participant(beta, Seq(listener))) =>
      for {
        (id, _) <- alpha.createAndGetUpdateId(
          submitter,
          new AgreementFactory(listener, submitter),
        )
        _ <- p.synchronize
        tx <- beta.transactionById(id, Seq(listener), AcsDelta)
        txsFromStream <- beta.transactions(AcsDelta, listener)
      } yield {

        assert(
          tx.commandId.isEmpty,
          s"The command identifier for the flat transaction was supposed to be empty but it's `${tx.commandId}` instead.",
        )

        assert(
          txsFromStream.sizeIs == 1,
          s"One transaction expected but got ${txsFromStream.size} instead.",
        )

        val txFromStreamCommandId = txsFromStream.headOption.value.commandId
        assert(
          txFromStreamCommandId.isEmpty,
          s"The command identifier for the transaction was supposed to be empty but it's `$txFromStreamCommandId` instead.",
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
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(alice)), Participant(beta, Seq(bob))) =>
      val template = new BranchingSignatories(false, alice, bob)
      val create = beta.submitAndWaitForTransactionRequest(bob, template.create.commands)
      for {
        transactionResponse <- beta.submitAndWaitForTransaction(create)
        _ <- p.synchronize
        aliceTransactions <- alpha.transactions(AcsDelta, alice)
      } yield {
        val transaction = transactionResponse.getTransaction
        val branchingContractId = createdEvents(transaction)
          .map(_.contractId)
          .headOption
          .getOrElse(fail(s"Expected single create event"))
        val contractsVisibleByAlice = aliceTransactions.flatMap(createdEvents).map(_.contractId)
        assert(
          !contractsVisibleByAlice.contains(branchingContractId),
          s"The transaction ${transaction.updateId} should not have been disclosed.",
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
  )(implicit ec => {
    case Participants(Participant(alpha, Seq(alice)), Participant(beta, Seq(bob, eve))) =>
      import ClearIdsImplicits.*
      val template =
        new BranchingControllers(alice, true, bob, eve)
      for {
        _ <- alpha.create(alice, template)(BranchingControllers.COMPANION)
        _ <- eventually("flatTransactions") {
          for {
            aliceView <- alpha.transactions(AcsDelta, alice)
            bobView <- beta.transactions(AcsDelta, bob)
            evesView <- beta.transactions(AcsDelta, eve)
          } yield {
            val aliceCreate =
              assertSingleton("Alice should see one transaction", aliceView.flatMap(createdEvents))
            assertEquals(
              "Alice arguments do not match",
              aliceCreate.getCreateArguments.clearValueIds,
              Record.fromJavaProto(template.toValue.toProtoRecord),
            )
            val bobCreate =
              assertSingleton("Bob should see one transaction", bobView.flatMap(createdEvents))
            assertEquals(
              "Bob arguments do not match",
              bobCreate.getCreateArguments.clearValueIds,
              Record.fromJavaProto(template.toValue.toProtoRecord),
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
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(alice)), Participant(beta, Seq(bob, eve))) =>
      val template =
        new BranchingControllers(alice, false, bob, eve)
      val create = alpha.submitAndWaitForTransactionRequest(alice, template.create.commands)
      for {
        transactionResponse <- alpha.submitAndWaitForTransaction(create)
        _ <- p.synchronize
        transactions <- beta.transactions(AcsDelta, bob)
      } yield {
        val transaction = transactionResponse.getTransaction
        assert(
          !transactions.exists(_.updateId != transaction.updateId),
          s"The transaction ${transaction.updateId} should not have been disclosed.",
        )
      }
  })

  test(
    "TXDiscloseCreateToObservers",
    "Disclose create to observers",
    allocate(SingleParty, TwoParties),
    tags = privacyHappyCase(
      asset = "Flat Transaction",
      happyCase = "Transaction with a create event is disclosed to observers",
    ),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(alice)), Participant(beta, observers)) =>
      val template = new WithObservers(alice, observers.map(_.getValue).asJava)
      val create = alpha.submitAndWaitRequest(alice, template.create.commands)
      for {
        _ <- p.synchronize // Ensures parties are visible to both participants
        updateId <- alpha.submitAndWait(create).map(_.updateId)
        _ <- p.synchronize // Ensures the transactions are visible to both participants
        transactions <- beta.transactions(AcsDelta, observers*)
      } yield {
        assert(transactions.exists(_.updateId == updateId))
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
  )(implicit ec => { case Participants(Participant(ledger, Seq(bank, alice, bob))) =>
    for {
      gbpIouIssue <- ledger.create(bank, new Iou(bank, bank, "GBP", 100.toBigDecimal, Nil.asJava))
      gbpTransfer <- ledger.exerciseAndGetContract[IouTransfer.ContractId, IouTransfer](
        bank,
        gbpIouIssue.exerciseIou_Transfer(alice),
      )
      dkkIouIssue <- ledger.create(bank, new Iou(bank, bank, "DKK", 110.toBigDecimal, Nil.asJava))
      dkkTransfer <- ledger.exerciseAndGetContract[IouTransfer.ContractId, IouTransfer](
        bank,
        dkkIouIssue.exerciseIou_Transfer(bob),
      )
      aliceIou1 <- ledger.exerciseAndGetContract[Iou.ContractId, Iou](
        alice,
        gbpTransfer.exerciseIouTransfer_Accept(),
      )
      aliceIou <- ledger
        .exerciseAndGetContract[Iou.ContractId, Iou](alice, aliceIou1.exerciseIou_AddObserver(bob))
      bobIou <- ledger
        .exerciseAndGetContract[Iou.ContractId, Iou](bob, dkkTransfer.exerciseIouTransfer_Accept())

      trade <- ledger.create(
        alice,
        new IouTrade(
          alice,
          bob,
          aliceIou,
          bank,
          "GBP",
          100.toBigDecimal,
          bank,
          "DKK",
          110.toBigDecimal,
        ),
      )

      tree <- ledger.exercise(bob, trade.exerciseIouTrade_Accept(bobIou))

      aliceTransactions <- ledger.transactions(AcsDelta, alice)
      bobTransactions <- ledger.transactions(AcsDelta, bob)
    } yield {
      val newIouList = createdEvents(tree)
        .filter(event => event.templateId.exists(_.entityName == "Iou"))

      assert(
        newIouList.sizeIs == 2,
        s"Expected 2 new IOUs created, found: ${newIouList.length}",
      )

      val newAliceIou = newIouList
        .find(iou =>
          iou.signatories.contains(alice.getValue) && iou.signatories.contains(bank.getValue)
        )
        .map(_.contractId)
        .getOrElse {
          fail(s"Not found an IOU owned by $alice")
        }

      val newBobIou = newIouList
        .find(iou =>
          iou.signatories.contains(bob.getValue) && iou.signatories.contains(bank.getValue)
        )
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
  )(implicit ec => { case Participants(Participant(ledger, Seq(bank, alice, bob))) =>
    for {
      iouIssue <- ledger.create(bank, new Iou(bank, bank, "GBP", 100.toBigDecimal, Nil.asJava))
      transfer <- ledger.exerciseAndGetContract[IouTransfer.ContractId, IouTransfer](
        bank,
        iouIssue.exerciseIou_Transfer(alice),
      )
      aliceIou <- ledger
        .exerciseAndGetContract[Iou.ContractId, Iou](alice, transfer.exerciseIouTransfer_Accept())
      _ <- ledger
        .exerciseAndGetContract[Iou.ContractId, Iou](alice, aliceIou.exerciseIou_AddObserver(bob))
      aliceFlatTransactions <- ledger.transactions(AcsDelta, alice)
      bobFlatTransactions <- ledger.transactions(AcsDelta, bob)
      aliceBankFlatTransactions <- ledger.transactions(AcsDelta, alice, bank)
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
    "TXLedgerEffectsRequestingPartiesWitnessVisibility",
    "Transactions in the ledger effects transaction stream should not leak witnesses",
    allocate(Parties(3)),
    tags = privacyHappyCase(
      asset = "Transaction",
      happyCase = "Transactions in the ledger effects transaction stream are not leaking witnesses",
    ),
  )(implicit ec => { case Participants(Participant(ledger, Seq(bank, alice, bob))) =>
    for {
      iouIssue <- ledger.create(bank, new Iou(bank, bank, "GBP", 100.toBigDecimal, Nil.asJava))
      transfer <- ledger.exerciseAndGetContract[IouTransfer.ContractId, IouTransfer](
        bank,
        iouIssue.exerciseIou_Transfer(alice),
      )(IouTransfer.COMPANION)
      aliceIou <- ledger
        .exerciseAndGetContract[Iou.ContractId, Iou](alice, transfer.exerciseIouTransfer_Accept())
      _ <- ledger
        .exerciseAndGetContract[Iou.ContractId, Iou](alice, aliceIou.exerciseIou_AddObserver(bob))
      aliceTransactionsLedgerEffects <- ledger.transactions(LedgerEffects, alice)
      bobTransactionsLedgerEffects <- ledger.transactions(LedgerEffects, bob)
      aliceBankTransactionsLedgerEffects <- ledger.transactions(LedgerEffects, alice, bank)
    } yield {
      onlyRequestingPartiesAsWitnesses(allTxWitnesses(aliceTransactionsLedgerEffects), alice)(
        "Alice's ledger effects transactions contain other parties as witnesses"
      )

      onlyRequestingPartiesAsWitnesses(allTxWitnesses(bobTransactionsLedgerEffects), bob)(
        "Bob's ledger effects transactions contain other parties as witnesses"
      )

      onlyRequestingPartiesAsWitnesses(
        allTxWitnesses(aliceBankTransactionsLedgerEffects),
        alice,
        bank,
      )(
        "Alice's and Bank's ledger effects transactions contain other parties as witnesses"
      )
    }
  })
}

object TransactionServiceVisibilityIT {
  private def onlyRequestingPartiesAsWitnesses(
      allWitnesses: Set[String],
      requestingParties: Party*
  )(msg: String): Unit = {
    val nonRequestingWitnesses = allWitnesses.diff(requestingParties.map(_.getValue).toSet)
    assert(
      nonRequestingWitnesses.isEmpty,
      s"$msg: ${nonRequestingWitnesses.mkString("[", ",", "]")}",
    )
  }

  private def allTxWitnesses(transactions: Vector[Transaction]): Set[String] =
    transactions
      .flatMap(_.events.map(_.event).flatMap {
        case Event.Event.Empty => Seq.empty
        case Event.Event.Created(createdEvent) => createdEvent.witnessParties
        case Event.Event.Archived(archivedEvent) => archivedEvent.witnessParties
        case Event.Event.Exercised(exercisedEvent) => exercisedEvent.witnessParties
      })
      .toSet
}
