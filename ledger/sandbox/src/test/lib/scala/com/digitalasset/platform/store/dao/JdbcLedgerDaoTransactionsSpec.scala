// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.data.Ref.{Identifier, Party}
import com.digitalasset.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.EventId
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_service.GetTransactionsResponse
import com.digitalasset.platform.ApiOffset
import com.digitalasset.platform.store.entries.LedgerEntry
import org.scalatest.{AsyncFlatSpec, Inside, LoneElement, Matchers, OptionValues}

import scala.concurrent.Future

private[dao] trait JdbcLedgerDaoTransactionsSpec extends OptionValues with Inside with LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (lookupFlatTransactionById)"

  it should "return nothing for a mismatching transaction id" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(transactionId = "WRONG", Set(tx.submittingParty.get))
    } yield {
      result shouldBe None
    }
  }

  it should "return nothing for a mismatching party" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(tx.transactionId, Set("WRONG"))
    } yield {
      result shouldBe None
    }
  }

  it should "return the expected flat transaction for a correct request (create)" in {
    for {
      (offset, tx) <- store(singleCreate)
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(tx.transactionId, Set(tx.submittingParty.get))
    } yield {
      inside(result.value.transaction) {
        case Some(transaction) =>
          transaction.commandId shouldBe tx.commandId.get
          transaction.offset shouldBe ApiOffset.toApiString(offset)
          transaction.effectiveAt.value.seconds shouldBe tx.ledgerEffectiveTime.getEpochSecond
          transaction.effectiveAt.value.nanos shouldBe tx.ledgerEffectiveTime.getNano
          transaction.transactionId shouldBe tx.transactionId
          transaction.workflowId shouldBe tx.workflowId.getOrElse("")
          inside(transaction.events.loneElement.event.created) {
            case Some(created) =>
              val (eventId, createNode: NodeCreate.WithTxValue[AbsoluteContractId]) =
                tx.transaction.nodes.head
              created.eventId shouldBe eventId
              created.witnessParties should contain only tx.submittingParty.get
              created.agreementText.getOrElse("") shouldBe createNode.coinst.agreementText
              created.contractKey shouldBe None
              created.createArguments shouldNot be(None)
              created.signatories should contain theSameElementsAs createNode.signatories
              created.observers should contain theSameElementsAs createNode.stakeholders.diff(
                createNode.signatories)
              created.templateId shouldNot be(None)
          }
      }
    }
  }

  it should "return the expected flat transaction for a correct request (exercise)" in {
    for {
      (_, create) <- store(singleCreate)
      (offset, exercise) <- store(singleExercise(nonTransient(create).loneElement))
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(exercise.transactionId, Set(exercise.submittingParty.get))
    } yield {
      inside(result.value.transaction) {
        case Some(transaction) =>
          transaction.commandId shouldBe exercise.commandId.get
          transaction.offset shouldBe ApiOffset.toApiString(offset)
          transaction.transactionId shouldBe exercise.transactionId
          transaction.effectiveAt.value.seconds shouldBe exercise.ledgerEffectiveTime.getEpochSecond
          transaction.effectiveAt.value.nanos shouldBe exercise.ledgerEffectiveTime.getNano
          transaction.workflowId shouldBe exercise.workflowId.getOrElse("")
          inside(transaction.events.loneElement.event.archived) {
            case Some(archived) =>
              val (eventId, exerciseNode: NodeExercises.WithTxValue[EventId, AbsoluteContractId]) =
                exercise.transaction.nodes.head
              archived.eventId shouldBe eventId
              archived.witnessParties should contain only exercise.submittingParty.get
              archived.contractId shouldBe exerciseNode.targetCoid.coid
              archived.templateId shouldNot be(None)
          }
      }
    }
  }

  it should "hide events on transient contracts to the original submitter" in {
    for {
      (offset, tx) <- store(fullyTransient)
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(tx.transactionId, Set(tx.submittingParty.get))
    } yield {
      inside(result.value.transaction) {
        case Some(transaction) =>
          transaction.commandId shouldBe tx.commandId.get
          transaction.offset shouldBe ApiOffset.toApiString(offset)
          transaction.transactionId shouldBe tx.transactionId
          transaction.effectiveAt.value.seconds shouldBe tx.ledgerEffectiveTime.getEpochSecond
          transaction.effectiveAt.value.nanos shouldBe tx.ledgerEffectiveTime.getNano
          transaction.workflowId shouldBe tx.workflowId.getOrElse("")
          transaction.events shouldBe Seq.empty
      }
    }
  }

  behavior of "JdbcLedgerDao (getFlatTransactions)"

  it should "match the results of lookupFlatTransactionById" in {
    for {
      (from, to, transactions) <- storeTestFixture()
      individualLookups <- lookupIndividually(transactions, Set(alice, bob, charlie))
      result <- transactionsOf(
        ledgerDao.transactionsReader
          .getFlatTransactions(
            startExclusive = from,
            endInclusive = to,
            filter = Map(alice -> Set.empty, bob -> Set.empty, charlie -> Set.empty),
            pageSize = 100,
          ))
    } yield {
      result should contain theSameElementsInOrderAs individualLookups
    }
  }

  it should "return the same result regardless the page size" in {
    for {
      (from, to, _) <- storeTestFixture()
      result1 <- transactionsOf(
        ledgerDao.transactionsReader
          .getFlatTransactions(
            startExclusive = from,
            endInclusive = to,
            filter = Map(alice -> Set.empty, bob -> Set.empty, charlie -> Set.empty),
            pageSize = 100,
          ))
      result2 <- transactionsOf(
        ledgerDao.transactionsReader
          .getFlatTransactions(
            startExclusive = from,
            endInclusive = to,
            filter = Map(alice -> Set.empty, bob -> Set.empty, charlie -> Set.empty),
            pageSize = 1,
          ))
    } yield {
      result1 should contain theSameElementsInOrderAs result2
    }
  }

  it should "filter correctly by party" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, tx) <- store(withChildren)
      to <- ledgerDao.lookupLedgerEnd()
      individualLookupForAlice <- lookupIndividually(Seq(tx), as = Set(alice))
      individualLookupForBob <- lookupIndividually(Seq(tx), as = Set(bob))
      individualLookupForCharlie <- lookupIndividually(Seq(tx), as = Set(charlie))
      resultForAlice <- transactionsOf(
        ledgerDao.transactionsReader
          .getFlatTransactions(
            startExclusive = from,
            endInclusive = to,
            filter = Map(alice -> Set.empty),
            pageSize = 100,
          ))
      resultForBob <- transactionsOf(
        ledgerDao.transactionsReader
          .getFlatTransactions(
            startExclusive = from,
            endInclusive = to,
            filter = Map(bob -> Set.empty),
            pageSize = 100,
          ))
      resultForCharlie <- transactionsOf(
        ledgerDao.transactionsReader
          .getFlatTransactions(
            startExclusive = from,
            endInclusive = to,
            filter = Map(charlie -> Set.empty),
            pageSize = 100,
          ))
    } yield {
      individualLookupForAlice should contain theSameElementsInOrderAs resultForAlice
      individualLookupForBob should contain theSameElementsInOrderAs resultForBob
      individualLookupForCharlie should contain theSameElementsInOrderAs resultForCharlie
    }
  }

  it should "filter correctly for a single party" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- store(
        multipleCreates(
          operator = "operator",
          signatoriesAndTemplates = Seq(
            alice -> "pkg:mod:Template1",
            bob -> "pkg:mod:Template3",
            alice -> "pkg:mod:Template3",
          )
        ))
      to <- ledgerDao.lookupLedgerEnd()
      result <- transactionsOf(
        ledgerDao.transactionsReader
          .getFlatTransactions(
            startExclusive = from,
            endInclusive = to,
            filter = Map(alice -> Set(Identifier.assertFromString("pkg:mod:Template3"))),
            pageSize = 100,
          ))
    } yield {
      inside(result.loneElement.events.loneElement.event.created) {
        case Some(create) =>
          create.witnessParties.loneElement shouldBe alice
          val identifier = create.templateId.value
          identifier.packageId shouldBe "pkg"
          identifier.moduleName shouldBe "mod"
          identifier.entityName shouldBe "Template3"
      }
    }
  }

  it should "filter correctly by multiple parties with the same template" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- store(
        multipleCreates(
          operator = "operator",
          signatoriesAndTemplates = Seq(
            alice -> "pkg:mod:Template1",
            bob -> "pkg:mod:Template3",
            alice -> "pkg:mod:Template3",
          )
        ))
      to <- ledgerDao.lookupLedgerEnd()
      result <- transactionsOf(
        ledgerDao.transactionsReader
          .getFlatTransactions(
            startExclusive = from,
            endInclusive = to,
            filter = Map(
              alice -> Set(
                Identifier.assertFromString("pkg:mod:Template3"),
              ),
              bob -> Set(
                Identifier.assertFromString("pkg:mod:Template3"),
              )
            ),
            pageSize = 100,
          ))
    } yield {
      val events = result.loneElement.events.toArray
      events should have length 2
      inside(events(0).event.created) {
        case Some(create) =>
          create.witnessParties.loneElement shouldBe bob
          val identifier = create.templateId.value
          identifier.packageId shouldBe "pkg"
          identifier.moduleName shouldBe "mod"
          identifier.entityName shouldBe "Template3"
      }
      inside(events(1).event.created) {
        case Some(create) =>
          create.witnessParties.loneElement shouldBe alice
          val identifier = create.templateId.value
          identifier.packageId shouldBe "pkg"
          identifier.moduleName shouldBe "mod"
          identifier.entityName shouldBe "Template3"
      }
    }
  }

  it should "filter correctly by multiple parties with different templates" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- store(
        multipleCreates(
          operator = "operator",
          signatoriesAndTemplates = Seq(
            alice -> "pkg:mod:Template1",
            bob -> "pkg:mod:Template3",
            alice -> "pkg:mod:Template3",
          )
        ))
      to <- ledgerDao.lookupLedgerEnd()
      result <- transactionsOf(
        ledgerDao.transactionsReader
          .getFlatTransactions(
            startExclusive = from,
            endInclusive = to,
            filter = Map(
              alice -> Set(
                Identifier.assertFromString("pkg:mod:Template1"),
              ),
              bob -> Set(
                Identifier.assertFromString("pkg:mod:Template3"),
              )
            ),
            pageSize = 100,
          ))
    } yield {
      val events = result.loneElement.events.toArray
      events should have length 2
      inside(events(0).event.created) {
        case Some(create) =>
          create.witnessParties.loneElement shouldBe alice
          val identifier = create.templateId.value
          identifier.packageId shouldBe "pkg"
          identifier.moduleName shouldBe "mod"
          identifier.entityName shouldBe "Template1"
      }
      inside(events(1).event.created) {
        case Some(create) =>
          create.witnessParties.loneElement shouldBe bob
          val identifier = create.templateId.value
          identifier.packageId shouldBe "pkg"
          identifier.moduleName shouldBe "mod"
          identifier.entityName shouldBe "Template3"
      }
    }
  }

  it should "filter correctly by multiple parties with different template and wildcards" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, _) <- store(
        multipleCreates(
          operator = "operator",
          signatoriesAndTemplates = Seq(
            alice -> "pkg:mod:Template1",
            bob -> "pkg:mod:Template3",
            alice -> "pkg:mod:Template3",
          )
        ))
      to <- ledgerDao.lookupLedgerEnd()
      result <- transactionsOf(
        ledgerDao.transactionsReader
          .getFlatTransactions(
            startExclusive = from,
            endInclusive = to,
            filter = Map(
              alice -> Set(
                Identifier.assertFromString("pkg:mod:Template3"),
              ),
              bob -> Set.empty
            ),
            pageSize = 100,
          ))
    } yield {
      val events = result.loneElement.events.toArray
      events should have length 2
      inside(events(0).event.created) {
        case Some(create) =>
          create.witnessParties.loneElement shouldBe bob
          val identifier = create.templateId.value
          identifier.packageId shouldBe "pkg"
          identifier.moduleName shouldBe "mod"
          identifier.entityName shouldBe "Template3"
      }
      inside(events(1).event.created) {
        case Some(create) =>
          create.witnessParties.loneElement shouldBe alice
          val identifier = create.templateId.value
          identifier.packageId shouldBe "pkg"
          identifier.moduleName shouldBe "mod"
          identifier.entityName shouldBe "Template3"
      }
    }
  }

  private def onlyFor(party: Party): Map[Party, Set[Identifier]] =
    Map(party -> Set.empty)

  private def storeTestFixture(): Future[(Offset, Offset, Seq[LedgerEntry.Transaction])] =
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, t1) <- store(singleCreate)
      (_, t2) <- store(singleCreate)
      (_, t3) <- store(singleExercise(nonTransient(t2).loneElement))
      (_, t4) <- store(fullyTransient)
      (_, t5) <- store(fullyTransientWithChildren)
      (_, t6) <- store(withChildren)
      to <- ledgerDao.lookupLedgerEnd()
    } yield (from, to, Seq(t1, t2, t3, t4, t5, t6))

  private def lookupIndividually(
      transactions: Seq[LedgerEntry.Transaction],
      as: Set[Party],
  ): Future[Seq[Transaction]] =
    Future
      .sequence(
        transactions.map(tx =>
          ledgerDao.transactionsReader
            .lookupFlatTransactionById(tx.transactionId, as)))
      .map(_.flatMap(_.toList.flatMap(_.transaction.toList)))

  private def transactionsOf(
      source: Source[(Offset, GetTransactionsResponse), NotUsed],
  ): Future[Seq[Transaction]] =
    source
      .map(_._2)
      .runWith(Sink.seq)
      .map(_.flatMap(_.transactions))

}
