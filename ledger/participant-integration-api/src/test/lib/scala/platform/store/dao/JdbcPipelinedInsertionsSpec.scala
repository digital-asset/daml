// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.util.UUID

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.EventId
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.lf.transaction.BlindingInfo
import com.daml.lf.transaction.Node.NodeCreate
import com.daml.lf.value.Value.ContractId
import com.daml.platform.ApiOffset
import com.daml.platform.indexer.OffsetStep
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inside, LoneElement, OptionValues}

import scala.concurrent.Future

trait JdbcPipelinedInsertionsSpec extends Inside with OptionValues with Matchers with LoneElement {
  self: AsyncFlatSpec with JdbcLedgerDaoSuite with JdbcPipelinedTransactionInsertion =>
  private val ok = io.grpc.Status.Code.OK.value()
  behavior of "Pipelined JdbcLedgerDao (on PostgreSQL)"

  it should "match the results of lookupTransaction (flat and trees)" in {
    for {
      (from, to, transactions) <- storeTestFixture()
      flatTxs <- lookupFlatTxsIndividually(transactions, Set(alice, bob, charlie))
      treeTxs <- lookupTreeTxsIndividually(transactions, Set(alice, bob, charlie))
      flatTransactionsResult <- flatTxsOf(
        ledgerDao.transactionsReader
          .getFlatTransactions(
            startExclusive = from,
            endInclusive = to,
            filter = Map(alice -> Set.empty, bob -> Set.empty, charlie -> Set.empty),
            verbose = true,
          )
      )
      transactionTreesResult <- treeTxsOf(
        ledgerDao.transactionsReader
          .getTransactionTrees(
            startExclusive = from,
            endInclusive = to,
            requestingParties = Set(alice, bob, charlie),
            verbose = true,
          )
      )
    } yield {
      comparableTreeTxs(
        transactionTreesResult
      ) should contain theSameElementsInOrderAs comparableTreeTxs(treeTxs)
      comparableFlatTxs(
        flatTransactionsResult
      ) should contain theSameElementsInOrderAs comparableFlatTxs(flatTxs)
    }
  }

  private def lookupTreeTxsIndividually(
      transactions: Seq[LedgerEntry.Transaction],
      as: Set[Party],
  ): Future[Seq[TransactionTree]] =
    Future
      .sequence(
        transactions.map(tx =>
          ledgerDao.transactionsReader
            .lookupTransactionTreeById(tx.transactionId, as)
        )
      )
      .map(_.flatMap(_.toList.flatMap(_.transaction.toList)))

  private def flatTxsOf(
      source: Source[(Offset, GetTransactionsResponse), NotUsed]
  ): Future[Seq[Transaction]] =
    source
      .map(_._2)
      .runWith(Sink.seq)
      .map(_.flatMap(_.transactions))

  private def treeTxsOf(
      source: Source[(Offset, GetTransactionTreesResponse), NotUsed]
  ): Future[Seq[TransactionTree]] =
    source
      .map(_._2)
      .runWith(Sink.seq)
      .map(_.flatMap(_.transactions))

  // Ensure two sequences of transactions are comparable:
  // - witnesses do not have to appear in a specific order
  private def comparableFlatTxs(txs: Seq[Transaction]): Seq[Transaction] = {
    import com.daml.platform.api.v1.event.EventOps._
    txs.map(tx => tx.copy(events = tx.events.map(_.modifyWitnessParties(_.sorted))))
  }

  // Ensure two sequences of transaction trees are comparable:
  // - witnesses do not have to appear in a specific order
  private def comparableTreeTxs(txs: Seq[TransactionTree]): Seq[TransactionTree] = {
    import com.daml.platform.api.v1.event.EventOps._

    import scala.collection.compat._

    txs.map(tx =>
      tx.copy(eventsById = tx.eventsById.view.mapValues(_.modifyWitnessParties(_.sorted)).toMap)
    )
  }

  private def storeTestFixture(): Future[(Offset, Offset, Seq[LedgerEntry.Transaction])] = {
    val (offset1, t1) = singleCreate
    val offsetStep1 = nextOffsetStep(offset1)
    val (offset2, t2) = singleCreate
    val offsetStep2 = nextOffsetStep(offset2)
    val (offset3, t3) = singleExercise(nonTransient(t2).loneElement)
    val offsetStep3 = nextOffsetStep(offset3)
    val (offset4, t4) = fullyTransient
    val offsetStep4 = nextOffsetStep(offset4)
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- storeBatch(
        List(
          txEntry(offsetStep1, t1, Map.empty),
          txEntry(offsetStep2, t2, Map.empty),
          txEntry(offsetStep3, t3, Map.empty),
          txEntry(offsetStep4, t4, Map.empty),
        )
      )
      to <- ledgerDao.lookupLedgerEnd()
    } yield (from, to, Seq(t1, t2, t3, t4))
  }

  private def lookupFlatTxsIndividually(
      transactions: Seq[LedgerEntry.Transaction],
      as: Set[Party],
  ): Future[Seq[Transaction]] =
    Future
      .sequence(
        transactions.map(tx =>
          ledgerDao.transactionsReader
            .lookupFlatTransactionById(tx.transactionId, as)
        )
      )
      .map(_.flatMap(_.toList.flatMap(_.transaction.toList)))

  it should "return all events in the expected order (when batched)" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (offset1, create) = singleCreate
      offsetStep1 = nextOffsetStep(offset1)
      firstContractId = nonTransient(create).loneElement
      (offset2, exercise) = exerciseWithChild(firstContractId)
      offsetStep2 = nextOffsetStep(offset2)
      _ <- storeBatch(
        List(
          txEntry(offsetStep1, create, Map.empty),
          txEntry(offsetStep2, exercise, Map.empty),
        )
      )
      result <- ledgerDao.transactionsReader
        .getFlatTransactions(
          from,
          offset2,
          exercise.actAs.map(submitter => submitter -> Set.empty[Identifier]).toMap,
          verbose = true,
        )
        .runWith(Sink.seq)
    } yield {
      import com.daml.ledger.api.v1.event.Event
      import com.daml.ledger.api.v1.event.Event.Event.{Archived, Created}

      val txs = extractAllTransactions(result)

      inside(txs) { case Vector(tx1, tx2) =>
        tx1.transactionId shouldBe create.transactionId
        tx2.transactionId shouldBe exercise.transactionId
        inside(tx1.events) { case Seq(Event(Created(createdEvent))) =>
          createdEvent.contractId shouldBe firstContractId.coid
        }
        inside(tx2.events) { case Seq(Event(Archived(archivedEvent)), Event(Created(_))) =>
          archivedEvent.contractId shouldBe firstContractId.coid
        }
      }
    }
  }

  it should "serialize a batch of transactions with contracts that are archived within the same batch" in {
    val divulgedContractId = ContractId.assertFromString(s"#divulged-${UUID.randomUUID}")

    // This test writes a batch of transactions that create one regular and one divulged
    // contract that are archived within the same batch.
    for {
      from <- ledgerDao.lookupLedgerEnd()
      // TX1: create a single contract that will stay active
      templateId = testIdentifier("NonTransientContract")
      (offset1, tx1) = singleCreate(create(_, Set(alice), templateId), List(alice))
      offsetStep1 = nextOffsetStep(offset1)
      contractId = nonTransient(tx1).loneElement
      // TX2: create a single contract that will be archived within the same batch
      (offset2, tx2) = singleCreate
      offsetStep2 = nextOffsetStep(offset2)

      transientContractId = nonTransient(tx2).loneElement
      // TX3: archive contract created in TX2
      (offset3, tx3) = singleExercise(transientContractId)
      offsetStep3 = nextOffsetStep(offset3)

      // TX4: divulge a contract
      (offset4, tx4) = emptyTransaction(alice)
      offsetStep4 = nextOffsetStep(offset4)

      // TX5: archive previously divulged contract
      (offset5, tx5) = singleExercise(divulgedContractId)
      offsetStep5 = nextOffsetStep(offset5)
      _ <- storeBatch(
        List(
          txEntry(offsetStep1, tx1, Map.empty),
          txEntry(offsetStep2, tx2, Map.empty, Option.empty[BlindingInfo]),
          txEntry(offsetStep3, tx3, Map.empty, Option.empty[BlindingInfo]),
          txEntry(
            offsetStep4,
            tx4,
            Map((divulgedContractId, someVersionedContractInstance) -> Set(charlie)),
          ),
          txEntry(offsetStep5, tx5, Map.empty, Option.empty[BlindingInfo]),
        )
      )
      completions <- getCompletions(from, offset5, defaultAppId, Set(alice))
      contractLookup <- ledgerDao.lookupActiveOrDivulgedContract(contractId, Set(alice))
      transientLookup <- ledgerDao.lookupActiveOrDivulgedContract(transientContractId, Set(alice))
      divulgedLookup <- ledgerDao.lookupActiveOrDivulgedContract(divulgedContractId, Set(alice))
    } yield {
      completions should contain allOf (
        tx1.commandId.get -> ok,
        tx2.commandId.get -> ok,
        tx3.commandId.get -> ok,
        tx4.commandId.get -> ok,
        tx5.commandId.get -> ok,
      )
      contractLookup.value.template shouldBe templateId

      transientLookup shouldBe None
      divulgedLookup shouldBe None
    }
  }

  it should "allow idempotent transaction insertions" in {
    val key = "some-key"
    val (offset, tx) = txCreateContractWithKey(alice, key, Some("1337"))
    val maybeSubmitterInfo = submitterInfo(tx)
    val offsetStep = nextOffsetStep(offset)
    val preparedInsert = prepareInsert(maybeSubmitterInfo, tx, offsetStep)
    for {
      _ <- ledgerDao.storeTransactionEvents(preparedInsert)
      // Assume the indexer restarts after events insertion
      _ <- ledgerDao.storeTransactionEvents(preparedInsert)
      _ <- ledgerDao.storeTransactionState(preparedInsert)
      // Assume the indexer restarts after state insertion
      _ <- store(
        submitterInfo(tx),
        tx,
        offsetStep,
        List.empty,
        None,
      ) // The whole transaction insertion succeeds
      result <- ledgerDao.transactionsReader
        .lookupFlatTransactionById(tx.transactionId, tx.actAs.toSet)
    } yield {
      inside(result.value.transaction) { case Some(transaction) =>
        transaction.commandId shouldBe tx.commandId.get
        transaction.offset shouldBe ApiOffset.toApiString(offset)
        transaction.effectiveAt.value.seconds shouldBe tx.ledgerEffectiveTime.getEpochSecond
        transaction.effectiveAt.value.nanos shouldBe tx.ledgerEffectiveTime.getNano
        transaction.transactionId shouldBe tx.transactionId
        transaction.workflowId shouldBe tx.workflowId.getOrElse("")
        inside(transaction.events.loneElement.event.created) { case Some(created) =>
          val (nodeId, createNode: NodeCreate[ContractId]) =
            tx.transaction.nodes.head
          created.eventId shouldBe EventId(tx.transactionId, nodeId).toLedgerString
          created.witnessParties should contain only (tx.actAs: _*)
          created.agreementText.getOrElse("") shouldBe createNode.coinst.agreementText
          created.contractKey shouldNot be(None)
          created.createArguments shouldNot be(None)
          created.signatories should contain theSameElementsAs createNode.signatories
          created.observers should contain theSameElementsAs createNode.stakeholders.diff(
            createNode.signatories
          )
          created.templateId shouldNot be(None)
        }
      }
    }
  }

  it should "not retrieve a transaction with an offset past the ledger end (when requested by single party)" in {
    assertNoResponseFor(singleCreate)
  }

  it should "not retrieve a transaction with an offset past the ledger end (when requested by multiple parties)" in {
    assertNoResponseFor(multiPartySingleCreate)
  }

  private def txEntry(
      offsetStep: OffsetStep,
      tx: LedgerEntry.Transaction,
      divulgedContracts: Map[(ContractId, v1.ContractInst), Set[Party]],
      blindingInfo: Option[BlindingInfo] = None,
  ) =
    (
      (offsetStep, tx),
      divulgedContracts.keysIterator.map(c => v1.DivulgedContract(c._1, c._2)).toList,
      blindingInfo,
    )

  private def assertNoResponseFor(
      offsetTx: (Offset, LedgerEntry.Transaction)
  ): Future[Assertion] = {
    val (offset, tx) = offsetTx
    val maybeSubmitterInfo = submitterInfo(tx)
    val preparedInsert = prepareInsert(maybeSubmitterInfo, tx, nextOffsetStep(offset))
    for {
      _ <- ledgerDao.storeTransactionEvents(preparedInsert)
      _ <- ledgerDao.storeTransactionState(preparedInsert)
      transactionTreeResponse <- ledgerDao.transactionsReader.lookupTransactionTreeById(
        transactionId = tx.transactionId,
        tx.actAs.toSet,
      )
      transactionResponse <- ledgerDao.transactionsReader.lookupFlatTransactionById(
        transactionId = tx.transactionId,
        tx.actAs.toSet,
      )
    } yield {
      transactionTreeResponse shouldBe None
      transactionResponse shouldBe None
    }
  }

  private def extractAllTransactions(
      responses: Seq[(Offset, GetTransactionsResponse)]
  ): Vector[Transaction] =
    responses.foldLeft(Vector.empty[Transaction])((b, a) => b ++ a._2.transactions.toVector)
}
