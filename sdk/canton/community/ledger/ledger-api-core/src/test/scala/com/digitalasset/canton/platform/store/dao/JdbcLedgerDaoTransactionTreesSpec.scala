// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v2.transaction.TransactionTree
import com.daml.ledger.api.v2.update_service.GetUpdateTreesResponse
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.UseOriginalViewPackageId
import com.digitalasset.canton.platform.store.entries.LedgerEntry
import com.digitalasset.canton.platform.store.utils.EventOps.TreeEventOps
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.transaction.Node
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.*
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

private[dao] trait JdbcLedgerDaoTransactionTreesSpec
    extends OptionValues
    with Inside
    with LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (lookupTransactionTreeById, lookupTransactionTreeByOffset)"

  it should "return nothing for a mismatching transaction id" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.updateReader
        .lookupTransactionTreeById(
          updateId = "WRONG",
          tx.actAs.toSet,
          EventProjectionProperties(
            verbose = true,
            templateWildcardWitnesses = Some(tx.actAs.toSet),
          )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
        )
    } yield {
      result shouldBe None
    }
  }

  it should "return nothing for a mismatching offset" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.updateReader
        .lookupTransactionTreeByOffset(
          offset = Offset.tryFromLong(12345678L),
          tx.actAs.toSet,
          EventProjectionProperties(
            verbose = true,
            templateWildcardWitnesses = Some(tx.actAs.toSet),
          )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
        )
    } yield {
      result shouldBe None
    }
  }

  it should "return nothing for a mismatching party" in {
    for {
      (offset, tx) <- store(singleCreate)
      resultById <- ledgerDao.updateReader
        .lookupTransactionTreeById(
          tx.updateId,
          Set("WRONG"),
          EventProjectionProperties(
            verbose = true,
            templateWildcardWitnesses = Some(Set("WRONG")),
          )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
        )
      resultByOffset <- ledgerDao.updateReader
        .lookupTransactionTreeByOffset(
          offset,
          Set("WRONG"),
          EventProjectionProperties(
            verbose = true,
            templateWildcardWitnesses = Some(Set("WRONG")),
          )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
        )
    } yield {
      resultById shouldBe None
      resultByOffset shouldBe resultById
    }
  }

  it should "return the expected transaction tree for a correct request (create)" in {
    for {
      (offset, tx) <- store(singleCreate)
      resultById <- ledgerDao.updateReader
        .lookupTransactionTreeById(
          tx.updateId,
          tx.actAs.toSet,
          EventProjectionProperties(
            verbose = true,
            templateWildcardWitnesses = Some(tx.actAs.toSet),
          )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
        )
      resultByOffset <- ledgerDao.updateReader
        .lookupTransactionTreeByOffset(
          offset,
          tx.actAs.toSet,
          EventProjectionProperties(
            verbose = true,
            templateWildcardWitnesses = Some(tx.actAs.toSet),
          )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
        )
    } yield {
      inside(resultById.value.transaction) { case Some(transaction) =>
        inside(tx.transaction.nodes.headOption) { case Some((nodeId, createNode: Node.Create)) =>
          transaction.commandId shouldBe tx.commandId.value
          transaction.offset shouldBe offset.unwrap
          TimestampConversion.toLf(
            transaction.effectiveAt.value,
            TimestampConversion.ConversionMode.Exact,
          ) shouldBe tx.ledgerEffectiveTime
          transaction.updateId shouldBe tx.updateId
          transaction.workflowId shouldBe tx.workflowId.getOrElse("")
          val created = transaction.eventsById.values.loneElement.getCreated
          created.offset shouldBe offset.unwrap
          created.nodeId shouldBe nodeId.index
          created.witnessParties should contain only (tx.actAs*)
          created.contractKey shouldBe None
          created.createArguments shouldNot be(None)
          created.signatories should contain theSameElementsAs createNode.signatories
          created.observers should contain theSameElementsAs createNode.stakeholders.diff(
            createNode.signatories
          )
          created.templateId shouldNot be(None)
        }
      }
      resultByOffset shouldBe resultById
    }
  }

  it should "return the expected transaction tree for a correct request (exercise)" in {
    for {
      (_, create) <- store(singleCreate)
      (offset, exercise) <- store(singleExercise(nonTransient(create).loneElement))
      resultById <- ledgerDao.updateReader
        .lookupTransactionTreeById(
          exercise.updateId,
          exercise.actAs.toSet,
          EventProjectionProperties(
            verbose = true,
            templateWildcardWitnesses = Some(exercise.actAs.toSet),
          )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
        )
      resultByOffset <- ledgerDao.updateReader
        .lookupTransactionTreeByOffset(
          offset,
          exercise.actAs.toSet,
          EventProjectionProperties(
            verbose = true,
            templateWildcardWitnesses = Some(exercise.actAs.toSet),
          )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
        )
    } yield {
      inside(resultById.value.transaction) { case Some(transaction) =>
        inside(exercise.transaction.nodes.headOption) {
          case Some((nodeId, exerciseNode: Node.Exercise)) =>
            transaction.commandId shouldBe exercise.commandId.value
            transaction.offset shouldBe offset.unwrap
            TimestampConversion.toLf(
              transaction.effectiveAt.value,
              TimestampConversion.ConversionMode.Exact,
            ) shouldBe exercise.ledgerEffectiveTime
            transaction.updateId shouldBe exercise.updateId
            transaction.workflowId shouldBe exercise.workflowId.getOrElse("")
            val exercised = transaction.eventsById.values.loneElement.getExercised
            exercised.offset shouldBe offset.unwrap
            exercised.nodeId shouldBe nodeId.index
            exercised.witnessParties should contain only (exercise.actAs*)
            exercised.contractId shouldBe exerciseNode.targetCoid.coid
            exercised.templateId shouldNot be(None)
            exercised.actingParties should contain theSameElementsAs exerciseNode.actingParties
            exercised.lastDescendantNodeId shouldBe nodeId.index
            exercised.choice shouldBe exerciseNode.choiceId
            exercised.choiceArgument shouldNot be(None)
            exercised.consuming shouldBe true
            exercised.exerciseResult shouldNot be(None)
        }
      }
      resultByOffset shouldBe resultById
    }
  }

  it should "return the expected transaction tree for a correct request (create, exercise)" in {
    for {
      (offset, tx) <- store(fullyTransient())
      resultById <- ledgerDao.updateReader
        .lookupTransactionTreeById(
          tx.updateId,
          tx.actAs.toSet,
          EventProjectionProperties(
            verbose = true,
            templateWildcardWitnesses = Some(tx.actAs.toSet),
          )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
        )
      resultByOffset <- ledgerDao.updateReader
        .lookupTransactionTreeByOffset(
          offset,
          tx.actAs.toSet,
          EventProjectionProperties(
            verbose = true,
            templateWildcardWitnesses = Some(tx.actAs.toSet),
          )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
        )
    } yield {
      inside(resultById.value.transaction) { case Some(transaction) =>
        val (createNodeId, createNode) =
          tx.transaction.nodes.collectFirst { case (nodeId, node: Node.Create) =>
            nodeId -> node
          }.value
        val (exerciseNodeId, exerciseNode) =
          tx.transaction.nodes.collectFirst { case (nodeId, node: Node.Exercise) =>
            nodeId -> node
          }.value

        transaction.commandId shouldBe tx.commandId.value
        transaction.offset shouldBe offset.unwrap
        transaction.updateId shouldBe tx.updateId
        transaction.workflowId shouldBe tx.workflowId.getOrElse("")
        TimestampConversion.toLf(
          transaction.effectiveAt.value,
          TimestampConversion.ConversionMode.Exact,
        ) shouldBe tx.ledgerEffectiveTime

        val created = transaction
          .eventsById(createNodeId.index)
          .getCreated
        val exercised = transaction
          .eventsById(exerciseNodeId.index)
          .getExercised

        created.offset shouldBe offset.unwrap
        created.nodeId shouldBe createNodeId.index
        created.witnessParties should contain only (tx.actAs*)
        created.contractKey shouldBe None
        created.createArguments shouldNot be(None)
        created.signatories should contain theSameElementsAs createNode.signatories
        created.observers should contain theSameElementsAs createNode.stakeholders.diff(
          createNode.signatories
        )
        created.templateId shouldNot be(None)

        exercised.offset shouldBe offset.unwrap
        exercised.nodeId shouldBe exerciseNodeId.index
        exercised.witnessParties should contain only (tx.actAs*)
        exercised.contractId shouldBe exerciseNode.targetCoid.coid
        exercised.templateId shouldNot be(None)
        exercised.actingParties should contain theSameElementsAs exerciseNode.actingParties
        exercised.lastDescendantNodeId shouldBe exerciseNodeId.index
        exercised.choice shouldBe exerciseNode.choiceId
        exercised.choiceArgument shouldNot be(None)
        exercised.consuming shouldBe true
        exercised.exerciseResult shouldNot be(None)
      }
      resultByOffset shouldBe resultById
    }
  }

  it should "return a transaction tree with the expected shape for a partially visible transaction" in {
    for {
      (offset, tx) <- store(partiallyVisible)
      resultById <- ledgerDao.updateReader
        .lookupTransactionTreeById(
          tx.updateId,
          Set(alice),
          EventProjectionProperties(
            verbose = true,
            templateWildcardWitnesses = Some(Set(alice)),
          )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
        ) // only two children are visible to Alice
      resultByOffset <- ledgerDao.updateReader
        .lookupTransactionTreeByOffset(
          offset,
          Set(alice),
          EventProjectionProperties(
            verbose = true,
            templateWildcardWitnesses = Some(Set(alice)),
          )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
        )
    } yield {
      inside(resultById.value.transaction) { case Some(transaction) =>
        transaction.eventsById should have size 2
      }
      resultByOffset shouldBe resultById
    }
  }

  behavior of "JdbcLedgerDao (getTransactionTrees)"

  it should "match the results of lookupTransactionTreeById" in {
    for {
      (from, to, transactions) <- storeTestFixture()
      lookups <- lookupIndividually(transactions, Set(alice, bob, charlie))
      result <- transactionsOf(
        ledgerDao.updateReader
          .getTransactionTrees(
            startInclusive = from,
            endInclusive = to,
            requestingParties = Some(Set(alice, bob, charlie)),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              templateWildcardWitnesses = Some(Set(alice, bob, charlie)),
            )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
          )
      )
    } yield {
      comparable(result) should contain theSameElementsInOrderAs comparable(lookups)
    }
  }

  it should "work correctly for party-wildcard" in {
    for {
      (from, to, _) <- storeTestFixture()
      result <- transactionsOf(
        ledgerDao.updateReader
          .getTransactionTrees(
            startInclusive = from,
            endInclusive = to,
            requestingParties = Some(Set(alice, bob, charlie)),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              templateWildcardWitnesses = Some(Set(alice, bob, charlie)),
            )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
          )
      )
      resultPartyWildcard <- transactionsOf(
        ledgerDao.updateReader
          .getTransactionTrees(
            startInclusive = from,
            endInclusive = to,
            requestingParties = None,
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              templateWildcardWitnesses = None,
            )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
          )
      )
    } yield {
      comparable(result) should contain theSameElementsInOrderAs comparable(resultPartyWildcard)
    }
  }

  it should "filter correctly by party" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, tx) <- store(
        multipleCreates(
          charlie,
          Seq(
            (alice, someTemplateId, someContractArgument),
            (bob, someTemplateId, someContractArgument),
          ),
        )
      )
      to <- ledgerDao.lookupLedgerEnd()
      individualLookupForAlice <- lookupIndividually(Seq(tx), as = Set(alice))
      individualLookupForBob <- lookupIndividually(Seq(tx), as = Set(bob))
      individualLookupForCharlie <- lookupIndividually(Seq(tx), as = Set(charlie))
      resultForAlice <- transactionsOf(
        ledgerDao.updateReader
          .getTransactionTrees(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            requestingParties = Some(Set(alice)),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              templateWildcardWitnesses = Some(Set(alice)),
            )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
          )
      )
      resultForBob <- transactionsOf(
        ledgerDao.updateReader
          .getTransactionTrees(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            requestingParties = Some(Set(bob)),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              templateWildcardWitnesses = Some(Set(bob)),
            )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
          )
      )
      resultForCharlie <- transactionsOf(
        ledgerDao.updateReader
          .getTransactionTrees(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            requestingParties = Some(Set(charlie)),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              templateWildcardWitnesses = Some(Set(charlie)),
            )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
          )
      )
    } yield {
      individualLookupForAlice should contain theSameElementsInOrderAs resultForAlice
      individualLookupForBob should contain theSameElementsInOrderAs resultForBob
      individualLookupForCharlie should contain theSameElementsInOrderAs resultForCharlie
    }
  }

  private def storeTestFixture(): Future[(Offset, Offset, Seq[LedgerEntry.Transaction])] =
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, t1) <- store(singleCreate)
      (_, t2) <- store(singleCreate)
      (_, t3) <- store(singleExercise(nonTransient(t2).loneElement))
      (_, t4) <- store(fullyTransient())
      to <- ledgerDao.lookupLedgerEnd()
    } yield (
      from.fold(Offset.firstOffset)(_.lastOffset.increment),
      to.value.lastOffset,
      Seq(t1, t2, t3, t4),
    )

  private def lookupIndividually(
      transactions: Seq[LedgerEntry.Transaction],
      as: Set[Ref.Party],
  ): Future[Seq[TransactionTree]] =
    Future
      .sequence(
        transactions.map(tx =>
          ledgerDao.updateReader
            .lookupTransactionTreeById(
              tx.updateId,
              as,
              EventProjectionProperties(
                verbose = true,
                templateWildcardWitnesses = Some(as.map(_.toString)),
              )(interfaceViewPackageUpgrade = UseOriginalViewPackageId),
            )
        )
      )
      .map(_.flatMap(_.toList.flatMap(_.transaction.toList)))

  private def transactionsOf(
      source: Source[(Offset, GetUpdateTreesResponse), NotUsed]
  ): Future[Seq[TransactionTree]] =
    source
      .map(_._2)
      .runWith(Sink.seq)
      .map(_.flatMap(_.update match {
        case GetUpdateTreesResponse.Update.TransactionTree(txTree) => Seq(txTree)
        case _ => Nil
      }))

  // Ensure two sequences of transaction trees are comparable:
  // - witnesses do not have to appear in a specific order
  private def comparable(txs: Seq[TransactionTree]): Seq[TransactionTree] =
    txs.map(tx =>
      tx.copy(eventsById = tx.eventsById.view.mapValues(_.modifyWitnessParties(_.sorted)).toMap)
    )
}
