// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.topology.DefaultTestIdentities.domainManager
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{
  Add,
  Positive,
  Remove,
  Replace,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class TopologyTransactionCollectionTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private lazy val uid1 = UniqueIdentifier.tryFromProtoPrimitive("da::tluafed")
  private lazy val uid2 = UniqueIdentifier.tryFromProtoPrimitive("da::chop")

  private lazy val factory: TestingOwnerWithKeys =
    new TestingOwnerWithKeys(domainManager, loggerFactory, parallelExecutionContext)

  private lazy val addStoredTx: StoredTopologyTransaction[Add] = {
    val tm = CantonTimestamp.now()
    StoredTopologyTransaction(
      SequencedTime(tm),
      EffectiveTime(tm),
      None,
      factory.mkAdd(IdentifierDelegation(uid1, factory.SigningKeys.key1)),
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private lazy val removeStoredTx: StoredTopologyTransaction[Remove] = {
    val tm = CantonTimestamp.now()
    val reversedTransaction = addStoredTx.transaction.transaction.reverse

    StoredTopologyTransaction(
      SequencedTime(tm),
      EffectiveTime(tm),
      None,
      addStoredTx.transaction
        .update(transaction = reversedTransaction)
        .asInstanceOf[SignedTopologyTransaction[Remove]],
    )
  }

  private def domainParametersChange(domainId: DomainId): StoredTopologyTransaction[Replace] = {
    val tm = CantonTimestamp.now()
    StoredTopologyTransaction(
      SequencedTime(tm),
      EffectiveTime(tm),
      None,
      factory.mkDmGov(
        DomainParametersChange(domainId, TestDomainParameters.defaultDynamic),
        factory.SigningKeys.key1,
      ),
    )
  }

  private lazy val replaceStoredTx1: StoredTopologyTransaction[Replace] = domainParametersChange(
    DomainId(uid1)
  )
  private lazy val replaceStoredTx2: StoredTopologyTransaction[Replace] = domainParametersChange(
    DomainId(uid2)
  )

  "StoredTopologyTransactions" should {
    lazy val simpleTransactionCollection = StoredTopologyTransactions(
      Seq(addStoredTx, removeStoredTx, replaceStoredTx1, replaceStoredTx2)
    )

    "collect for simple collection" in {
      simpleTransactionCollection.collectOfType[Add] shouldBe StoredTopologyTransactions(
        Seq(addStoredTx)
      )

      simpleTransactionCollection
        .collectOfType[Remove] shouldBe StoredTopologyTransactions(
        Seq(removeStoredTx)
      )

      simpleTransactionCollection
        .collectOfType[Replace] shouldBe StoredTopologyTransactions(
        Seq(replaceStoredTx1, replaceStoredTx2)
      )

      simpleTransactionCollection
        .collectOfType[Positive] shouldBe StoredTopologyTransactions(
        Seq(addStoredTx, replaceStoredTx1, replaceStoredTx2)
      )
    }

    "split simple collections" in {
      val expectedResult = (
        StoredTopologyTransactions(Seq(addStoredTx)),
        StoredTopologyTransactions(Seq(removeStoredTx)),
        StoredTopologyTransactions(Seq(replaceStoredTx1, replaceStoredTx2)),
      )

      simpleTransactionCollection.split shouldBe expectedResult
    }

    "retrieve positive transactions" in {
      val expectedResult = PositiveStoredTopologyTransactions(
        StoredTopologyTransactions(Seq(addStoredTx)),
        StoredTopologyTransactions(Seq(replaceStoredTx1, replaceStoredTx2)),
      )

      simpleTransactionCollection.positiveTransactions shouldBe expectedResult
    }

  }

  "SignedTopologyTransactions" should {
    val addSignedTx = addStoredTx.transaction
    val removeSignedTx = removeStoredTx.transaction
    val replaceSignedTx1 = replaceStoredTx1.transaction
    val replaceSignedTx2 = replaceStoredTx2.transaction

    lazy val simpleTransactionCollection = SignedTopologyTransactions(
      Seq(addSignedTx, removeSignedTx, replaceSignedTx1, replaceSignedTx2)
    )

    val isRemovePredicate =
      (tx: SignedTopologyTransaction[TopologyChangeOp]) =>
        tx.operation match {
          case _: Remove => true
          case _ => false
        }

    val isRemovePredicateF =
      (tx: SignedTopologyTransaction[TopologyChangeOp]) => Future.successful(isRemovePredicate(tx))

    "split simple collections" in {
      val expectedResult = (
        SignedTopologyTransactions(Seq(addSignedTx)),
        SignedTopologyTransactions(Seq(removeSignedTx)),
        SignedTopologyTransactions(Seq(replaceSignedTx1, replaceSignedTx2)),
      )

      simpleTransactionCollection.split shouldBe expectedResult
    }

    "filter simple collections" in {
      val expectedResult = SignedTopologyTransactions(Seq(removeSignedTx))

      simpleTransactionCollection.filter(isRemovePredicate) shouldBe expectedResult
      simpleTransactionCollection.filter(isRemovePredicateF).futureValue shouldBe expectedResult
    }
  }

}
