// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.ledger.javaapi.data.Identifier
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder.CreateKey
import com.digitalasset.daml.lf.transaction.test.{TestNodeBuilder, TransactionBuilder}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ComparesLfTransactions.{TxTree, buildLfTransaction}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.protocol.RollbackContext.RollbackScope
import com.digitalasset.canton.protocol.WellFormedTransaction.WithSuffixes
import com.digitalasset.canton.topology.{PartyId, UniqueIdentifier}
import com.digitalasset.canton.util.LfTransactionUtil
import com.digitalasset.canton.{
  BaseTest,
  ComparesLfTransactions,
  HasExecutionContext,
  LfValue,
  NeedsNewLfContractIds,
}
import org.scalatest.wordspec.AnyWordSpec

/** Tests WellFormedTransaction.merge particularly with respect to handling of top-level rollback nodes.
  */
class WellFormedTransactionMergeTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ComparesLfTransactions
    with NeedsNewLfContractIds {

  private val alice = PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"alice::party"))
  private val bob = PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"bob::party"))
  private val carol = PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"bob::party"))

  // Top-level lf transaction builder for "state-less" lf node creations.
  private implicit val tb: TestNodeBuilder = TestNodeBuilder

  import TransactionBuilder.Implicits.*

  private val subTxTree0 = TxTree(
    tb.fetch(create(newLfContractId(), iou.Iou.TEMPLATE_ID, alice, bob), byKey = false)
  )
  private val subTxTree1 = TxTree(create(newLfContractId(), iou.Iou.TEMPLATE_ID, alice, bob))
  private val contractCreate = create(newLfContractId(), iou.Iou.TEMPLATE_ID, alice, alice)
  private val subTxTree2 = Seq(
    TxTree(contractCreate),
    TxTree(tb.fetch(contractCreate, byKey = false)),
    TxTree(
      tb.exercise(
        contract = contractCreate,
        choice = "Call",
        consuming = true,
        actingParties = Set(alice.toLf),
        argument = LfValue.ValueUnit,
        byKey = false,
      ),
      TxTree(
        tb.rollback(),
        TxTree(
          create(
            newLfContractId(),
            iou.GetCash.TEMPLATE_ID,
            alice,
            alice,
            arg = args(
              LfValue.ValueParty(alice.toLf),
              LfValue.ValueParty(alice.toLf),
              args(LfValue.ValueNumeric(com.digitalasset.daml.lf.data.Numeric.assertFromString("0.0"))),
            ),
          )
        ),
      ),
    ),
  )
  private val subTxTree3 = TxTree(
    create(newLfContractId(), iou.Iou.TEMPLATE_ID, carol, alice, Seq(bob))
  )
  private val subTxTree4 = TxTree(
    tb.exercise(
      contract = create(newLfContractId(), iou.Iou.TEMPLATE_ID, bob, bob),
      choice = "Archive",
      consuming = true,
      actingParties = Set(bob.toLf),
      argument = LfValue.ValueUnit,
      byKey = false,
    )
  )

  "WellFormedTransaction.merge" should {
    import scala.language.implicitConversions
    implicit def toPositiveInt(i: Int): PositiveInt = PositiveInt.tryCreate(i)

    "wrap transactions under common rollback" when {
      "single transaction has multiple roots" in {
        val actual = merge(inputTransaction(Seq(1), Seq(subTxTree1) ++ subTxTree2: _*))
        val expected = expectedTransaction(TxTree(tb.rollback(), Seq(subTxTree1) ++ subTxTree2: _*))

        assertTransactionsMatch(expected, actual)
      }

      "two single-root transactions" in {
        val actual = merge(
          inputTransaction(Seq(1), subTxTree1),
          inputTransaction(Seq(1), subTxTree2*),
        )
        val expected = expectedTransaction(TxTree(tb.rollback(), Seq(subTxTree1) ++ subTxTree2: _*))

        assertTransactionsMatch(expected, actual)
      }

      "multilevel rollback scopes match" in {
        val levels = 5

        val actual = merge(
          inputTransaction((1 to levels).map(PositiveInt.tryCreate), subTxTree1),
          inputTransaction((1 to levels).map(PositiveInt.tryCreate), subTxTree2*),
        )
        val expected = expectedTransaction(
          (2 to levels).foldLeft(TxTree(tb.rollback(), Seq(subTxTree1) ++ subTxTree2: _*)) {
            case (a, _) => TxTree(tb.rollback(), a)
          }
        )

        assertTransactionsMatch(expected, actual)

      }
    }

    "not wrap transactions with differing rollbacks" when {
      "rollback followed by non-rollback" in {
        val actual = merge(
          inputTransaction(Seq(1), subTxTree1),
          inputTransaction(Seq.empty, subTxTree2*),
        )
        val expected = expectedTransaction(Seq(TxTree(tb.rollback(), subTxTree1)) ++ subTxTree2: _*)

        assertTransactionsMatch(expected, actual)
      }

      "non-rollback followed by rollback" in {
        val actual = merge(
          inputTransaction(Seq.empty, subTxTree1),
          inputTransaction(Seq(1), subTxTree2*),
        )
        val expected = expectedTransaction(subTxTree1, TxTree(tb.rollback(), subTxTree2*))

        assertTransactionsMatch(expected, actual)
      }

      "rollbacks separated by non-rollback" in {
        val actual = merge(
          inputTransaction(Seq(1), subTxTree1),
          inputTransaction(Seq.empty, subTxTree2*),
          inputTransaction(Seq(1), subTxTree3),
        )
        val expected = expectedTransaction(
          Seq(TxTree(tb.rollback(), subTxTree1)) ++
            subTxTree2 :+
            TxTree(tb.rollback(), subTxTree3): _*
        )

        assertTransactionsMatch(expected, actual)
      }

      "rollback have no common scope prefix" in {
        val actual = merge(
          inputTransaction(Seq(1), subTxTree1),
          inputTransaction(Seq(2), subTxTree2*),
          inputTransaction(Seq(3), subTxTree3),
        )
        val expected = expectedTransaction(
          TxTree(tb.rollback(), subTxTree1),
          TxTree(tb.rollback(), subTxTree2*),
          TxTree(tb.rollback(), subTxTree3),
        )

        assertTransactionsMatch(expected, actual)
      }
    }

    "partially wrap transactions with shared rollback scope prefix" when {
      "rollback nesting level increases" in {
        val actual = merge(
          inputTransaction(Seq.empty, subTxTree0),
          inputTransaction(Seq(1), subTxTree1),
          inputTransaction(Seq(1, 2), subTxTree2*),
          inputTransaction(Seq(1, 2, 3, 4), subTxTree3),
          inputTransaction(Seq.empty, subTxTree4),
        )
        val expected = expectedTransaction(
          subTxTree0,
          TxTree(
            tb.rollback(),
            subTxTree1,
            TxTree(
              tb.rollback(),
              subTxTree2 :+ TxTree(tb.rollback(), TxTree(tb.rollback(), subTxTree3)): _*
            ),
          ),
          subTxTree4,
        )

        assertTransactionsMatch(expected, actual)
      }

      "rollback nesting level decreases" in {
        val actual = merge(
          inputTransaction(Seq.empty, subTxTree0),
          inputTransaction(Seq(1, 2, 3, 4), subTxTree1),
          inputTransaction(Seq(1, 2, 3), subTxTree2*),
          inputTransaction(Seq(1), subTxTree3),
          inputTransaction(Seq.empty, subTxTree4),
        )
        val expected = expectedTransaction(
          subTxTree0,
          TxTree(
            tb.rollback(),
            TxTree(
              tb.rollback(),
              TxTree(tb.rollback(), Seq(TxTree(tb.rollback(), subTxTree1)) ++ subTxTree2: _*),
            ),
            subTxTree3,
          ),
          subTxTree4,
        )

        assertTransactionsMatch(expected, actual)
      }

      "rollback scope interrupted" in {
        val actual = merge(
          inputTransaction(Seq.empty, subTxTree0),
          inputTransaction(Seq(1, 2, 3), subTxTree1),
          // Interrupting a rollback context should never happen (given the pre-order traversal), but
          // this test documents how the current implementation would "reset" the tracking of rollback scopes.
          inputTransaction(
            Seq.empty,
            subTxTree2*
          ),
          inputTransaction(Seq(1, 2, 3), subTxTree3),
          inputTransaction(Seq.empty, subTxTree4),
        )
        val expected = expectedTransaction(
          Seq(subTxTree0) :+
            TxTree(tb.rollback(), TxTree(tb.rollback(), TxTree(tb.rollback(), subTxTree1))) :++
            subTxTree2 :+
            TxTree(tb.rollback(), TxTree(tb.rollback(), TxTree(tb.rollback(), subTxTree3))) :+
            subTxTree4: _*
        )

        assertTransactionsMatch(expected, actual)
      }
    }
  }

  private def transactionHelper[T](txTrees: TxTree*)(f: LfVersionedTransaction => T): T = f(
    buildLfTransaction(txTrees*)
  )

  private def inputTransaction(
      rbScope: RollbackScope,
      txTrees: TxTree*
  ): WithRollbackScope[WellFormedTransaction[WithSuffixes]] =
    transactionHelper(txTrees*)(lfTx =>
      WithRollbackScope(
        rbScope,
        WellFormedTransaction.normalizeAndAssert(
          lfTx,
          TransactionMetadata(
            CantonTimestamp.Epoch,
            CantonTimestamp.Epoch,
            lfTx.nodes.collect {
              case (nid, node) if LfTransactionUtil.nodeHasSeed(node) => nid -> hasher()
            },
          ),
          WithSuffixes,
        ),
      )
    )

  private def expectedTransaction(txTrees: TxTree*): LfVersionedTransaction = buildLfTransaction(
    txTrees*
  )

  private def merge(
      transactions: WithRollbackScope[WellFormedTransaction[WithSuffixes]]*
  ) = {
    valueOrFail(
      WellFormedTransaction.merge(
        NonEmpty.from(transactions).valueOrFail("Cannot merge empty list of transactions")
      )
    )("unexpectedly failed to merge").unwrap
  }

  private def create[T](
      cid: LfContractId,
      template: Identifier,
      payer: PartyId,
      owner: PartyId,
      viewers: Seq[PartyId] = Seq.empty,
      arg: LfValue = notUsed,
  )(implicit tb: TestNodeBuilder) = {
    val lfPayer = payer.toLf
    val lfOwner = owner.toLf
    val lfViewers = viewers.map(_.toLf)
    val lfObservers = Set(lfOwner) ++ lfViewers.toSet

    val lfTemplateId = templateIdFromIdentifier(template)

    tb.create(
      id = cid,
      templateId = lfTemplateId,
      argument = template match {
        case iou.Iou.TEMPLATE_ID =>
          require(
            arg == notUsed,
            "For IOUs, this function figures out the sig and obs parameters by itself",
          )
          args(
            LfValue.ValueParty(lfPayer),
            LfValue.ValueParty(lfOwner),
            args(LfValue.ValueNumeric(com.digitalasset.daml.lf.data.Numeric.assertFromString("0.0"))),
            valueList(lfObservers.map(LfValue.ValueParty)),
          )
        case _ => arg
      },
      signatories = Set(lfPayer),
      observers = lfObservers,
      key = CreateKey.NoKey,
    )
  }

}
