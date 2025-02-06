// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.TransactionViewDecomposition.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.RollbackContext.{RollbackScope, RollbackSibling}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.util.LfTransactionUtil
import com.digitalasset.canton.{
  BaseTest,
  ComparesLfTransactions,
  HasExecutionContext,
  LfPartyId,
  LfValue,
  NeedsNewLfContractIds,
}
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder.CreateKey
import com.digitalasset.daml.lf.transaction.test.TreeTransactionBuilder.NodeWrapper
import com.digitalasset.daml.lf.transaction.test.{
  TestIdFactory,
  TestNodeBuilder,
  TreeTransactionBuilder,
}
import org.scalatest.wordspec.AnyWordSpec

class TransactionViewDecompositionTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ComparesLfTransactions
    with NeedsNewLfContractIds {

  lazy val factory: TransactionViewDecompositionFactory.type = TransactionViewDecompositionFactory
  s"With factory ${factory.getClass.getSimpleName}" when {

    val exampleTransactionFactory = new ExampleTransactionFactory()()

    val examples =
      exampleTransactionFactory.standardHappyCases
    examples foreach { example =>
      s"decomposing $example into views" must {
        "yield the correct views" in {
          factory
            .fromTransaction(
              exampleTransactionFactory.topologySnapshot,
              example.wellFormedUnsuffixedTransaction,
              RollbackContext.empty,
              Some(ExampleTransactionFactory.submitter),
            )
            .futureValueUS
            .toList shouldEqual example.rootViewDecompositions.toList
        }
      }
    }
  }

  "A view decomposition" when {
    import ExampleTransactionFactory.*

    "there are lots of top-level nodes" can {
      "be constructed without stack overflow" in {
        val flatTransactionSize = 10000

        val decomposition = timeouts.default.await("Decomposing test transaction")(
          TransactionViewDecompositionFactory
            .fromTransaction(
              defaultTopologySnapshot,
              wftWithCreateNodes(flatTransactionSize, signatory, observer),
              RollbackContext.empty,
              None,
            )
            .failOnShutdown
        )

        decomposition.size shouldBe flatTransactionSize
      }
    }

    "a transaction with nested rollbacks" can {

      import RollbackDecomposition.*
      import com.digitalasset.daml.lf.transaction.test.TreeTransactionBuilder.*

      object tif extends TestIdFactory

      val alice: LfPartyId = signatory
      val bob: LfPartyId = observer
      val carol: LfPartyId = extra

      val embeddedRollbackExample: LfVersionedTransaction = toVersionedTransaction(
        exerciseNode(tif.newCid, signatories = Set(alice)).withChildren(
          exerciseNode(tif.newCid, signatories = Set(alice)).withChildren(
            TestNodeBuilder
              .rollback()
              .withChildren(
                exerciseNode(tif.newCid, signatories = Set(alice), observers = Set(carol))
              )
          ),
          exerciseNode(tif.newCid, signatories = Set(alice), observers = Set(bob)),
        )
      )

      val expected = List(
        RbNewTree(
          rbScope(PositiveInt.one),
          Set(alice),
          List[RollbackDecomposition](
            RbSameTree(rbScope(PositiveInt.one)),
            RbNewTree(rbScope(PositiveInt.one, PositiveInt.one), Set(alice, carol)),
            RbNewTree(rbScope(PositiveInt.two), Set(alice, bob)),
          ),
        )
      )

      "does not re-used rollback contexts" in {

        val decomposition = TransactionViewDecompositionFactory
          .fromTransaction(
            defaultTopologySnapshot,
            toWellFormedUnsuffixedTransaction(embeddedRollbackExample),
            RollbackContext.empty,
            None,
          )
          .futureValueUS

        val actual = RollbackDecomposition.rollbackDecomposition(decomposition)

        actual shouldBe expected
      }
    }

    "new view counting" can {
      object tif extends TestIdFactory
      val node = exerciseNode(tif.newCid, signatories = Set.empty)
      val sameView = SameView(node, LfNodeId(0), RollbackContext.empty)
      var nextThreshold: NonNegativeInt = NonNegativeInt.zero
      def newView(children: TransactionViewDecomposition*): NewView = {
        // Trick: Use unique thresholds to get around NewView nesting check
        // that requires informees or thresholds to differ.
        nextThreshold = nextThreshold + NonNegativeInt.one
        NewView(
          node,
          ViewConfirmationParameters.tryCreate(
            Set.empty,
            Seq(Quorum(Map.empty, nextThreshold)),
          ),
          None,
          LfNodeId(0),
          children,
          RollbackContext.empty,
        )
      }

      "deal with empty transactions" in {
        TransactionViewDecomposition.countNestedViews(Seq.empty) shouldBe 0
      }

      "count single view" in {
        TransactionViewDecomposition.countNestedViews(Seq(newView())) shouldBe 1
      }

      "not count same view" in {
        TransactionViewDecomposition.countNestedViews(Seq(newView(sameView))) shouldBe 1
      }

      "count multiple sibling views" in {
        TransactionViewDecomposition.countNestedViews(
          Seq(newView(newView(), sameView, newView(), sameView, newView()))
        ) shouldBe 4
      }

      "count nested views" in {
        TransactionViewDecomposition.countNestedViews(
          Seq(newView(newView(newView(newView())), sameView, newView(newView(), newView())))
        ) shouldBe 7
      }

    }

  }

  private def wftWithCreateNodes(
      size: Int,
      signatory: LfPartyId,
      observer: LfPartyId,
  ): WellFormedTransaction[WithoutSuffixes] = {
    val alice = signatory
    val bob = observer

    val tx = TreeTransactionBuilder.toVersionedTransaction(
      (0 until size)
        .map[NodeWrapper] { _ =>
          TestNodeBuilder.create(
            id = newLfContractIdUnsuffixed(),
            templateId = ExampleTransactionFactory.templateId,
            argument = args(
              LfValue.ValueParty(alice),
              LfValue.ValueParty(bob),
              args(notUsed),
              seq(LfValue.ValueParty(bob)),
            ),
            signatories = Set(alice),
            observers = Set(bob),
            key = CreateKey.NoKey,
          )
        }*
    )

    toWellFormedUnsuffixedTransaction(tx)

  }

  private def toWellFormedUnsuffixedTransaction(
      tx: LfVersionedTransaction
  ): WellFormedTransaction[WithoutSuffixes] =
    WellFormedTransaction
      .normalizeAndCheck(
        tx,
        TransactionMetadata(
          CantonTimestamp.Epoch,
          CantonTimestamp.Epoch,
          tx.nodes.collect {
            case (nid, node) if LfTransactionUtil.nodeHasSeed(node) => nid -> hasher()
          },
        ),
        WithoutSuffixes,
      )
      .value

}

sealed trait RollbackDecomposition
object RollbackDecomposition {

  final case class RbNewTree(
      rb: RollbackScope,
      informees: Set[LfPartyId],
      children: Seq[RollbackDecomposition] = Seq.empty,
  ) extends RollbackDecomposition

  final case class RbSameTree(rb: RollbackScope) extends RollbackDecomposition

  /** The purpose of this method is to map a tree [[TransactionViewDecomposition]] onto a [[RollbackDecomposition]]
    * hierarchy aid comparison. The [[RollbackContext.nextChild]] value is significant but is not available
    * for inspection or construction. For this reason we use trick of entering a rollback context and then converting
    * to a rollback scope that has as its last sibling the nextChild value.
    */
  def rollbackDecomposition(
      decompositions: Seq[TransactionViewDecomposition]
  ): List[RollbackDecomposition] =
    decompositions
      .map[RollbackDecomposition] {
        case view: NewView =>
          RbNewTree(
            view.rbContext.enterRollback.rollbackScope.toList,
            view.viewConfirmationParameters.informees,
            rollbackDecomposition(view.tailNodes),
          )
        case view: SameView =>
          RbSameTree(view.rbContext.enterRollback.rollbackScope.toList)
      }
      .toList

  def rbScope(rollbackScope: RollbackSibling*): RollbackScope = rollbackScope.toList

}
