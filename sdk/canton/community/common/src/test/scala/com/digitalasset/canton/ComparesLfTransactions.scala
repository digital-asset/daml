// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.ledger.javaapi.data.Identifier
import com.daml.lf.data.{FrontStack, ImmArray}
import com.daml.lf.transaction.NodeId
import com.daml.lf.transaction.test.NodeIdTransactionBuilder
import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{toIdentifier, toPackageId}
import com.digitalasset.canton.ComparesLfTransactions.TxTree
import com.digitalasset.canton.logging.pretty.PrettyTestInstances.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.*
import org.scalatest.{Assertion, Suite}

/** Test utility to compare actual and expected lf transactions using a human-readable, hierarchical serialization of lf
  * nodes.
  */
trait ComparesLfTransactions {

  this: Suite =>

  /** Main compare entry point of two lf transactions that asserts that the "nested" representations of lf transactions
    * match relying on pretty-printing to produce a human-readable, multiline and hierarchical serialization.
    */
  def assertTransactionsMatch(
      expectedTx: LfVersionedTransaction,
      actualTx: LfVersionedTransaction,
  ): Assertion = {

    // Nest transaction nodes and eliminate redundant node-id child references.
    def nestedLfNodeFromFlat(tx: LfVersionedTransaction): Seq[TxTree] = {
      def go(nid: LfNodeId): TxTree = tx.nodes(nid) match {
        case en: LfNodeExercises =>
          TxTree(en.copy(children = ImmArray.empty), en.children.toSeq.map(go): _*)
        case rn: LfNodeRollback =>
          TxTree(rn.copy(children = ImmArray.empty), rn.children.toSeq.map(go): _*)
        case leafNode: LfLeafOnlyActionNode => TxTree(leafNode)
      }
      tx.roots.toSeq.map(go)
    }

    // Compare the nested transaction structure, easier to reason about in case of diffs - also not sensitive to node-ids.
    val expectedNested = nestedLfNodeFromFlat(expectedTx)
    val actualNested = nestedLfNodeFromFlat(actualTx)

    assert(actualNested == expectedNested)
  }

  // Various helpers that help "hide" lf value boilerplate from transaction representations for improved readability.
  def args(values: LfValue*): LfValue.ValueRecord =
    LfValue.ValueRecord(None, values.map(None -> _).to(ImmArray))

  def seq(values: LfValue*): LfValue.ValueList = valueList(values)

  def valueList(values: IterableOnce[LfValue]): LfValue.ValueList =
    LfValue.ValueList(FrontStack.from(values))

  val notUsed: LfValue = LfValue.ValueUnit

  protected def templateIdFromIdentifier(
      identifier: Identifier
  ): LfInterfaceId =
    toIdentifier(s"${identifier.getModuleName}:${identifier.getEntityName}")(
      toPackageId(identifier.getPackageId)
    )

}

object ComparesLfTransactions {

  /** The TxTree class adds the ability to arrange LfTransaction nodes in a tree structure rather than the flat
    * node-id-based arrangement.
    */
  final case class TxTree(lfNode: LfNode, childNodes: TxTree*) extends PrettyPrinting {
    override lazy val pretty: Pretty[TxTree] = prettyOfClass(
      unnamedParam(_.lfNode),
      unnamedParamIfNonEmpty(_.childNodes),
    )

    def lfTransaction: LfVersionedTransaction = {
      buildLfTransaction(this)
    }
  }

  def buildLfTransaction(trees: TxTree*): LfVersionedTransaction = {
    val builder = new NodeIdTransactionBuilder

    def addChild(parentNid: NodeId)(child: TxTree): Unit = {
      val nid = builder.add(child.lfNode, parentNid)
      child.childNodes.foreach(addChild(nid))
    }

    trees.foreach { tree =>
      val nid = builder.add(tree.lfNode)
      tree.childNodes.foreach(addChild(nid))
    }

    builder.build()
  }

}
