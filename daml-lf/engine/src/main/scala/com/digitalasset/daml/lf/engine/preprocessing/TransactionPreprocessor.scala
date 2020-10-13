// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data.{BackStack, ImmArray}
import com.daml.lf.transaction.{GenTransaction, Node, NodeId}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

private[preprocessing] final class TransactionPreprocessor(
    compiledPackages: MutableCompiledPackages) {

  import Preprocessor._

  val commandPreprocessor = new CommandPreprocessor(compiledPackages)

  @throws[PreprocessorException]
  private def unsafeAsValueWithNoContractIds(v: Value[Value.ContractId]): Value[Nothing] =
    v.ensureNoCid.fold(
      coid => fail(s"engine: found a contract ID $coid in the given value"),
      identity
    )

  // Translate a GenNode into an expression re-interpretable by the interpreter
  @throws[PreprocessorException]
  def unsafeTranslateNode[Cid <: Value.ContractId](
      acc: (Set[Value.ContractId], Set[Value.ContractId]),
      node: Node.GenNode.WithTxValue[NodeId, Cid]
  ): (speedy.Command, (Set[Value.ContractId], Set[Value.ContractId])) = {

    val (localCids, globalCids) = acc

    node match {
      case Node.NodeCreate(coid @ _, coinst, optLoc @ _, sigs @ _, stks @ _, key @ _) =>
        val identifier = coinst.template
        val (cmd, argCids) =
          commandPreprocessor.unsafePreprocessCreate(identifier, coinst.arg.value)
        val newGlobalCids = globalCids | argCids.filterNot(localCids)
        if (newGlobalCids(coid))
          fail("Conflicting discriminators between a global and local contract ID.")

        cmd -> (localCids + coid -> newGlobalCids)

      case Node.NodeExercises(
          coid,
          template,
          choice,
          optLoc @ _,
          consuming @ _,
          actingParties @ _,
          chosenVal,
          stakeholders @ _,
          signatories @ _,
          controllersDifferFromActors @ _,
          children @ _,
          exerciseResult @ _,
          key @ _) =>
        val templateId = template
        val (cmd, argCids) =
          commandPreprocessor.unsafePreprocessExercise(templateId, coid, choice, chosenVal.value)
        (cmd, (localCids, globalCids | argCids.filterNot(localCids)))
      case Node.NodeFetch(coid, templateId, _, _, _, _, _) =>
        val cmd = commandPreprocessor.unsafePreprocessFetch(templateId, coid)
        (cmd, (localCids, if (localCids(coid)) globalCids else globalCids + coid))
      case Node.NodeLookupByKey(templateId, _, key, _) =>
        val keyValue = unsafeAsValueWithNoContractIds(key.key.value)
        val cmd = commandPreprocessor.unsafePreprocessLookupByKey(templateId, keyValue)
        (cmd, acc)
    }
  }

  @throws[PreprocessorException]
  def unsafeTranslateTransactionRoots[Cid <: Value.ContractId](
      tx: GenTransaction.WithTxValue[NodeId, Cid],
  ): (ImmArray[speedy.Command], Set[ContractId]) = {

    type Acc = ((Set[Value.ContractId], Set[Value.ContractId]), BackStack[speedy.Command])

    val ((_, globalCids), cmds) =
      tx.roots.foldLeft[Acc](((Set.empty, Set.empty), BackStack.empty)) {
        case ((cids, stack), id) =>
          tx.nodes.get(id) match {
            case None =>
              fail(s"invalid transaction, root refers to non-existing node $id")
            case Some(node) =>
              node match {
                case Node.NodeFetch(_, _, _, _, _, _, _) =>
                  fail(s"Transaction contains a fetch root node $id")
                case Node.NodeLookupByKey(_, _, _, _) =>
                  fail(s"Transaction contains a lookup by key root node $id")
                case _ =>
                  val (cmd, acc) = unsafeTranslateNode(cids, node)
                  (acc, stack :+ cmd)
              }
          }
      }

    cmds.toImmArray -> globalCids
  }

}
