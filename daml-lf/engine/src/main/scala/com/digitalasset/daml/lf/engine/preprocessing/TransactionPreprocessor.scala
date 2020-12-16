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
      node: Node.GenNode[NodeId, Cid]
  ): (speedy.Command, (Set[Value.ContractId], Set[Value.ContractId])) = {

    val (localCids, globalCids) = acc

    node match {
      case Node.NodeCreate(
          coid @ _,
          coinst,
          optLoc @ _,
          sigs @ _,
          stks @ _,
          key @ _,
          version @ _) =>
        val identifier = coinst.template
        if (globalCids(coid))
          fail("Conflicting discriminators between a global and local contract ID.")

        val (cmd, newCids) =
          commandPreprocessor.unsafePreprocessCreate(identifier, coinst.arg)
        val newGlobalCids = globalCids + coid
        val newLocalCids = localCids | newCids.filterNot(globalCids)
        cmd -> (newLocalCids -> newGlobalCids)

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
          choiceObservers @ _,
          children @ _,
          exerciseResult @ _,
          key @ _,
          byKey @ _,
          version @ _,
          ) =>
        val templateId = template
        val (cmd, newCids) =
          commandPreprocessor.unsafePreprocessExercise(templateId, coid, choice, chosenVal)
        (cmd, (localCids | newCids.filterNot(globalCids), globalCids))
      case Node.NodeFetch(coid, templateId, _, _, _, _, _, _, _) =>
        val cmd = commandPreprocessor.unsafePreprocessFetch(templateId, coid)
        (cmd, acc)
      case Node.NodeLookupByKey(templateId, _, key, _, _) =>
        val keyValue = unsafeAsValueWithNoContractIds(key.key)
        val cmd = commandPreprocessor.unsafePreprocessLookupByKey(templateId, keyValue)
        (cmd, acc)
    }
  }

  @throws[PreprocessorException]
  def unsafeTranslateTransactionRoots[Cid <: Value.ContractId](
      tx: GenTransaction[NodeId, Cid],
  ): (ImmArray[speedy.Command], Set[ContractId]) = {

    type Acc = ((Set[Value.ContractId], Set[Value.ContractId]), BackStack[speedy.Command])

    val ((localCids, _), cmds) =
      tx.roots.foldLeft[Acc](((Set.empty, Set.empty), BackStack.empty)) {
        case ((cids, stack), id) =>
          tx.nodes.get(id) match {
            case None =>
              fail(s"invalid transaction, root refers to non-existing node $id")
            case Some(node) =>
              node match {
                case _: Node.NodeFetch[_] =>
                  fail(s"Transaction contains a fetch root node $id")
                case _: Node.NodeLookupByKey[_] =>
                  fail(s"Transaction contains a lookup by key root node $id")
                case _ =>
                  val (cmd, acc) = unsafeTranslateNode(cids, node)
                  (acc, stack :+ cmd)
              }
          }
      }

    cmds.toImmArray -> localCids
  }

}
