// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data.{BackStack, ImmArray}
import com.daml.lf.transaction.{GenTransaction, Node, NodeId}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

private[preprocessing] final class TransactionPreprocessor(
    compiledPackages: MutableCompiledPackages
) {

  import Preprocessor._

  val commandPreprocessor = new CommandPreprocessor(compiledPackages)

  @throws[PreprocessorException]
  private def unsafeAsValueWithNoContractIds(v: Value[Value.ContractId]): Value[Nothing] =
    v.ensureNoCid.fold(
      coid => fail(s"engine: found a contract ID $coid in the given value"),
      identity,
    )

  // Translate a GenNode into an expression re-interpretable by the interpreter
  @throws[PreprocessorException]
  def unsafeTranslateNode[Cid <: Value.ContractId](
      node: Node.GenNode[NodeId, Cid]
  ): (speedy.Command, Set[Value.ContractId]) = {

    node match {
      case _: Node.NodeRollback[_] =>
        // TODO https://github.com/digital-asset/daml/issues/8020
        // how on earth can we turn a rollback node back into a speedy command?
        sys.error("rollback nodes are not supported")
      case create: Node.NodeCreate[Cid] =>
        commandPreprocessor.unsafePreprocessCreate(create.templateId, create.arg)

      case exe: Node.NodeExercises[_, Cid] =>
        commandPreprocessor.unsafePreprocessExercise(
          exe.templateId,
          exe.targetCoid,
          exe.choiceId,
          exe.chosenValue,
        )
      case fetch: Node.NodeFetch[Cid] =>
        val cmd = commandPreprocessor.unsafePreprocessFetch(fetch.templateId, fetch.coid)
        (cmd, Set.empty)
      case lookup: Node.NodeLookupByKey[Cid] =>
        val keyValue = unsafeAsValueWithNoContractIds(lookup.key.key)
        val cmd = commandPreprocessor.unsafePreprocessLookupByKey(lookup.templateId, keyValue)
        (cmd, Set.empty)
    }
  }

  @throws[PreprocessorException]
  def unsafeTranslateTransactionRoots[Cid <: Value.ContractId](
      tx: GenTransaction[NodeId, Cid]
  ): (ImmArray[speedy.Command], Set[ContractId]) = {

    type Acc = (Set[Value.ContractId], Set[Value.ContractId], BackStack[speedy.Command])

    val (_, globalCids, cmds) =
      tx.roots.foldLeft[Acc]((Set.empty, Set.empty, BackStack.empty)) {
        case ((localCids0, globalCids0, stack), id) =>
          val (newGlobals, newLocals, cmd) = tx.nodes.get(id) match {
            case None =>
              fail(s"invalid transaction, root refers to non-existing node $id")
            case Some(node) =>
              node match {
                case create: Node.NodeCreate[Cid] =>
                  val (cmd, newCids) =
                    commandPreprocessor.unsafePreprocessCreate(create.templateId, create.arg)
                  val newGlobalCids = newCids.filterNot(localCids0)
                  (newGlobalCids, List(create.coid), cmd)
                case exe: Node.NodeExercises[_, Cid] =>
                  val templateId = exe.templateId
                  val (cmd, newCids) =
                    commandPreprocessor.unsafePreprocessExercise(
                      templateId,
                      exe.targetCoid,
                      exe.choiceId,
                      exe.chosenValue,
                    )
                  val newGlobalCids = newCids.filterNot(localCids0)
                  val newLocalCids = GenTransaction(tx.nodes, ImmArray(id)).localContracts.keys
                  (newGlobalCids, newLocalCids, cmd)

                case _: Node.NodeFetch[_] =>
                  fail(s"Transaction contains a fetch root node $id")
                case _: Node.NodeLookupByKey[_] =>
                  fail(s"Transaction contains a lookup by key root node $id")
                case _: Node.NodeRollback[_] =>
                  // TODO https://github.com/digital-asset/daml/issues/8020
                  // how on earth can we turn a rollback node back into a speedy command?
                  sys.error("rollback nodes are not supported")
              }
          }
          val globalCids = globalCids0 | newGlobals
          if (newLocals.exists(globalCids))
            fail("Conflicting discriminators between a global and local contract ID.")
          val localCids = localCids0 ++ newLocals
          (localCids, globalCids, stack :+ cmd)
      }

    cmds.toImmArray -> globalCids
  }

}
