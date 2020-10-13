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
      case create: Node.NodeCreate[_, _] =>
        val identifier = create.coinst.template
        if (globalCids(create.coid))
          fail("Conflicting discriminators between a global and local contract ID.")

        val (cmd, newCids) =
          commandPreprocessor.unsafePreprocessCreate(identifier, create.coinst.arg.value)
        val newGlobalCids = globalCids + create.coid
        val newLocalCids = localCids | newCids.filterNot(globalCids)
        cmd -> (newLocalCids -> newGlobalCids)
      case exerciseByKey: Node.NodeExercises[_, _, _] if exerciseByKey.byKey.getOrElse(false) =>
        val templateId = exerciseByKey.templateId
        val (cmd, newCids) =
          commandPreprocessor
            .unsafePreprocessExerciseByKey(
              templateId,
              exerciseByKey.key.fold(fail("unexpected exerciseByKey without key."))(_.key.value),
              exerciseByKey.choiceId,
              exerciseByKey.chosenValue.value,
            )
        (cmd, (localCids | newCids.filterNot(globalCids), globalCids))

      case exercise: Node.NodeExercises[_, _, _] =>
        val templateId = exercise.templateId
        val (cmd, newCids) =
          commandPreprocessor
            .unsafePreprocessExercise(
              templateId,
              exercise.targetCoid,
              exercise.choiceId,
              exercise.chosenValue.value,
            )
        (cmd, (localCids | newCids.filterNot(globalCids), globalCids))
      case fetchByKey: Node.NodeFetch[_, _] if fetchByKey.byKey.getOrElse(false) =>
        val (cmd, newCids) = commandPreprocessor.unsafePreprocessFetchByKey(
          fetchByKey.templateId,
          fetchByKey.key.fold(fail("unexpected exerciseByKey without key."))(_.key.value))
        (cmd, (localCids | newCids.filterNot(globalCids), globalCids))
      case fetch: Node.NodeFetch[_, _] =>
        val cmd = commandPreprocessor.unsafePreprocessFetch(fetch.templateId, fetch.coid)
        (cmd, acc)
      case lookup: Node.NodeLookupByKey[_, _] =>
        val keyValue = unsafeAsValueWithNoContractIds(lookup.key.key.value)
        val cmd = commandPreprocessor.unsafePreprocessLookupByKey(lookup.templateId, keyValue)
        (cmd, acc)
    }
  }

  @throws[PreprocessorException]
  def unsafeTranslateTransactionRoots[Cid <: Value.ContractId](
      tx: GenTransaction.WithTxValue[NodeId, Cid],
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
                case Node.NodeFetch(_, _, _, _, _, _, _, _) =>
                  fail(s"Transaction contains a fetch root node $id")
                case Node.NodeLookupByKey(_, _, _, _) =>
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
