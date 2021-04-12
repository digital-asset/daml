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

  private[this] case class Acc(
      globalCids: Set[ContractId],
      localCids: Set[ContractId],
      commands: BackStack[speedy.Command],
  ) {
    def update(
        newInputCids: Iterable[ContractId],
        newLocalCids: Iterable[ContractId],
        cmd: speedy.Command,
    ) = Acc(
      globalCids ++ newInputCids.filterNot(localCids),
      localCids ++ newLocalCids,
      commands :+ cmd,
    )
  }

  /*
   * Translate a transaction tree into a sequence of Speedy commands and collect the global CIDs.
   * A contract ID `cid` is considered *local* w.r.t. a node `n`, if either:
   *  - it is local in a previous node w.r.t. traversal order, or
   *  - `n` is a create node such that `n.coid == cid`
   * A contract ID `cid` is considered *global* in a root node `n`, if
   *  - `cid` is not considered local w.r.t. `n`, and
   *  - if `cid` is an input of a `n`, i.e. :
   *    * `n` is a create node and `cid` appear in the payload of the create contract (`n.arg`)
   *    * `n` is an exercise node and `cid` appear in the exercise argument (`n.choosenValue`)
   *    * `n` is an exercise node and `cid` is the ID of the exercise contract (`n.targetCoid`)
   * A contract ID is *global* w.r.t a transaction `tx` if it is global w.r.t one of the root `tx`.
   */
  @throws[PreprocessorException]
  def unsafeTranslateTransactionRoots[Cid <: Value.ContractId](
      tx: GenTransaction[NodeId, Cid]
  ): (ImmArray[speedy.Command], Set[ContractId]) = {

    val result = tx.roots.foldLeft(Acc(Set.empty, Set.empty, BackStack.empty)) { (acc, id) =>
      tx.nodes.get(id) match {
        case None =>
          fail(s"invalid transaction, root refers to non-existing node $id")
        case Some(node) =>
          node match {
            case create: Node.NodeCreate[Cid] =>
              val (cmd, newCids) =
                commandPreprocessor.unsafePreprocessCreate(create.templateId, create.arg)
              acc.update(newCids, List(create.coid), cmd)
            case exe: Node.NodeExercises[_, Cid] =>
              val (cmd, newCids) = commandPreprocessor.unsafePreprocessExercise(
                exe.templateId,
                exe.targetCoid,
                exe.choiceId,
                exe.chosenValue,
              )
              val newLocalCids = GenTransaction(tx.nodes, ImmArray(id)).localContracts.keys
              acc.update(newCids, newLocalCids, cmd)
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
    }

    // The following check ensures that `localCids ∩ globalCids = ∅`.
    // It is probably not 100% necessary, as the reinterpretation should catch the cases where it is not true.
    // We still prefer to perform it here as:
    //  - it is cheap,
    //  - it catches obviously buggy transaction,
    //  - it is easier to reason about "soundness" of preprocessing under the disjointness assumption.
    if (result.localCids exists result.globalCids)
      fail("Conflicting discriminators between a global and local contract ID.")

    result.commands.toImmArray -> result.globalCids
  }

}
