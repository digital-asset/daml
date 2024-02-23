// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data.{BackStack, ImmArray}
import com.daml.lf.transaction.{Node, NodeId, SubmittedTransaction}

private[preprocessing] final class TransactionPreprocessor(
    commandPreprocessor: CommandPreprocessor
) {

  private[this] def invalidRootNode(nodeId: NodeId, message: String) =
    throw Error.Preprocessing.RootNode(nodeId, message)

  /*
   * Translates a transaction tree into a sequence of Speedy commands
   * and collects the global contract IDs.
   * A contract ID `cid` is considered *local* w.r.t. a node `n`, if
   * either:
   *  - it is local in any node appearing previously (w.r.t. traversal
   *    order) in the transaction, or
   *  - `n` is a create node such that `n.coid == cid`
   *
   * A contract ID `cid` is considered *global* in a root node `n`,
   * if:
   *  - `cid` is not considered local w.r.t. `n`, and
   *  - if `cid` is reference in the input fields of a `n`, i.e. :
   *    - `n` is a create node and `cid` appears in the payload of the
   *      create contract (`n.arg`), or
   *    - `n` is an exercise node and `cid` is the ID of the exercise
   *      contract (`n.targetCoid`), or
   *    - `n` is an exercise node and `cid` appears in the exercise
   *      argument (`n.choosenValue`).
   *
   * A contract ID is considered *global* w.r.t. a transaction `tx` if
   * it is global w.r.t. one of the roots of `tx`.
   *
   * Note that it is, in general, not possible to recover from a
   * transaction, the original sequence of commands that generated this
   * transaction. In particular:
   *  - we cannot distinguish a exercise performed "by ID" from an
   *    exercise performed "by key" (as of LF v1.13).
   *  - we cannot distinguish a createAndExercise from a create
   *    followed by an exercise.
   *
   * Consequently the sequence of commands and the set of global
   * contract IDs generated by this method may be different from the
   * original sequence of commands. In particular:
   * - all exercises are translated into exercise by ID.
   * - a cid is not considered global if there exists a create node
   *   within the transaction that creates a contract with the same ID.
   *
   * Under the assumption that the underlying ledger guarantees the
   * uniqueness of all contract IDs (including transient contracts),
   * the reinterpretation of the generated transaction will succeed
   * iff the original submission was valid and succeeded.
   *
   * See review comments in https://github.com/digital-asset/daml/pull/9370
   * for more details.
   */
  @throws[Error.Preprocessing.Error]
  def unsafeTranslateTransactionRoots(
      tx: SubmittedTransaction
  ): ImmArray[speedy.Command] = {

    val result = tx.roots.foldLeft(BackStack.empty[speedy.Command]) { (acc, id) =>
      tx.nodes.get(id) match {
        case Some(node: Node.Action) =>
          node match {
            case create: Node.Create =>
              acc :+ commandPreprocessor.unsafePreprocessCreate(
                create.templateId,
                create.arg,
                strict = true,
              )
            case exe: Node.Exercise =>
              val cmd = exe.keyOpt match {
                case Some(key) if exe.byKey =>
                  commandPreprocessor.unsafePreprocessExerciseByKey(
                    exe.templateId,
                    key.globalKey.key,
                    exe.choiceId,
                    exe.chosenValue,
                    strict = true,
                  )
                case _ =>
                  commandPreprocessor.unsafePreprocessExerciseTemplate(
                    exe.templateId,
                    exe.targetCoid,
                    exe.choiceId,
                    exe.chosenValue,
                    strict = true,
                  )
              }
              acc :+ cmd
            case _: Node.Fetch =>
              invalidRootNode(id, s"Transaction contains a fetch root node $id")
            case _: Node.LookupByKey =>
              invalidRootNode(id, s"Transaction contains a lookup by key root node $id")
          }
        case Some(_: Node.Rollback) =>
          invalidRootNode(id, s"invalid transaction, root refers to a rollback node $id")
        case None =>
          invalidRootNode(id, s"invalid transaction, root refers to non-existing node $id")
      }
    }

    result.toImmArray
  }

}
