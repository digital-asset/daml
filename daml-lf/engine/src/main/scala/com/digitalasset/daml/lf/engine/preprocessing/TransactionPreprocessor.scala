// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data.ImmArray
import com.daml.lf.transaction.{GenTransaction, Node, Transaction}
import com.daml.lf.value.Value

private[preprocessing] final class TransactionPreprocessor(
    compiledPackages: MutableCompiledPackages) {

  import Preprocessor._

  val commandPreprocessor = new CommandPreprocessor(compiledPackages)

  // A cast of a value to a value which uses only absolute contract IDs.
  // In particular, the cast will succeed for all values contained in the root nodes of a Transaction produced by submit
  @throws[PreprocessorException]
  private def unsafeAsValueWithAbsoluteContractIds(
      v: Value[Value.ContractId]
  ): Value[Value.AbsoluteContractId] =
    v.ensureNoRelCid
      .fold(rcoid => fail(s"unexpected relative contract id $rcoid"), identity)

  @throws[PreprocessorException]
  private def unsafeAsAbsoluteContractId(coid: Value.ContractId): Value.AbsoluteContractId =
    coid match {
      case rcoid: Value.RelativeContractId =>
        fail(s"not an absolute contract ID: $rcoid")
      case acoid: Value.AbsoluteContractId =>
        acoid
    }

  @throws[PreprocessorException]
  private def unsafeAsValueWithNoContractIds(v: Value[Value.ContractId]): Value[Nothing] =
    v.ensureNoCid.fold(
      coid => fail(s"engine: found a contract ID $coid in the given value"),
      identity
    )

  // Translate a GenNode into an expression re-interpretable by the interpreter
  @throws[PreprocessorException]
  def unsafeTranslateNode[Cid <: Value.ContractId](
      node: Node.GenNode.WithTxValue[Transaction.NodeId, Cid],
  ): speedy.Command = {

    node match {
      case Node.NodeCreate(
          nodeSeed @ _,
          coid @ _,
          coinst,
          optLoc @ _,
          sigs @ _,
          stks @ _,
          key @ _) =>
        val identifier = coinst.template
        val arg = unsafeAsValueWithAbsoluteContractIds(coinst.arg.value)
        commandPreprocessor.unsafePreprocessCreate(identifier, arg)

      case Node.NodeExercises(
          nodeSeed @ _,
          coid,
          template,
          choice,
          optLoc @ _,
          consuming @ _,
          actingParties @ _,
          chosenVal,
          stakeholders @ _,
          signatories @ _,
          controllers @ _,
          children @ _,
          exerciseResult @ _,
          key @ _) =>
        val templateId = template
        val arg = unsafeAsValueWithAbsoluteContractIds(chosenVal.value)
        commandPreprocessor.unsafePreprocessExercise(templateId, coid, choice, arg)
      case Node.NodeFetch(coid, templateId, _, _, _, _, _) =>
        val acoid = unsafeAsAbsoluteContractId(coid)
        commandPreprocessor.unsafePreprocessFetch(templateId, acoid)

      case Node.NodeLookupByKey(templateId, _, key, _) =>
        val keyValue = unsafeAsValueWithNoContractIds(key.key.value)
        commandPreprocessor.unsafePreprocessLookupByKey(templateId, keyValue)
    }
  }

  @throws[PreprocessorException]
  def unsafeTranslateTransactionRoots[Cid <: Value.ContractId](
      tx: GenTransaction.WithTxValue[Transaction.NodeId, Cid],
  ): ImmArray[(Transaction.NodeId, speedy.Command)] =
    tx.roots.map(id =>
      tx.nodes.get(id) match {
        case None =>
          fail(s"invalid transaction, root refers to non-existing node $id")
        case Some(node) =>
          node match {
            case Node.NodeFetch(_, _, _, _, _, _, _) =>
              fail(s"Transaction contains a fetch root node $id")
            case _ =>
              id -> unsafeTranslateNode(node)
          }
    })

}
