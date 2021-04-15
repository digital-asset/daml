// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

import scala.collection.immutable.{TreeMap, TreeSet}

/** The effects of a transaction, that is:
  * @param consumedContracts
  *     The contracts consumed by this transaction.
  * @param createdContracts
  *     The contracts created by this transaction.
  * @param updatedContractKeys
  *     The contract keys created or updated as part of the transaction.
  */
final case class Effects(
    consumedContracts: TreeSet[ContractId],
    createdContracts: TreeMap[ContractId, Node.NodeCreate[ContractId]],
    updatedContractKeys: TreeMap[GlobalKey, Option[ContractId]],
)

/** utilities to compute the effects of a DAML transaction */
object Effects {

  val Empty = Effects(TreeSet.empty, TreeMap.empty, TreeMap.empty)

  /** Compute the effects of a DAML transaction, that is, the created and consumed contracts. */
  def computeEffects(tx: Transaction.Transaction): Effects = {
    // TODO(JM): Skip transient contracts in createdContracts/updateContractKeys. E.g. rewrite this to
    //  fold bottom up (with reversed roots!) and skip creates of archived contracts.
    tx.fold(Effects.Empty) { case (acc, (_, node)) =>
      node match {
        case create: Node.NodeCreate[Value.ContractId] =>
          val createdContracts = acc.createdContracts.updated(create.coid, create)
          val updatedContractKeys = create.key match {
            case Some(keyWithMaintainers) =>
              val globalKey = GlobalKey.assertBuild(create.templateId, keyWithMaintainers.key)
              acc.updatedContractKeys.updated(globalKey, Some(create))
            case None =>
              acc.updatedContractKeys
          }
          acc.copy(
            createdContracts = createdContracts,
            updatedContractKeys = updatedContractKeys,
          )

        case exe: Node.NodeExercises[NodeId, Value.ContractId] if exe.consuming =>
          val consumedContracts = acc.consumedContracts + exe.targetCoid
          val updatedContractKeys = exe.key match {
            case Some(keyWithMaintainers) =>
              val globalKey = GlobalKey.assertBuild(exe.templateId, keyWithMaintainers.key)
              acc.updatedContractKeys.updated(globalKey, None)
            case None =>
              acc.updatedContractKeys
          }
          acc.copy(
            consumedContracts = consumedContracts,
            updatedContractKeys = updatedContractKeys,
          )

        case _: Node.NodeLookupByKey[_] | _: Node.NodeLookupByKey[_] |
            _: Node.NodeExercises[_, _] =>
          acc

        case _: Node.NodeRollback[_] =>
          // TODO https://github.com/digital-asset/daml/issues/8020
          sys.error("rollback nodes are not supported")
      }
    }
  }

}
