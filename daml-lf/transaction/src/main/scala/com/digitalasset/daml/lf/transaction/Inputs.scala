// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.data.Ref
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

import scala.collection.immutable.TreeSet

/** The inputs of the transaction, that is:
  *   - the contracts were fetched
  *   - all the parties involved
  *   - all the contract keys creted, consumed, fetch, or looked up.
  */
/** @param contracts
  *     The contracts fetched by this transaction.
  *
  * @param parties
  *     The contracts created by this transaction.
  *     When the transaction is committed, keys marking the activeness of these
  *     contracts should be created. The key should be a combination of the transaction
  *     id and the relative contract id (that is, the node index).
  * @param keys
  *     The contract keys created or updated as part of the transaction.
  */
case class Inputs(
    contracts: TreeSet[ContractId],
    parties: TreeSet[Ref.Party],
    keys: TreeSet[GlobalKey],
)

object Inputs {

  /** Compute the inputs to a DAML transaction (in traversal order), that is, the referenced contracts, parties,
    *  and key.
    */
  def collectInputs(
      tx: Transaction.Transaction,
      addContract: ContractId => Unit,
      addParty: Ref.Party => Unit,
      addGlobalKey: GlobalKey => Unit,
  ): Unit = {

    val localContract = tx.localContracts

    def addContractInput(coid: ContractId): Unit =
      if (!localContract.isDefinedAt(coid))
        addContract(coid)

    def addPartyInputs(parties: Set[Ref.Party]): Unit =
      parties.toList.sorted(Ref.Party.ordering).foreach(addParty)

    def addContractKey(
        tmplId: Ref.Identifier,
        key: Node.KeyWithMaintainers[Value[ContractId]],
    ): Unit =
      addGlobalKey(GlobalKey.assertBuild(tmplId, key.key))

    tx.foreach { case (_, node) =>
      node match {
        case _: Node.NodeRollback[_] =>
          // TODO https://github.com/digital-asset/daml/issues/8020
          sys.error("rollback nodes are not supported")
        case fetch: Node.NodeFetch[Value.ContractId] =>
          addContractInput(fetch.coid)
          fetch.key.foreach(addContractKey(fetch.templateId, _))

        case create: Node.NodeCreate[Value.ContractId] =>
          create.key.foreach(addContractKey(create.templateId, _))

        case exe: Node.NodeExercises[NodeId, Value.ContractId] =>
          addContractInput(exe.targetCoid)
          exe.key.foreach(addContractKey(exe.templateId, _))

        case lookup: Node.NodeLookupByKey[Value.ContractId] =>
          // We need both the contract key state and the contract state. The latter is used to verify
          // that the submitter can access the contract.
          lookup.result.foreach(addContractInput)
          addContractKey(lookup.templateId, lookup.key)
      }

      addPartyInputs(node.informeesOfNode)
    }
  }

  def computeInputs(tx: Transaction.Transaction): Inputs = {
    val contractIds = TreeSet.newBuilder[ContractId](ContractId.`Cid Order`.toScalaOrdering)
    val parties = TreeSet.newBuilder[Ref.Party](Ref.Party.ordering)
    val globalKeys = TreeSet.newBuilder[GlobalKey]
    collectInputs(tx, contractIds.+=, parties.+=, globalKeys.+=)
    Inputs(
      contractIds.result(),
      parties.result(),
      globalKeys.result(),
    )
  }
}
