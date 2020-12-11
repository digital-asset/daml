// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.TransactionMeta
import com.daml.lf.data.Ref._
import com.daml.lf.transaction.{GlobalKey, Node, NodeId, Transaction}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

import scala.collection.mutable

/** Internal utilities to compute the inputs and effects of a DAML transaction */
private[kvutils] object InputsAndEffects {

  /** The effects of the transaction, that is what contracts
    * were consumed and created, and what contract keys were updated.
    */
  /**
    *
    * @param consumedContracts
    *     The contracts consumed by this transaction.
    *     When committing the transaction these contracts must be marked consumed.
    *     A contract should be marked consumed when the transaction is committed,
    *     regardless of the ledger effective time of the transaction (e.g. a transaction
    *     with an earlier ledger effective time that gets committed later would find the
    *     contract inactive).
    * @param createdContracts
    *     The contracts created by this transaction.
    *     When the transaction is committed, keys marking the activeness of these
    *     contracts should be created. The key should be a combination of the transaction
    *     id and the relative contract id (that is, the node index).
    * @param updatedContractKeys
    *     The contract keys created or updated as part of the transaction.
    */
  final case class Effects(
      consumedContracts: List[DamlStateKey],
      createdContracts: List[(DamlStateKey, Node.NodeCreate[ContractId])],
      updatedContractKeys: Map[DamlStateKey, Option[ContractId]]
  )

  object Effects {
    val empty = Effects(List.empty, List.empty, Map.empty)
  }

  /** Compute the inputs to a DAML transaction, that is, the referenced contracts, keys
    * and packages.
    */
  def computeInputs(
      tx: Transaction.Transaction,
      meta: TransactionMeta,
  ): List[DamlStateKey] = {
    val inputs = mutable.LinkedHashSet[DamlStateKey]()

    {
      import PackageId.ordering
      inputs ++=
        meta.optUsedPackages
          .getOrElse(
            throw new InternalError("Transaction was not annotated with used packages")
          )
          .toList
          .sorted
          .map(DamlStateKey.newBuilder.setPackageId(_).build)
    }

    val localContract = tx.localContracts

    def addContractInput(coid: ContractId): Unit =
      if (!localContract.isDefinedAt(coid))
        inputs += contractIdToStateKey(coid)

    def partyInputs(parties: Set[Party]): List[DamlStateKey] = {
      import Party.ordering
      parties.toList.sorted.map(partyStateKey)
    }

    tx.foreach {
      case (_, node) =>
        node match {
          case fetch: Node.NodeFetch[Value.ContractId] =>
            addContractInput(fetch.coid)
            fetch.key.foreach { keyWithMaintainers =>
              inputs += globalKeyToStateKey(
                GlobalKey(fetch.templateId, forceNoContractIds(keyWithMaintainers.key)))
            }

          case create: Node.NodeCreate[Value.ContractId] =>
            create.key.foreach { keyWithMaintainers =>
              inputs += globalKeyToStateKey(
                GlobalKey(create.coinst.template, forceNoContractIds(keyWithMaintainers.key)))
            }

          case exe: Node.NodeExercises[NodeId, Value.ContractId] =>
            addContractInput(exe.targetCoid)

          case lookup: Node.NodeLookupByKey[Value.ContractId] =>
            // We need both the contract key state and the contract state. The latter is used to verify
            // that the submitter can access the contract.
            lookup.result.foreach(addContractInput)
            inputs += globalKeyToStateKey(
              GlobalKey(lookup.templateId, forceNoContractIds(lookup.key.key)))
        }

        inputs ++= partyInputs(node.informeesOfNode)
    }

    inputs.toList
  }

  /** Compute the effects of a DAML transaction, that is, the created and consumed contracts. */
  def computeEffects(tx: Transaction.Transaction): Effects = {
    // TODO(JM): Skip transient contracts in createdContracts/updateContractKeys. E.g. rewrite this to
    // fold bottom up (with reversed roots!) and skip creates of archived contracts.
    tx.fold(Effects.empty) {
      case (effects, (_, node)) =>
        node match {
          case _: Node.NodeFetch[Value.ContractId] =>
            effects
          case create: Node.NodeCreate[Value.ContractId] =>
            effects.copy(
              createdContracts = contractIdToStateKey(create.coid) -> create :: effects.createdContracts,
              updatedContractKeys = create.key
                .fold(effects.updatedContractKeys)(
                  keyWithMaintainers =>
                    effects.updatedContractKeys.updated(
                      globalKeyToStateKey(
                        GlobalKey
                          .build(
                            create.coinst.template,
                            keyWithMaintainers.key
                          )
                          .fold(
                            _ =>
                              throw Err.InvalidSubmission(
                                "Contract IDs are not supported in contract keys."),
                            identity)),
                      Some(create.coid)
                  )
                )
            )

          case exe: Node.NodeExercises[NodeId, Value.ContractId] =>
            if (exe.consuming) {
              effects.copy(
                consumedContracts = contractIdToStateKey(exe.targetCoid) :: effects.consumedContracts,
                updatedContractKeys = exe.key
                  .fold(effects.updatedContractKeys)(
                    key =>
                      effects.updatedContractKeys.updated(
                        globalKeyToStateKey(GlobalKey(exe.templateId, forceNoContractIds(key.key))),
                        None)
                  )
              )
            } else {
              effects
            }
          case _: Node.NodeLookupByKey[Value.ContractId] =>
            effects
        }
    }
  }
}
