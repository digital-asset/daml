// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import scala.collection.mutable

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.SubmittedTransaction
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  RelativeContractId,
  VersionedValue
}

/** Internal utilities to compute the inputs and effects of a DAML transaction */
private[kvutils] object InputsAndEffects {

  /** The effects of the transaction, that is what contracts
    * were consumed and created, and what contract keys were updated.
    */
  final case class Effects(
      /** The contracts consumed by this transaction.
        * When committing the transaction these contracts must be marked consumed.
        * A contract should be marked consumed when the transaction is committed,
        * regardless of the ledger effective time of the transaction (e.g. a transaction
        * with an earlier ledger effective time that gets committed later would find the
        * contract inactive).
        */
      consumedContracts: List[DamlStateKey],
      /** The contracts created by this transaction.
        * When the transaction is committed, keys marking the activeness of these
        * contracts should be created. The key should be a combination of the transaction
        * id and the relative contract id (that is, the node index).
        */
      createdContracts: List[(DamlStateKey, NodeCreate[ContractId, VersionedValue[ContractId]])],
      /** The contract keys created or updated as part of the transaction. */
      updatedContractKeys: Map[DamlStateKey, DamlContractKeyState]
  )
  object Effects {
    val empty = Effects(List.empty, List.empty, Map.empty)
  }

  /** Compute the inputs to a DAML transaction, that is, the referenced contracts, keys
    * and packages.
    */
  def computeInputs(tx: SubmittedTransaction): List[DamlStateKey] = {
    val inputs = mutable.LinkedHashSet[DamlStateKey]()

    {
      import PackageId.ordering
      inputs ++=
        tx.optUsedPackages
          .getOrElse(
            throw new InternalError("Transaction was not annotated with used packages")
          )
          .toList
          .sorted
          .map(DamlStateKey.newBuilder.setPackageId(_).build)
    }

    def addContractInput(coid: ContractId): Unit =
      coid match {
        case acoid: AbsoluteContractId =>
          inputs += absoluteContractIdToStateKey(acoid)
        case _ =>
      }

    def partyInputs(parties: Set[Party]): List[DamlStateKey] = {
      import Party.ordering
      parties.toList.sorted.map(partyStateKey)
    }

    tx.foreach {
      case (_, node) =>
        node match {
          case fetch: NodeFetch[ContractId] =>
            addContractInput(fetch.coid)

          case create: NodeCreate[ContractId, VersionedValue[ContractId]] =>
            create.key.foreach { keyWithMaintainers =>
              inputs += contractKeyToStateKey(
                GlobalKey(create.coinst.template, forceNoContractIds(keyWithMaintainers.key)))
            }

          case exe: NodeExercises[_, ContractId, _] =>
            addContractInput(exe.targetCoid)

          case lookup: NodeLookupByKey[ContractId, Transaction.Value[ContractId]] =>
            // We need both the contract key state and the contract state. The latter is used to verify
            // that the submitter can access the contract.
            lookup.result.foreach(addContractInput)
            inputs += contractKeyToStateKey(
              GlobalKey(lookup.templateId, forceNoContractIds(lookup.key.key)))
        }

        inputs ++= partyInputs(node.informeesOfNode)
    }

    inputs.toList
  }

  /** Compute the effects of a DAML transaction, that is, the created and consumed contracts. */
  def computeEffects(entryId: DamlLogEntryId, tx: SubmittedTransaction): Effects = {
    // TODO(JM): Skip transient contracts in createdContracts/updateContractKeys. E.g. rewrite this to
    // fold bottom up (with reversed roots!) and skip creates of archived contracts.
    tx.fold(Effects.empty) {
      case (effects, (nodeId, node)) =>
        node match {
          case fetch: NodeFetch[ContractId] =>
            effects
          case create: NodeCreate[ContractId, VersionedValue[ContractId]] =>
            effects.copy(
              createdContracts =
                relativeContractIdToStateKey(entryId, create.coid.asInstanceOf[RelativeContractId]) -> create
                  :: effects.createdContracts,
              updatedContractKeys = create.key
                .fold(effects.updatedContractKeys)(
                  keyWithMaintainers =>
                    effects.updatedContractKeys +
                      (contractKeyToStateKey(
                        GlobalKey(
                          create.coinst.template,
                          forceNoContractIds(keyWithMaintainers.key))) ->
                        DamlContractKeyState.newBuilder
                          .setContractId(encodeRelativeContractId(
                            entryId,
                            create.coid.asInstanceOf[RelativeContractId]))
                          .build))
            )

          case exe: NodeExercises[_, ContractId, VersionedValue[ContractId]] =>
            if (exe.consuming) {
              val stateKey = exe.targetCoid match {
                case acoid: AbsoluteContractId =>
                  absoluteContractIdToStateKey(acoid)
                case rcoid: RelativeContractId =>
                  relativeContractIdToStateKey(entryId, rcoid)
              }
              effects.copy(
                consumedContracts = stateKey :: effects.consumedContracts,
                updatedContractKeys = exe.key
                  .fold(effects.updatedContractKeys)(
                    key =>
                      effects.updatedContractKeys +
                        (contractKeyToStateKey(
                          GlobalKey(exe.templateId, forceNoContractIds(key.key))) ->
                          DamlContractKeyState.newBuilder.build)
                  )
              )
            } else {
              effects
            }
          case l: NodeLookupByKey[_, _] =>
            effects
        }
    }
  }
}
