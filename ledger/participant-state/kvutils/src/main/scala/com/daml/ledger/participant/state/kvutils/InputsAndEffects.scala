// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import scala.collection.mutable
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId, VersionedValue}

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
      createdContracts: List[
        (DamlStateKey, NodeCreate[AbsoluteContractId, VersionedValue[AbsoluteContractId]])],
      /** The contract keys created or updated as part of the transaction. */
      updatedContractKeys: Map[DamlStateKey, Option[AbsoluteContractId]]
  )
  object Effects {
    val empty = Effects(List.empty, List.empty, Map.empty)
  }

  /** Compute the inputs to a DAML transaction, that is, the referenced contracts, keys
    * and packages.
    */
  def computeInputs(tx: Transaction.AbsTransaction): List[DamlStateKey] = {
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

    val localContract = tx.localContracts

    def addContractInput(coid: ContractId): Unit =
      coid match {
        case acoid: AbsoluteContractId if (!localContract.isDefinedAt(acoid)) =>
          inputs += contractIdToStateKey(acoid)
        case _ =>
      }

    def partyInputs(parties: Set[Party]): List[DamlStateKey] = {
      import Party.ordering
      parties.toList.sorted.map(partyStateKey)
    }

    tx.foreach {
      case (_, node) =>
        node match {
          case fetch @ NodeFetch(_, _, _, _, _, _) =>
            addContractInput(fetch.coid)

          case create @ NodeCreate(_, _, _, _, _, _, _) =>
            create.key.foreach { keyWithMaintainers =>
              inputs += globalKeyToStateKey(
                GlobalKey(create.coinst.template, forceNoContractIds(keyWithMaintainers.key.value)))
            }

          case exe @ NodeExercises(_, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            addContractInput(exe.targetCoid)

          case lookup @ NodeLookupByKey(_, _, _, _) =>
            // We need both the contract key state and the contract state. The latter is used to verify
            // that the submitter can access the contract.
            lookup.result.foreach(addContractInput)
            inputs += globalKeyToStateKey(
              GlobalKey(lookup.templateId, forceNoContractIds(lookup.key.key.value)))
        }

        inputs ++= partyInputs(node.informeesOfNode)
    }

    inputs.toList
  }

  /** Compute the effects of a DAML transaction, that is, the created and consumed contracts. */
  def computeEffects(entryId: DamlLogEntryId, tx: Transaction.AbsTransaction): Effects = {
    // TODO(JM): Skip transient contracts in createdContracts/updateContractKeys. E.g. rewrite this to
    // fold bottom up (with reversed roots!) and skip creates of archived contracts.
    tx.fold(Effects.empty) {
      case (effects, (nodeId, node)) =>
        node match {
          case fetch @ NodeFetch(_, _, _, _, _, _) =>
            effects
          case create @ NodeCreate(_, _, _, _, _, _, _) =>
            effects.copy(
              createdContracts = contractIdToStateKey(create.coid) -> create :: effects.createdContracts,
              updatedContractKeys = create.key
                .fold(effects.updatedContractKeys)(
                  keyWithMaintainers =>
                    effects.updatedContractKeys.updated(
                      (globalKeyToStateKey(
                        GlobalKey(
                          create.coinst.template,
                          // FIXME: We probably should not crash here.
                          keyWithMaintainers.key.value.assertNoCid(_ =>
                            "Unexpected contract id in contract key")
                        ))),
                      Some(create.coid)
                  )
                )
            )

          case exe @ NodeExercises(_, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
            if (exe.consuming) {
              effects.copy(
                consumedContracts = contractIdToStateKey(exe.targetCoid) :: effects.consumedContracts,
                updatedContractKeys = exe.key
                  .fold(effects.updatedContractKeys)(
                    key =>
                      effects.updatedContractKeys.updated(
                        (globalKeyToStateKey(
                          GlobalKey(exe.templateId, forceNoContractIds(key.key.value)))),
                        None)
                  )
              )
            } else {
              effects
            }
          case l @ NodeLookupByKey(_, _, _, _) =>
            effects
        }
    }
  }
}
