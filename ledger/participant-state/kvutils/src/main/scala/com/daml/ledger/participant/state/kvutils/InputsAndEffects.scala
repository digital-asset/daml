// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.Conversions.{
  absoluteContractIdToLogEntryId,
  absoluteContractIdToStateKey,
  relativeContractIdToStateKey
}
import com.daml.ledger.participant.state.v1.SubmittedTransaction
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.transaction.Node.{
  NodeCreate,
  NodeExercises,
  NodeFetch,
  NodeLookupByKey
}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VContractId, RelativeContractId}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._

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
      createdContracts: List[DamlStateKey]
  )

  /** Compute the inputs to a DAML transaction, that is, the referenced contracts
    * and packages.
    */
  def computeInputs(tx: SubmittedTransaction): (List[DamlLogEntryId], List[DamlStateKey]) = {
    val packageInputs: List[DamlStateKey] = tx.usedPackages.map { pkgId =>
      DamlStateKey.newBuilder.setPackageId(pkgId).build
    }.toList

    def addLogEntryInput(inputs: List[DamlLogEntryId], coid: VContractId): List[DamlLogEntryId] =
      coid match {
        case acoid: AbsoluteContractId =>
          absoluteContractIdToLogEntryId(acoid)._1 :: inputs
        case _ =>
          inputs
      }
    def addStateInput(inputs: List[DamlStateKey], coid: VContractId): List[DamlStateKey] =
      coid match {
        case acoid: AbsoluteContractId =>
          absoluteContractIdToStateKey(acoid) :: inputs
        case _ =>
          inputs
      }

    tx.fold(GenTransaction.TopDown, (List.empty[DamlLogEntryId], packageInputs)) {
      case ((logEntryInputs, stateInputs), (nodeId, node)) =>
        node match {
          case fetch: NodeFetch[VContractId] =>
            (
              addLogEntryInput(logEntryInputs, fetch.coid),
              addStateInput(stateInputs, fetch.coid)
            )
          case create: NodeCreate[_, _] =>
            (logEntryInputs, stateInputs)
          case exe: NodeExercises[_, VContractId, _] =>
            (
              addLogEntryInput(logEntryInputs, exe.targetCoid),
              addStateInput(stateInputs, exe.targetCoid)
            )
          case l: NodeLookupByKey[_, _] =>
            sys.error("computeInputs: NodeLookupByKey not implemented!")
        }
    }
  }

  /** Compute the effects of a DAML transaction, that is, the created and consumed contracts. */
  def computeEffects(entryId: DamlLogEntryId, tx: SubmittedTransaction): Effects = {
    tx.fold(GenTransaction.TopDown, Effects(List.empty, List.empty)) {
      case (effects, (nodeId, node)) =>
        node match {
          case fetch: NodeFetch[VContractId] =>
            effects
          case create: NodeCreate[_, _] =>
            // FIXME(JM): Track created keys
            effects.copy(
              createdContracts =
                relativeContractIdToStateKey(entryId, create.coid.asInstanceOf[RelativeContractId])
                  :: effects.createdContracts
            )
          case exe: NodeExercises[_, VContractId, _] =>
            if (exe.consuming) {
              exe.targetCoid match {
                case acoid: AbsoluteContractId =>
                  effects.copy(
                    consumedContracts = absoluteContractIdToStateKey(acoid) :: effects.consumedContracts
                  )
                case _ =>
                  effects
              }
            } else {
              effects
            }
          case l: NodeLookupByKey[_, _] =>
            sys.error("computeEffects: NodeLookupByKey not implemented!")
        }
    }
  }

}
