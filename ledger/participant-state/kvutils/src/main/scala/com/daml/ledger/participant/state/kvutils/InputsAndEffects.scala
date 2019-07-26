// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.v1.SubmittedTransaction
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.daml.lf.transaction.Node.{
  NodeCreate,
  NodeExercises,
  NodeFetch,
  NodeLookupByKey,
  GlobalKey
}
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  RelativeContractId,
  VersionedValue
}
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
      createdContracts: List[(DamlStateKey, NodeCreate[ContractId, VersionedValue[ContractId]])],
      /** The contract keys created or updated as part of the transaction. */
      updatedContractKeys: List[(DamlStateKey, DamlContractKeyState)]
  )
  object Effects {
    val empty = Effects(List.empty, List.empty, List.empty)
  }

  /** Compute the inputs to a DAML transaction, that is, the referenced contracts
    * and packages.
    */
  def computeInputs(tx: SubmittedTransaction): (List[DamlLogEntryId], List[DamlStateKey]) = {
    val packageInputs: List[DamlStateKey] = tx.usedPackages.map { pkgId =>
      DamlStateKey.newBuilder.setPackageId(pkgId).build
    }.toList

    def addLogEntryInput(inputs: List[DamlLogEntryId], coid: ContractId): List[DamlLogEntryId] =
      coid match {
        case acoid: AbsoluteContractId =>
          absoluteContractIdToLogEntryId(acoid)._1 :: inputs
        case _ =>
          inputs
      }
    def addStateInput(inputs: List[DamlStateKey], coid: ContractId): List[DamlStateKey] =
      coid match {
        case acoid: AbsoluteContractId =>
          absoluteContractIdToStateKey(acoid) :: inputs
        case _ =>
          inputs
      }

    tx.fold(GenTransaction.TopDown, (List.empty[DamlLogEntryId], packageInputs)) {
      case ((logEntryInputs, stateInputs), (nodeId, node)) =>
        node match {
          case fetch: NodeFetch[ContractId] =>
            (
              addLogEntryInput(logEntryInputs, fetch.coid),
              addStateInput(stateInputs, fetch.coid)
            )
          case create: NodeCreate[ContractId, VersionedValue[ContractId]] =>
            (
              logEntryInputs,
              create.key.fold(stateInputs) { keyWithM =>
                contractKeyToStateKey(GlobalKey(
                  create.coinst.template,
                  forceAbsoluteContractIds(keyWithM.key))) :: stateInputs
              }
            )
          case exe: NodeExercises[_, ContractId, _] =>
            (
              addLogEntryInput(logEntryInputs, exe.targetCoid),
              addStateInput(stateInputs, exe.targetCoid)
            )
          case l: NodeLookupByKey[ContractId, Transaction.Value[ContractId]] =>
            (
              logEntryInputs,
              // We need both the contract key state and the contract state. The latter is used to verify
              // that the submitter can access the contract.
              contractKeyToStateKey(GlobalKey(l.templateId, forceAbsoluteContractIds(l.key.key))) ::
                l.result.fold(stateInputs)(addStateInput(stateInputs, _))
            )
        }
    }
  }

  /** Compute the effects of a DAML transaction, that is, the created and consumed contracts. */
  def computeEffects(entryId: DamlLogEntryId, tx: SubmittedTransaction): Effects = {
    tx.fold(GenTransaction.TopDown, Effects.empty) {
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
                    contractKeyToStateKey(
                      GlobalKey(
                        create.coinst.template,
                        forceAbsoluteContractIds(keyWithMaintainers.key))) ->
                      DamlContractKeyState.newBuilder
                        .setContractId(encodeRelativeContractId(
                          entryId,
                          create.coid.asInstanceOf[RelativeContractId]))
                        .build
                      :: effects.updatedContractKeys)
            )
          case exe: NodeExercises[_, ContractId, VersionedValue[ContractId]] =>
            if (exe.consuming) {
              exe.targetCoid match {
                case acoid: AbsoluteContractId =>
                  effects.copy(
                    consumedContracts = absoluteContractIdToStateKey(acoid) :: effects.consumedContracts,
                    updatedContractKeys = exe.key
                      .fold(effects.updatedContractKeys)(
                        key =>
                          contractKeyToStateKey(
                            GlobalKey(exe.templateId, forceAbsoluteContractIds(key))) ->
                            DamlContractKeyState.newBuilder.build :: effects.updatedContractKeys)
                  )
                case _ =>
                  effects
              }
            } else {
              effects
            }
          case l: NodeLookupByKey[_, _] =>
            effects
        }
    }
  }
}
