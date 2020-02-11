// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import scala.annotation.tailrec

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.SubmittedTransaction
import com.digitalasset.daml.lf.data.{ImmArray, InsertOrdSet}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  RelativeContractId,
  VersionedValue,
  ValueParty,
  ValueRecord,
  ValueVariant,
  ValueList,
  ValueStruct,
  ValueOptional,
  ValueTextMap,
  ValueGenMap,
  ValueContractId,
  ValueNumeric,
  ValueInt64,
  ValueTimestamp,
  ValueBool,
  ValueDate,
  ValueEnum,
  ValueText,
  ValueUnit
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
    val packageInputs: InsertOrdSet[DamlStateKey] = {
      val usedPackages = tx.optUsedPackages.getOrElse(
        throw new InternalError("Transaction was not annotated with used packages")
      )
      import PackageId.ordering
      InsertOrdSet.fromSeq(
        usedPackages.toList.sorted
          .map { pkgId =>
            DamlStateKey.newBuilder.setPackageId(pkgId).build
          }
      )
    }

    def contractInputs(coid: ContractId): InsertOrdSet[DamlStateKey] =
      coid match {
        case acoid: AbsoluteContractId =>
          InsertOrdSet.empty + absoluteContractIdToStateKey(acoid)
        case _ =>
          InsertOrdSet.empty
      }

    def partyInputs(parties: Set[Party]): InsertOrdSet[DamlStateKey] = {
      import Party.ordering
      InsertOrdSet.fromSeq(parties.toList.sorted.map(partyStateKey))
    }

    def foldValue(inputs: InsertOrdSet[DamlStateKey], v: Value[ContractId]): InsertOrdSet[DamlStateKey] = {
      @tailrec def go(inputs: InsertOrdSet[DamlStateKey], vs: Seq[Value[ContractId]]): InsertOrdSet[DamlStateKey] = {
        vs match {
          case Nil => inputs
          case ValueParty(p) :: tail => go(inputs ++ partyInputs(Set(p)), tail)
          case v :: tail => go(inputs, tail ++ subValues(v).toSeq)
        }
      }
      go(inputs, Seq(v))
    }

    def partyInputsInValues() =
      tx.foldValues(packageInputs) { case (inputs, VersionedValue(_, v)) =>
          foldValue(inputs, v)
      }

    tx.fold(packageInputs: Set[DamlStateKey]) {
        case (inputs, (nodeId, node)) =>
          node match {
            case fetch: NodeFetch[ContractId] =>
              inputs ++ contractInputs(fetch.coid) ++ partyInputs(fetch.signatories) ++
                partyInputs(fetch.stakeholders)
            case create: NodeCreate[ContractId, VersionedValue[ContractId]] =>
              create.key.fold(inputs) { keyWithM =>
                inputs + contractKeyToStateKey(
                  GlobalKey(create.coinst.template, forceNoContractIds(keyWithM.key)))
              } ++ partyInputs(create.signatories) ++ partyInputs(create.stakeholders)
            case exe: NodeExercises[_, ContractId, _] =>
              inputs ++ contractInputs(exe.targetCoid) ++ partyInputs(exe.stakeholders) ++ partyInputs(
                exe.signatories) ++ partyInputs(exe.controllers)
            case l: NodeLookupByKey[ContractId, Transaction.Value[ContractId]] =>
              // We need both the contract key state and the contract state. The latter is used to verify
              // that the submitter can access the contract.
              l.result.fold(inputs)(inputs ++ contractInputs(_)) +
                contractKeyToStateKey(GlobalKey(l.templateId, forceNoContractIds(l.key.key)))
          }
      }.toList ++ partyInputsInValues()
    }

  def subValues(v: Value[ContractId]): ImmArray[Value[ContractId]] =
    v match {
      case ValueRecord(_, fields) => fields.map(_._2)
      case ValueList(elems) => elems.toImmArray
      case ValueStruct(fields) => fields.map(_._2)
      case ValueVariant(_, _, v) => ImmArray(v)
      case ValueOptional(optV) => optV match {
        case Some(v) => ImmArray(v)
        case None => ImmArray.empty
      }
      case ValueTextMap(map) => map.values
      case ValueGenMap(entries) =>
        entries.map(_._1).slowAppend(entries.map(_._2)) // TODO Review: is there a better way?
      case // Enumerating explicitly all cases to make it exhaustive and always get compilation errors on model changes
        ValueContractId(_) | ValueNumeric(_) | ValueInt64(_) | ValueTimestamp(_) | ValueBool(_) | ValueDate(_)
        | ValueParty(_) | ValueText(_) | ValueEnum(_, _) | ValueUnit
        => ImmArray.empty
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
