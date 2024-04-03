// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.lf.command.{ApiCommand}
import com.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.daml.lf.value.{Value => V}
import com.daml.lf.model.test.Ledgers._

class ToCommands(universalTemplatePkgId: Ref.PackageId) {

  type PartyIdMapping = Map[PartyId, Ref.Party]
  type ContractIdMapping = Map[ContractId, V.ContractId]

  private def mkIdentifier(name: String): Ref.Identifier =
    Ref.Identifier(universalTemplatePkgId, Ref.QualifiedName.assertFromString(name))

  private def mkName(name: String): Ref.Name = Ref.Name.assertFromString(name)

  private def mkRecord(tycon: String, fields: (String, V)*): V = {
    V.ValueRecord(
      tycon = Some(mkIdentifier(tycon)),
      fields = fields.view
        .map { case (name, value) =>
          Some(mkName(name)) -> value
        }
        .to(ImmArray),
    )
  }

  private def mkVariant(tycon: String, variant: String, value: V): V =
    V.ValueVariant(
      tycon = Some(mkIdentifier(tycon)),
      variant = mkName(variant),
      value = value,
    )

  private def mkEnum(tycon: String, value: String): V =
    V.ValueEnum(
      tycon = Some(mkIdentifier(tycon)),
      value = mkName(value),
    )

  private def mkList(values: IterableOnce[V]): V =
    V.ValueList(values.iterator.to(FrontStack))

  private def partyToValue(partyIds: PartyIdMapping, partyId: PartyId): V =
    V.ValueParty(partyIds(partyId))

  private def partySetToValue(partyIds: PartyIdMapping, parties: PartySet): V =
    mkList(parties.view.map(partyToValue(partyIds, _)))

  private def kindToValue(kind: ExerciseKind): V =
    kind match {
      case Consuming => mkEnum("Universal:Kind", "Consuming")
      case NonConsuming => mkEnum("Universal:Kind", "NonConsuming")
    }

  def actionToValue(partyIds: PartyIdMapping, action: Action): V =
    action match {
      case Create(contractId, signatories, observers) =>
        mkVariant(
          "Universal:TxAction",
          "Create",
          mkRecord(
            "Universal:TxAction.Create",
            "contractId" -> V.ValueInt64(contractId.longValue),
            "signatories" -> partySetToValue(partyIds, signatories),
            "observers" -> partySetToValue(partyIds, observers),
          ),
        )
      case Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
        mkVariant(
          "Universal:TxAction",
          "Exercise",
          mkRecord(
            "Universal:TxAction.Exercise",
            "kind" -> kindToValue(kind),
            "contractId" -> V.ValueInt64(contractId.longValue),
            "controllers" -> partySetToValue(partyIds, controllers),
            "choiceObservers" -> partySetToValue(partyIds, choiceObservers),
            "subTransaction" -> mkList(subTransaction.view.map(actionToValue(partyIds, _))),
          ),
        )
      case Fetch(contractId) =>
        mkVariant(
          "Universal:TxAction",
          "Fetch",
          mkRecord(
            "Universal:TxAction.Fetch",
            "contractId" -> V.ValueInt64(contractId.longValue),
          ),
        )
      case Rollback(subTransaction) =>
        mkVariant(
          "Universal:TxAction",
          "Rollback",
          mkRecord(
            "Universal:TxAction.Rollback",
            "subTransaction" -> mkList(subTransaction.view.map(actionToValue(partyIds, _))),
          ),
        )
    }

  private def envToValue(contractIds: ContractIdMapping): V =
    V.ValueGenMap(
      contractIds.view
        .map { case (k, v) =>
          V.ValueInt64(k.longValue) -> V.ValueContractId(v)
        }
        .to(ImmArray)
    )

  def actionToApiCommand(
      partyIds: PartyIdMapping,
      contractIds: ContractIdMapping,
      action: Action,
  ): ApiCommand =
    action match {
      case Create(_, signatories, observers) =>
        ApiCommand.Create(
          mkIdentifier("Universal:Universal"),
          mkRecord(
            "Universal:Universal",
            "signatories" -> partySetToValue(partyIds, signatories),
            "observers" -> partySetToValue(partyIds, observers),
          ),
        )
      case Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
        val choiceName = kind match {
          case Consuming => "ConsumingChoice"
          case NonConsuming => "NonConsumingChoice"
        }
        ApiCommand.Exercise(
          typeId = mkIdentifier("Universal:Universal"),
          contractId = contractIds(contractId),
          choiceId = mkName(choiceName),
          argument = mkRecord(
            s"Universal:$choiceName",
            "env" -> envToValue(contractIds),
            "controllers" -> partySetToValue(partyIds, controllers),
            "choiceObservers" -> partySetToValue(partyIds, choiceObservers),
            "subTransaction" -> mkList(subTransaction.view.map(actionToValue(partyIds, _))),
          ),
        )
      case Fetch(_) => throw new RuntimeException("Fetch not supported at command level")
      case Rollback(_) => throw new RuntimeException("Rollback not supported at command level")
    }
}
