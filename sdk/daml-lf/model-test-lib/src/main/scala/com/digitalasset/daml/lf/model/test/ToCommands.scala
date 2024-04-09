// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import cats.implicits.toTraverseOps
import cats.instances.all._
import com.daml.lf.command.ApiCommand
import com.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.daml.lf.value.{Value => V}
import com.daml.lf.model.test.Ledgers._

object ToCommands {
  type PartyIdMapping = Map[PartyId, Ref.Party]
  type ContractIdMapping = Map[ContractId, V.ContractId]

  sealed trait TranslationError
  final case class PartyIdNotFound(partyId: PartyId) extends TranslationError
  final case class ContractIdNotFoundError(contractId: ContractId) extends TranslationError
}

class ToCommands(universalTemplatePkgId: Ref.PackageId) {

  import ToCommands._

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

  private def partyToValue(
      partyIds: PartyIdMapping,
      partyId: PartyId,
  ): Either[PartyIdNotFound, V] = {
    for {
      concretePartyId <- partyIds
        .get(partyId)
        .toRight(PartyIdNotFound(partyId))
    } yield V.ValueParty(concretePartyId)
  }

  private def partySetToValue(
      partyIds: PartyIdMapping,
      parties: PartySet,
  ): Either[PartyIdNotFound, V] =
    parties.toList.traverse(partyToValue(partyIds, _)).map(mkList)

  private def kindToValue(kind: ExerciseKind): V =
    kind match {
      case Consuming => mkEnum("Universal:Kind", "Consuming")
      case NonConsuming => mkEnum("Universal:Kind", "NonConsuming")
    }

  def actionToValue(partyIds: PartyIdMapping, action: Action): Either[PartyIdNotFound, V] =
    action match {
      case Create(contractId, signatories, observers) =>
        for {
          concreteSignatories <- partySetToValue(partyIds, signatories)
          concreteObservers <- partySetToValue(partyIds, observers)
        } yield mkVariant(
          "Universal:TxAction",
          "Create",
          mkRecord(
            "Universal:TxAction.Create",
            "contractId" -> V.ValueInt64(contractId.longValue),
            "signatories" -> concreteSignatories,
            "observers" -> concreteObservers,
          ),
        )
      case Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
        for {
          concreteControllers <- partySetToValue(partyIds, controllers)
          concreteChoiceObservers <- partySetToValue(partyIds, choiceObservers)
          translatedActions <- subTransaction.traverse(actionToValue(partyIds, _))
        } yield mkVariant(
          "Universal:TxAction",
          "Exercise",
          mkRecord(
            "Universal:TxAction.Exercise",
            "kind" -> kindToValue(kind),
            "contractId" -> V.ValueInt64(contractId.longValue),
            "controllers" -> concreteControllers,
            "choiceObservers" -> concreteChoiceObservers,
            "subTransaction" -> mkList(translatedActions),
          ),
        )
      case Fetch(contractId) =>
        Right(
          mkVariant(
            "Universal:TxAction",
            "Fetch",
            mkRecord(
              "Universal:TxAction.Fetch",
              "contractId" -> V.ValueInt64(contractId.longValue),
            ),
          )
        )
      case Rollback(subTransaction) =>
        for {
          translatedActions <- subTransaction.traverse(actionToValue(partyIds, _))
        } yield mkVariant(
          "Universal:TxAction",
          "Rollback",
          mkRecord(
            "Universal:TxAction.Rollback",
            "subTransaction" -> mkList(translatedActions),
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
  ): Either[TranslationError, ApiCommand] =
    action match {
      case Create(_, signatories, observers) =>
        for {
          concreteSignatories <- partySetToValue(partyIds, signatories)
          concreteObservers <- partySetToValue(partyIds, observers)
        } yield ApiCommand.Create(
          mkIdentifier("Universal:Universal"),
          mkRecord(
            "Universal:Universal",
            "signatories" -> concreteSignatories,
            "observers" -> concreteObservers,
          ),
        )
      case Exercise(kind, contractId, controllers, choiceObservers, subTransaction) =>
        val choiceName = kind match {
          case Consuming => "ConsumingChoice"
          case NonConsuming => "NonConsumingChoice"
        }
        for {
          concreteContractId <- contractIds
            .get(contractId)
            .toRight(ContractIdNotFoundError(contractId))
          concreteControllers <- partySetToValue(partyIds, controllers)
          concreteChoiceObservers <- partySetToValue(partyIds, choiceObservers)
          translatedActions <- subTransaction.traverse(actionToValue(partyIds, _))
        } yield ApiCommand.Exercise(
          typeId = mkIdentifier("Universal:Universal"),
          contractId = concreteContractId,
          choiceId = mkName(choiceName),
          argument = mkRecord(
            s"Universal:$choiceName",
            "env" -> envToValue(contractIds),
            "controllers" -> concreteControllers,
            "choiceObservers" -> concreteChoiceObservers,
            "subTransaction" -> mkList(translatedActions),
          ),
        )
      case Fetch(_) => throw new RuntimeException("Fetch not supported at command level")
      case Rollback(_) => throw new RuntimeException("Rollback not supported at command level")
    }
}
