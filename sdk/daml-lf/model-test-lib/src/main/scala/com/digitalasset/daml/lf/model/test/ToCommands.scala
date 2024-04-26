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
  sealed trait SomeContractId {
    def contractId: V.ContractId
  }
  final case class UniversalContractId(contractId: V.ContractId) extends SomeContractId
  final case class UniversalWithKeyContractId(contractId: V.ContractId) extends SomeContractId

  type PartyIdMapping = Map[PartyId, Ref.Party]
  type ContractIdMapping = Map[ContractId, SomeContractId]

  sealed trait TranslationError
  final case class PartyIdNotFound(partyId: PartyId) extends TranslationError
  final case class ContractIdNotFoundError(contractId: ContractId) extends TranslationError
}

class ToCommands(universalTemplatePkgId: Ref.PackageId) {

  import ToCommands._

  private def mkIdentifier(name: String): Ref.Identifier =
    Ref.Identifier(universalTemplatePkgId, Ref.QualifiedName.assertFromString(name))

  val universalTemplateId = mkIdentifier("Universal:Universal")
  val universalWithKeyTemplateId = mkIdentifier("Universal:UniversalWithKey")

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

  private def keyToValue(
      partyIds: PartyIdMapping,
      keyId: KeyId,
      maintainers: PartySet,
  ): Either[PartyIdNotFound, V] = for {
    concreteMaintainers <- partySetToValue(partyIds, maintainers)
  } yield mkRecord(
    "DA.Types:Tuple2",
    "_1" -> V.ValueInt64(keyId.longValue),
    "_2" -> concreteMaintainers,
  )

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
      case CreateWithKey(contractId, keyId, maintainers, signatories, observers) =>
        for {
          concreteMaintainers <- partySetToValue(partyIds, maintainers)
          concreteSignatories <- partySetToValue(partyIds, signatories)
          concreteObservers <- partySetToValue(partyIds, observers)
        } yield mkVariant(
          "Universal:TxAction",
          "CreateWithKey",
          mkRecord(
            "Universal:TxAction.CreateWithKey",
            "contractId" -> V.ValueInt64(contractId.longValue),
            "keyId" -> V.ValueInt64(keyId.longValue),
            "maintainers" -> concreteMaintainers,
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
      case ExerciseByKey(
            kind,
            _,
            keyId,
            maintainers,
            controllers,
            choiceObservers,
            subTransaction,
          ) =>
        for {
          concreteMaintainers <- partySetToValue(partyIds, maintainers)
          concreteControllers <- partySetToValue(partyIds, controllers)
          concreteChoiceObservers <- partySetToValue(partyIds, choiceObservers)
          translatedActions <- subTransaction.traverse(actionToValue(partyIds, _))
        } yield mkVariant(
          "Universal:TxAction",
          "ExerciseByKey",
          mkRecord(
            "Universal:TxAction.ExerciseByKey",
            "kind" -> kindToValue(kind),
            "keyId" -> V.ValueInt64(keyId.longValue),
            "maintainers" -> concreteMaintainers,
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
      case FetchByKey(contractId, keyId, maintainers) =>
        for {
          concreteMaintainers <- partySetToValue(partyIds, maintainers)
        } yield mkVariant(
          "Universal:TxAction",
          "FetchByKey",
          mkRecord(
            "Universal:TxAction.FetchByKey",
            "keyId" -> V.ValueInt64(keyId.longValue),
            "maintainers" -> concreteMaintainers,
            "expectedContractId" -> V.ValueInt64(contractId.longValue),
          ),
        )
      case LookupByKey(contractId, keyId, maintainers) =>
        for {
          concreteMaintainers <- partySetToValue(partyIds, maintainers)
        } yield mkVariant(
          "Universal:TxAction",
          "LookupByKey",
          mkRecord(
            "Universal:TxAction.LookupByKey",
            "keyId" -> V.ValueInt64(keyId.longValue),
            "maintainers" -> concreteMaintainers,
            "expectedResult" -> V.ValueOptional(contractId.map(cid => V.ValueInt64(cid.longValue))),
          ),
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
          V.ValueInt64(k.longValue) ->
            (v match {
              case UniversalContractId(v) =>
                mkVariant("Universal:SomeContractId", "UniversalContractId", V.ValueContractId(v))
              case UniversalWithKeyContractId(v) =>
                mkVariant(
                  "Universal:SomeContractId",
                  "UniversalWithKeyContractId",
                  V.ValueContractId(v),
                )
            })
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
          universalTemplateId,
          mkRecord(
            "Universal:Universal",
            "signatories" -> concreteSignatories,
            "observers" -> concreteObservers,
          ),
        )
      case CreateWithKey(_, keyId, maintainers, signatories, observers) =>
        for {
          concreteMaintainers <- partySetToValue(partyIds, maintainers)
          concreteSignatories <- partySetToValue(partyIds, signatories)
          concreteObservers <- partySetToValue(partyIds, observers)
        } yield ApiCommand.Create(
          universalWithKeyTemplateId,
          mkRecord(
            "Universal:UniversalWithKey",
            "keyId" -> V.ValueInt64(keyId.longValue),
            "maintainers" -> concreteMaintainers,
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
          someConcreteContractId <- contractIds
            .get(contractId)
            .toRight(ContractIdNotFoundError(contractId))
          concreteControllers <- partySetToValue(partyIds, controllers)
          concreteChoiceObservers <- partySetToValue(partyIds, choiceObservers)
          translatedActions <- subTransaction.traverse(actionToValue(partyIds, _))
        } yield someConcreteContractId match {
          case UniversalContractId(concreteContractId) =>
            ApiCommand.Exercise(
              typeId = universalTemplateId,
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
          case UniversalWithKeyContractId(concreteContractId) =>
            ApiCommand.Exercise(
              typeId = universalWithKeyTemplateId,
              contractId = concreteContractId,
              choiceId = mkName(s"K$choiceName"),
              argument = mkRecord(
                s"Universal:K$choiceName",
                "env" -> envToValue(contractIds),
                "controllers" -> concreteControllers,
                "choiceObservers" -> concreteChoiceObservers,
                "subTransaction" -> mkList(translatedActions),
              ),
            )
        }
      case ExerciseByKey(
            kind,
            _,
            keyId,
            maintainers,
            controllers,
            choiceObservers,
            subTransaction,
          ) =>
        val choiceName = kind match {
          case Consuming => "ConsumingChoice"
          case NonConsuming => "NonConsumingChoice"
        }
        for {
          concreteKey <- keyToValue(partyIds, keyId, maintainers)
          concreteControllers <- partySetToValue(partyIds, controllers)
          concreteChoiceObservers <- partySetToValue(partyIds, choiceObservers)
          translatedActions <- subTransaction.traverse(actionToValue(partyIds, _))
        } yield ApiCommand.ExerciseByKey(
          templateId = universalWithKeyTemplateId,
          contractKey = concreteKey,
          choiceId = mkName(s"K$choiceName"),
          argument = mkRecord(
            s"Universal:K$choiceName",
            "env" -> envToValue(contractIds),
            "controllers" -> concreteControllers,
            "choiceObservers" -> concreteChoiceObservers,
            "subTransaction" -> mkList(translatedActions),
          ),
        )
      case Fetch(_) => throw new RuntimeException("Fetch not supported at command level")
      case FetchByKey(_, _, _) =>
        throw new RuntimeException("FetchByKey not supported at command level")
      case LookupByKey(_, _, _) =>
        throw new RuntimeException("LookupByKey not supported at command level")
      case Rollback(_) => throw new RuntimeException("Rollback not supported at command level")
    }
}
