// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package data

import com.digitalasset.daml.lf.command.ApiContractKey
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  FatContractInstanceImpl,
  GlobalKeyWithMaintainers,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.{command, crypto, language, speedy}

import scala.collection.immutable.TreeSet
import scala.language.implicitConversions

private[lf] object CostModel {

  type Cost = Long

  trait CostModelImplicits {
    implicit def costOfPackageVersion(value: Ref.PackageVersion): Cost

    implicit def costOfLanguageVersion(value: language.LanguageVersion): Cost

    implicit def costOfTypeConRef(value: Ref.TypeConRef): Cost

    implicit def costOfIdentifier(value: Ref.Identifier): Cost

    implicit def costOfString(value: String): Cost

    implicit def costOfContractId(value: Value.ContractId): Cost

    implicit def costOfDate(value: Time.Date): Cost

    implicit def costOfTimestamp(value: Time.Timestamp): Cost

    implicit def costOfCreationTime(value: CreationTime): Cost

    implicit def costOfInt(value: Int): Cost

    implicit def costOfLong(value: Long): Cost

    implicit def costOfUnit(value: Unit): Cost

    implicit def costOfValue(value: Value): Cost

    implicit def costOfSpeedyValue(value: speedy.SValue): Cost

    implicit def costOfFatContractInstance(value: FatContractInstance): Cost

    implicit def costOfApiCommand(value: command.ApiCommand): Cost

    implicit def costOfSpeedyApiCommand(value: speedy.ApiCommand): Cost

    implicit def costOfApiContractKey(value: ApiContractKey): Cost

    implicit def costOfBytes(value: Bytes): Cost

    implicit def costOfHash(value: crypto.Hash): Cost

    implicit def costOfGlobalKeyWithMaintainers(value: GlobalKeyWithMaintainers): Cost

    implicit def costOfTuple2[A, B](
        value: (A, B)
    )(implicit fstCost: A => Cost, sndCost: B => Cost): Cost

    implicit def costOfOption[A](value: Option[A])(implicit elemCost: A => Cost): Cost

    implicit def costOfMap[A, B](
        value: Map[A, B]
    )(implicit keyCost: A => Cost, valueCost: B => Cost): Cost

    implicit def costOfImmArray[A](value: ImmArray[A])(implicit elemCost: A => Cost): Cost

    implicit def costOfSeq[A](value: Seq[A])(implicit elemCost: A => Cost): Cost

    implicit def costOfSortedList[A](value: SortedLookupList[A])(implicit
        elemCost: A => Cost
    ): Cost

    implicit def costOfFrontStack[A](value: FrontStack[A])(implicit elemCost: A => Cost): Cost

    implicit def costOfSet[A](value: Set[A])(implicit elemCost: A => Cost): Cost

    implicit def costOfTreeSet[A](value: TreeSet[A])(implicit elemCost: A => Cost): Cost
  }

  object EmptyCostModelImplicits extends CostModelImplicits {
    implicit def costOfPackageVersion(value: Ref.PackageVersion): Cost = 0L

    implicit def costOfLanguageVersion(value: language.LanguageVersion): Cost = 0L

    implicit def costOfTypeConRef(value: Ref.TypeConRef): Cost = 0L

    implicit def costOfIdentifier(value: Ref.Identifier): Cost = 0L

    implicit def costOfString(value: String): Cost = 0L

    implicit def costOfContractId(value: Value.ContractId): Cost = 0L

    implicit def costOfDate(value: Time.Date): Cost = 0L

    implicit def costOfTimestamp(value: Time.Timestamp): Cost = 0L

    implicit def costOfCreationTime(value: CreationTime): Cost = 0L

    implicit def costOfInt(value: Int): Cost = 0L

    implicit def costOfLong(value: Long): Cost = 0L

    implicit def costOfUnit(value: Unit): Cost = 0L

    implicit def costOfValue(value: Value): Cost = 0L

    implicit def costOfSpeedyValue(value: speedy.SValue): Cost = 0L

    implicit def costOfFatContractInstance(value: FatContractInstance): Cost = 0L

    implicit def costOfApiCommand(value: command.ApiCommand): Cost = 0L

    implicit def costOfSpeedyApiCommand(value: speedy.ApiCommand): Cost = 0L

    implicit def costOfApiContractKey(value: ApiContractKey): Cost = 0L

    implicit def costOfBytes(value: Bytes): Cost = 0L

    implicit def costOfHash(value: crypto.Hash): Cost = 0L

    implicit def costOfGlobalKeyWithMaintainers(value: GlobalKeyWithMaintainers): Cost = 0L

    implicit def costOfTuple2[A, B](
        value: (A, B)
    )(implicit fstCost: A => Cost, sndCost: B => Cost): Cost = 0L

    implicit def costOfOption[A](value: Option[A])(implicit elemCost: A => Cost): Cost = 0L

    implicit def costOfMap[A, B](
        value: Map[A, B]
    )(implicit keyCost: A => Cost, valueCost: B => Cost): Cost = 0L

    implicit def costOfImmArray[A](value: ImmArray[A])(implicit elemCost: A => Cost): Cost = 0L

    implicit def costOfSeq[A](value: Seq[A])(implicit elemCost: A => Cost): Cost = 0L

    implicit def costOfSortedList[A](value: SortedLookupList[A])(implicit
        elemCost: A => Cost
    ): Cost = 0L

    implicit def costOfFrontStack[A](value: FrontStack[A])(implicit elemCost: A => Cost): Cost =
      0L

    implicit def costOfSet[A](value: Set[A])(implicit elemCost: A => Cost): Cost = 0L

    implicit def costOfTreeSet[A](value: TreeSet[A])(implicit elemCost: A => Cost): Cost = 0L
  }

  object StructuralCostModelImplicits extends CostModelImplicits {
    implicit def costOfPackageVersion(value: Ref.PackageVersion): Cost =
      1 + value.segments.length.toLong

    implicit def costOfLanguageVersion(value: language.LanguageVersion): Cost =
      1 + value.pretty.length.toLong

    implicit def costOfTypeConRef(value: Ref.TypeConRef): Cost = 1 + value.toString.length.toLong

    implicit def costOfIdentifier(value: Ref.Identifier): Cost = 1 + value.toString.length.toLong

    implicit def costOfString(value: String): Cost = value.length.toLong

    implicit def costOfContractId(value: Value.ContractId): Cost = 1 + value.coid.length.toLong

    implicit def costOfDate(value: Time.Date): Cost = 1 + costOfInt(value.days)

    implicit def costOfTimestamp(value: Time.Timestamp): Cost = 1 + costOfLong(value.micros)

    implicit def costOfCreationTime(value: CreationTime): Cost =
      1 + costOfLong(CreationTime.encode(value))

    implicit def costOfInt(value: Int): Cost = 4

    implicit def costOfLong(value: Long): Cost = 8

    implicit def costOfUnit(value: Unit): Cost = 0

    implicit def costOfValue(value: Value): Cost = value match {
      case Value.ValueBool(_) =>
        1 + 1
      case Value.ValueText(txt) =>
        1 + txt.length.toLong
      case Value.ValueEnum(tycon, value) =>
        1 + costOfOption(tycon) + costOfString(value)
      case Value.ValueContractId(cid) =>
        1 + costOfContractId(cid)
      case Value.ValueDate(date) =>
        1 + costOfDate(date)
      case Value.ValueGenMap(map) =>
        1 + costOfImmArray(map)
      case Value.ValueInt64(n) =>
        1 + costOfLong(n)
      case Value.ValueList(value) =>
        1 + costOfFrontStack(value)(costOfValue)
      case Value.ValueNumeric(value) =>
        1 + 42 // FIXME:
      case Value.ValueOptional(opt) =>
        1 + costOfOption(opt)(costOfValue)
      case Value.ValueParty(p) =>
        1 + costOfString(p)
      case Value.ValueRecord(tyCon, fields) =>
        implicit def costOfFieldEntry(value: (Option[Ref.Name], Value)): Cost = {
          costOfTuple2(value)(costOfOption, costOfValue)
        }

        1 + costOfOption(tyCon) + costOfImmArray(fields)
      case Value.ValueTextMap(ls) =>
        1 + costOfSortedList(ls)(costOfValue)
      case Value.ValueTimestamp(ts) =>
        1 + costOfTimestamp(ts)
      case Value.ValueUnit =>
        1 + costOfUnit(())
      case Value.ValueVariant(tycon, variant, value) =>
        1 + costOfOption(tycon) + costOfString(variant) + costOfValue(value)
    }

    implicit def costOfSpeedyValue(value: speedy.SValue): Cost = costOfValue(value.toNormalizedValue)

    implicit def costOfFatContractInstance(value: FatContractInstance): Cost = {
      val FatContractInstanceImpl(
        version,
        contractId,
        pkgName,
        templateId,
        createArg,
        signatories,
        stakeholders,
        contractKey,
        createdAt,
        authData,
      ) = value

      1 + costOfLanguageVersion(version) +
        costOfContractId(contractId) +
        costOfString(pkgName) +
        costOfIdentifier(templateId) +
        costOfValue(createArg) +
        costOfTreeSet(signatories) +
        costOfTreeSet(stakeholders) +
        costOfOption(contractKey) +
        costOfCreationTime(createdAt) +
        costOfBytes(authData)
    }

    implicit def costOfApiCommand(value: command.ApiCommand): Cost = value match {
      case command.ApiCommand.Create(templateRef, arg) =>
        1 + costOfTypeConRef(templateRef) + costOfValue(arg)

      case command.ApiCommand.Exercise(typeRef, contractId, choiceId, arg) =>
        1 + costOfTypeConRef(typeRef) +
          costOfContractId(contractId) +
          costOfString(choiceId) +
          costOfValue(arg)

      case command.ApiCommand.ExerciseByKey(templateRef, contractKey, choiceId, arg) =>
        1 + costOfTypeConRef(templateRef) +
          costOfValue(contractKey) +
          costOfString(choiceId) +
          costOfValue(arg)

      case command.ApiCommand.CreateAndExercise(templateRef, createArg, choiceId, choiceArg) =>
        1 + costOfTypeConRef(templateRef) +
          costOfValue(createArg) +
          costOfString(choiceId) +
          costOfValue(choiceArg)
    }

    implicit def costOfSpeedyApiCommand(value: speedy.ApiCommand): Cost = value match {
      case speedy.Command.Create(templateId, arg) =>
        1 + costOfIdentifier(templateId) +
          costOfSpeedyValue(arg)
      case speedy.Command.CreateAndExercise(templateId, createArg, choiceId, choiceArg) =>
        1 + costOfIdentifier(templateId) +
          costOfSpeedyValue(createArg) +
          costOfString(choiceId) +
          costOfSpeedyValue(choiceArg)
      case speedy.Command.ExerciseTemplate(templateId, contractId, choiceId, arg) =>
        1 + costOfIdentifier(templateId) +
          costOfSpeedyValue(contractId) +
          costOfString(choiceId) +
          costOfSpeedyValue(arg)
      case speedy.Command.ExerciseInterface(interfaceId, contractId, choiceId, arg) =>
        1 + costOfIdentifier(interfaceId) +
          costOfSpeedyValue(contractId) +
          costOfString(choiceId) +
          costOfSpeedyValue(arg)
      case speedy.Command.ExerciseByKey(templateId, key, choiceId, arg) =>
        1 + costOfIdentifier(templateId) +
          costOfSpeedyValue(key) +
          costOfString(choiceId) +
          costOfSpeedyValue(arg)
    }

    implicit def costOfApiContractKey(value: ApiContractKey): Cost = {
      val ApiContractKey(templateRef, contractKey) = value

      1 + costOfTypeConRef(templateRef) + costOfValue(contractKey)
    }

    implicit def costOfBytes(value: Bytes): Cost = 1 + value.length.toLong

    implicit def costOfHash(value: crypto.Hash): Cost = 1 + value.bytes.length.toLong

    implicit def costOfGlobalKeyWithMaintainers(value: GlobalKeyWithMaintainers): Cost = {
      val GlobalKeyWithMaintainers(key, maintainers) = value
      val costOfGlobalKey =
        1 + costOfIdentifier(key.templateId) +
          costOfString(key.packageName) +
          costOfValue(key.key) +
          costOfHash(key.hash)

      1 + costOfGlobalKey + costOfSet(maintainers)
    }

    implicit def costOfTuple2[A, B](
        value: (A, B)
    )(implicit fstCost: A => Cost, sndCost: B => Cost): Cost =
      1 + fstCost(value._1) + sndCost(value._2)

    implicit def costOfOption[A](value: Option[A])(implicit elemCost: A => Cost): Cost =
      1 + value.map(elemCost).getOrElse(0L)

    implicit def costOfMap[A, B](
        value: Map[A, B]
    )(implicit keyCost: A => Cost, valueCost: B => Cost): Cost =
      1 + value.keys.map(keyCost).sum.toLong + value.values.map(valueCost).sum.toLong

    implicit def costOfImmArray[A](value: ImmArray[A])(implicit elemCost: A => Cost): Cost =
      1 + value.toSeq.map(elemCost).sum.toLong

    implicit def costOfSeq[A](value: Seq[A])(implicit elemCost: A => Cost): Cost =
      1 + value.map(elemCost).sum.toLong

    implicit def costOfSortedList[A](value: SortedLookupList[A])(implicit
        elemCost: A => Cost
    ): Cost = 1 + costOfImmArray(value.toImmArray)

    implicit def costOfFrontStack[A](value: FrontStack[A])(implicit elemCost: A => Cost): Cost =
      1 + costOfImmArray(value.toImmArray)(elemCost)

    implicit def costOfSet[A](value: Set[A])(implicit elemCost: A => Cost): Cost =
      1 + value.map(elemCost).sum.toLong

    implicit def costOfTreeSet[A](value: TreeSet[A])(implicit elemCost: A => Cost): Cost =
      1 + value.map(elemCost).sum.toLong
  }
}
