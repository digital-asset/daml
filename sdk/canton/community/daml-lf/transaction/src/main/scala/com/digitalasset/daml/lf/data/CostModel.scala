// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml
package lf
package data

import com.digitalasset.daml.lf.command.ApiContractKey
import com.digitalasset.daml.lf.{transaction => tx}
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  FatContractInstanceImpl,
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  SerializationVersion,
}
import com.digitalasset.daml.lf.value.Value

import scala.collection.immutable.TreeSet
import scala.language.implicitConversions

private[lf] object CostModel {

  type Cost = Long

  trait CostModelImplicits {
    implicit def costOfPackageVersion(value: Ref.PackageVersion): Cost

    implicit def costOfSerializationVersion(value: SerializationVersion): Cost

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

    implicit def costOfBoolean(value: Boolean): Cost

    implicit def costOfValue(value: Value): Cost

    implicit def costOfFatContractInstance(value: FatContractInstance): Cost

    implicit def costOfApiCommand(value: command.ApiCommand): Cost

    implicit def costOfApiContractKey(value: ApiContractKey): Cost

    implicit def costOfBytes(value: Bytes): Cost

    implicit def costOfHash(value: crypto.Hash): Cost

    implicit def costOfGlobalKey(value: GlobalKey): Cost

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

    implicit def costOfNodeId(value: tx.NodeId): Cost

    implicit def costOfTxNode(value: tx.Node): Cost
  }

  object EmptyCostModelImplicits extends CostModelImplicits {
    implicit def costOfPackageVersion(value: Ref.PackageVersion): Cost = 0L

    implicit def costOfSerializationVersion(value: SerializationVersion): Cost = 0L

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

    implicit def costOfBoolean(value: Boolean): Cost = 0L

    implicit def costOfValue(value: Value): Cost = 0L

    implicit def costOfFatContractInstance(value: FatContractInstance): Cost = 0L

    implicit def costOfApiCommand(value: command.ApiCommand): Cost = 0L

    implicit def costOfApiContractKey(value: ApiContractKey): Cost = 0L

    implicit def costOfBytes(value: Bytes): Cost = 0L

    implicit def costOfHash(value: crypto.Hash): Cost = 0L

    implicit def costOfGlobalKey(value: GlobalKey): Cost = 0L

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

    implicit def costOfNodeId(value: tx.NodeId): Cost = 0L

    implicit def costOfTxNode(value: tx.Node): Cost = 0L
  }

  object StructuralCostModelImplicits extends CostModelImplicits {
    implicit def costOfPackageVersion(value: Ref.PackageVersion): Cost =
      1 + value.segments.length.toLong

    implicit def costOfSerializationVersion(value: SerializationVersion): Cost =
      1 + SerializationVersion.All.view.map(SerializationVersion.toProtoValue(_).length).max.toLong

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

    implicit def costOfUnit(value: Unit): Cost = 1

    implicit def costOfBoolean(value: Boolean): Cost = 1

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
        1 + costOfString(value.toUnscaledString) + costOfInt(value.scale)
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

      1 + costOfSerializationVersion(version) +
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

    implicit def costOfApiContractKey(value: ApiContractKey): Cost = {
      val ApiContractKey(templateRef, contractKey) = value

      1 + costOfTypeConRef(templateRef) + costOfValue(contractKey)
    }

    implicit def costOfBytes(value: Bytes): Cost = 1 + value.length.toLong

    implicit def costOfHash(value: crypto.Hash): Cost = 1 + value.bytes.length.toLong

    implicit def costOfGlobalKey(value: GlobalKey): Cost = {
      1 + costOfIdentifier(value.templateId) +
        costOfString(value.packageName) +
        costOfValue(value.key) +
        costOfHash(value.hash)
    }

    implicit def costOfGlobalKeyWithMaintainers(value: GlobalKeyWithMaintainers): Cost = {
      val GlobalKeyWithMaintainers(key, maintainers) = value

      1 + costOfGlobalKey(key) + costOfSet(maintainers)
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

    implicit def costOfNodeId(value: tx.NodeId): Cost =
      1 + costOfInt(value.index)

    implicit def costOfTxNode(value: tx.Node): Cost = value match {
      case Node.Create(
            coid,
            packageName,
            templateId,
            arg,
            signatories,
            stakeholders,
            keyOpt,
            version,
          ) =>
        1 + costOfContractId(coid) +
          costOfString(packageName) +
          costOfIdentifier(templateId) +
          costOfValue(arg) +
          costOfSet(signatories) +
          costOfSet(stakeholders) +
          costOfOption(keyOpt) +
          costOfSerializationVersion(version)
      case Node.Fetch(
            coid,
            packageName,
            templateId,
            actingParties,
            signatories,
            stakeholders,
            keyOpt,
            byKey,
            interfaceId,
            version,
          ) =>
        1 + costOfContractId(coid) +
          costOfString(packageName) +
          costOfIdentifier(templateId) +
          costOfSet(actingParties) +
          costOfSet(signatories) +
          costOfSet(stakeholders) +
          costOfOption(keyOpt) +
          costOfBoolean(byKey) +
          costOfOption(interfaceId) +
          costOfSerializationVersion(version)
      case Node.LookupByKey(packageName, templateId, key, result, version) =>
        1 + costOfString(packageName) +
          costOfIdentifier(templateId) +
          costOfGlobalKeyWithMaintainers(key) +
          costOfOption(result) +
          costOfSerializationVersion(version)
      case Node.Exercise(
            targetCoid,
            packageName,
            templateId,
            interfaceId,
            choiceId,
            consuming,
            actingParties,
            chosenValue,
            stakeholders,
            signatories,
            choiceObservers,
            choiceAuthorizers,
            children,
            exerciseResult,
            keyOpt,
            byKey,
            version,
          ) =>
        1 + costOfContractId(targetCoid) +
          costOfString(packageName) +
          costOfIdentifier(templateId) +
          costOfOption(interfaceId) +
          costOfString(choiceId) +
          costOfBoolean(consuming) +
          costOfSet(actingParties) +
          costOfValue(chosenValue) +
          costOfSet(stakeholders) +
          costOfSet(signatories) +
          costOfSet(choiceObservers) +
          costOfOption(choiceAuthorizers) +
          costOfImmArray(children) +
          costOfOption(exerciseResult) +
          costOfOption(keyOpt) +
          costOfBoolean(byKey) +
          costOfSerializationVersion(version)
      case Node.Rollback(children) =>
        1 + costOfImmArray(children)
    }
  }
}
