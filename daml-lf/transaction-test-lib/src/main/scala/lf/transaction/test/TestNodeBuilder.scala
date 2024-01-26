// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction
package test

import com.daml.lf.transaction.test.TestNodeBuilder.{CreateKey, CreateTransactionVersion}
import com.daml.lf.data.Ref.{PackageId, PackageName, Party, TypeConName}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.{GlobalKeyWithMaintainers, Node, NodeId, TransactionVersion}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

import Ordering.Implicits._

trait TestNodeBuilder {

  def packageVersion(packageId: PackageId): Option[TransactionVersion] = None

  private def assertPackageVersion(packageId: PackageId): TransactionVersion =
    packageVersion(packageId).getOrElse(
      throw new IllegalArgumentException(s"Could not lookup transaction version for $packageId")
    )

  private def contractPackageVersion(contract: Node.Create): TransactionVersion =
    packageVersion(contract.templateId.packageId).getOrElse(contract.version)

  def create(
      id: ContractId,
      templateId: TypeConName,
      argument: Value,
      signatories: Set[Party],
      observers: Set[Party] = Set.empty,
      key: CreateKey = CreateKey.NoKey,
      // TODO: https://github.com/digital-asset/daml/issues/17995
      //  review if we should really provide a defaul package name.
      packageName: PackageName = Ref.PackageName.assertFromString("package-name"),
      version: CreateTransactionVersion = CreateTransactionVersion.StableMax,
  ): Node.Create = {

    val transactionVersion = version match {
      case CreateTransactionVersion.StableMax => TransactionVersion.StableVersions.max
      case CreateTransactionVersion.FromPackage => assertPackageVersion(templateId.packageId)
      case CreateTransactionVersion.Version(version) => version
    }

    val keyOpt = key match {
      case CreateKey.NoKey =>
        None
      case CreateKey.SignatoryMaintainerKey(value) =>
        Some(
          GlobalKeyWithMaintainers.assertBuild(
            templateId,
            value,
            signatories,
            Util.sharedKey(transactionVersion),
          )
        )
      case CreateKey.KeyWithMaintainers(value, maintainers) =>
        Some(
          GlobalKeyWithMaintainers.assertBuild(
            templateId,
            value,
            maintainers,
            Util.sharedKey(transactionVersion),
          )
        )
    }

    val maintainers: Set[Party] = keyOpt.fold(Set.empty[Party])(_.maintainers)

    Node.Create(
      coid = id,
      packageName =
        if (transactionVersion < TransactionVersion.minUpgrade) None else Some(packageName),
      templateId = templateId,
      arg = argument,
      agreementText = "", // to be removed
      signatories = signatories ++ maintainers,
      stakeholders = signatories ++ observers ++ maintainers,
      keyOpt = keyOpt,
      version = transactionVersion,
    )
  }

  def exercise(
      contract: Node.Create,
      choice: Ref.Name,
      consuming: Boolean,
      actingParties: Set[Ref.Party],
      argument: Value,
      byKey: Boolean,
      interfaceId: Option[Ref.TypeConName] = None,
      result: Option[Value] = None,
      choiceObservers: Set[Ref.Party] = Set.empty,
      children: ImmArray[NodeId] = ImmArray.empty,
  ): Node.Exercise =
    Node.Exercise(
      choiceObservers = choiceObservers,
      choiceAuthorizers = None,
      targetCoid = contract.coid,
      packageName = contract.packageName,
      templateId = contract.templateId,
      interfaceId = interfaceId,
      choiceId = choice,
      consuming = consuming,
      actingParties = actingParties,
      chosenValue = argument,
      stakeholders = contract.stakeholders,
      signatories = contract.signatories,
      children = children,
      exerciseResult = result,
      keyOpt = contract.keyOpt,
      byKey = byKey,
      version = contractPackageVersion(contract),
    )

  def fetch(
      contract: Node.Create,
      byKey: Boolean,
  ): Node.Fetch =
    Node.Fetch(
      coid = contract.coid,
      packageName = contract.packageName,
      templateId = contract.templateId,
      actingParties = contract.signatories.map(Ref.Party.assertFromString),
      signatories = contract.signatories,
      stakeholders = contract.stakeholders,
      keyOpt = contract.keyOpt,
      byKey = byKey,
      version = contractPackageVersion(contract),
    )

  def lookupByKey(contract: Node.Create, found: Boolean = true): Node.LookupByKey =
    Node.LookupByKey(
      packageName = contract.packageName,
      templateId = contract.templateId,
      key = contract.keyOpt.getOrElse(
        throw new IllegalArgumentException(
          "Cannot lookup by key a contract that does not have a key"
        )
      ),
      result = if (found) Some(contract.coid) else None,
      version = contractPackageVersion(contract),
    )

  def rollback(children: ImmArray[NodeId] = ImmArray.empty): Node.Rollback =
    Node.Rollback(children)

}

object TestNodeBuilder extends TestNodeBuilder {

  sealed trait CreateKey
  object CreateKey {
    case object NoKey extends CreateKey
    final case class SignatoryMaintainerKey(value: Value) extends CreateKey
    final case class KeyWithMaintainers(value: Value, maintainers: Set[Party]) extends CreateKey
  }

  sealed trait CreateTransactionVersion
  object CreateTransactionVersion {
    case object StableMax extends CreateTransactionVersion
    case object FromPackage extends CreateTransactionVersion
    final case class Version(version: TransactionVersion) extends CreateTransactionVersion
  }

}
