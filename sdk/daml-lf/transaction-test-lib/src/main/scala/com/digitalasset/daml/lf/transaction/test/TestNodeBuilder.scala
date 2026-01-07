// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction
package test

import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder.{
  CreateKey,
  CreateSerializationVersion,
}
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName, Party, TypeConId}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.{
  GlobalKeyWithMaintainers,
  Node,
  NodeId,
  SerializationVersion,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId

trait TestNodeBuilder {

  def serializationVersion(packageId: PackageId): Option[SerializationVersion] = None

  private def assertPackageVersion(packageId: PackageId): SerializationVersion =
    serializationVersion(packageId).getOrElse(
      throw new IllegalArgumentException(s"Could not lookup serialization version for $packageId")
    )

  private[this] def contractSerializationVersion(contract: Node.Create): SerializationVersion =
    serializationVersion(contract.templateId.packageId).getOrElse(contract.version)

  val defaultPackageName: Ref.PackageName =
    Ref.PackageName.assertFromString("-default-package-name-")

  def create(
      id: ContractId,
      templateId: TypeConId,
      argument: Value,
      signatories: Set[Party],
      observers: Set[Party] = Set.empty,
      key: CreateKey = CreateKey.NoKey,
      // TODO https://github.com/digital-asset/daml/issues/17995
      packageName: PackageName = defaultPackageName,
      version: CreateSerializationVersion = CreateSerializationVersion.StableMax,
  ): Node.Create = {

    val serializationVersion = version match {
      case CreateSerializationVersion.StableMax => SerializationVersion.StableVersions.max
      case CreateSerializationVersion.FromPackage => assertPackageVersion(templateId.packageId)
      case CreateSerializationVersion.Version(version) => version
    }

    val keyOpt = key match {
      case CreateKey.NoKey =>
        None
      case CreateKey.SignatoryMaintainerKey(value) =>
        Some(
          GlobalKeyWithMaintainers.assertBuild(templateId, value, signatories, packageName)
        )
      case CreateKey.KeyWithMaintainers(value, maintainers) =>
        Some(
          GlobalKeyWithMaintainers.assertBuild(templateId, value, maintainers, packageName)
        )
    }

    val maintainers: Set[Party] = keyOpt.fold(Set.empty[Party])(_.maintainers)

    Node.Create(
      coid = id,
      packageName = packageName,
      templateId = templateId,
      arg = argument,
      signatories = signatories ++ maintainers,
      stakeholders = signatories ++ observers ++ maintainers,
      keyOpt = keyOpt,
      version = serializationVersion,
    )
  }

  def exercise(
      contract: Node.Create,
      choice: Ref.Name,
      consuming: Boolean,
      actingParties: Set[Ref.Party],
      argument: Value,
      byKey: Boolean,
      interfaceId: Option[Ref.TypeConId] = None,
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
      version = contractSerializationVersion(contract),
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
      version = contractSerializationVersion(contract),
      interfaceId = None,
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
      version = contractSerializationVersion(contract),
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

  sealed trait CreateSerializationVersion
  object CreateSerializationVersion {
    case object StableMax extends CreateSerializationVersion
    case object FromPackage extends CreateSerializationVersion
    final case class Version(version: SerializationVersion) extends CreateSerializationVersion
  }

}
