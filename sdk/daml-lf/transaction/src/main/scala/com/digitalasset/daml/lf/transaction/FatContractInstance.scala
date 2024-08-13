// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.value.{CidContainer, Value, ValueReVersioner}

import scala.collection.immutable.TreeSet

// This should replace value.ContractInstance in the whole daml/canton codespace
// TODO: Rename to ContractInstance once value.ContractInstance is properly deprecated
sealed abstract class FatContractInstance extends CidContainer[FatContractInstance] {
  val version: TransactionVersion
  val contractId: Value.ContractId
  val packageName: Ref.PackageName
  val packageVersion: Option[Ref.PackageVersion]
  val templateId: Ref.TypeConName
  val createArg: Value
  val signatories: TreeSet[Ref.Party]
  val stakeholders: TreeSet[Ref.Party]
  val contractKeyWithMaintainers: Option[GlobalKeyWithMaintainers]
  val createdAt: Time.Timestamp
  val cantonData: Bytes
  private[lf] def toImplementation: FatContractInstanceImpl =
    this.asInstanceOf[FatContractInstanceImpl]
  final lazy val maintainers: TreeSet[Ref.Party] =
    contractKeyWithMaintainers.fold(TreeSet.empty[Ref.Party])(k => TreeSet.from(k.maintainers))
  final lazy val nonMaintainerSignatories: TreeSet[Ref.Party] = signatories -- maintainers
  final lazy val nonSignatoryStakeholders: TreeSet[Ref.Party] = stakeholders -- signatories
  def updateCreateAt(updatedTime: Time.Timestamp): FatContractInstance
  def setSalt(cantonData: Bytes): FatContractInstance
  def reVersion(target: TransactionVersion): Either[String, FatContractInstance]

  def toCreateNode = Node.Create(
    coid = contractId,
    packageName = packageName,
    packageVersion = packageVersion,
    templateId = templateId,
    arg = createArg,
    signatories = signatories,
    stakeholders = stakeholders,
    keyOpt = contractKeyWithMaintainers,
    version = version,
  )
}

private[lf] final case class FatContractInstanceImpl(
    version: TransactionVersion,
    contractId: Value.ContractId,
    packageName: Ref.PackageName,
    packageVersion: Option[Ref.PackageVersion],
    templateId: Ref.TypeConName,
    createArg: Value,
    signatories: TreeSet[Ref.Party],
    stakeholders: TreeSet[Ref.Party],
    contractKeyWithMaintainers: Option[GlobalKeyWithMaintainers],
    createdAt: Time.Timestamp,
    cantonData: Bytes,
) extends FatContractInstance
    with CidContainer[FatContractInstanceImpl] {

  // TODO (change implementation of KeyWithMaintainers.maintainer to TreeSet)
  require(maintainers.isInstanceOf[TreeSet[Ref.Party]], "maintainers should be a TreeSet")
  require(maintainers.subsetOf(signatories), "maintainers should be a subset of signatories")
  require(signatories.nonEmpty, "signatories should be non empty")
  require(signatories.subsetOf(stakeholders), "signatories should be a subset of stakeholders")

  override protected def self: FatContractInstanceImpl = this

  override def mapCid(f: Value.ContractId => Value.ContractId): FatContractInstanceImpl = {
    copy(
      contractId = f(contractId),
      createArg = createArg.mapCid(f),
    )
  }

  override def updateCreateAt(updatedTime: Time.Timestamp): FatContractInstanceImpl =
    copy(createdAt = updatedTime)

  override def setSalt(cantonData: Bytes): FatContractInstanceImpl = {
    assert(cantonData.nonEmpty)
    copy(cantonData = cantonData)
  }

  def reVersion(target: TransactionVersion): Either[String, FatContractInstance] = {

    def reV(v: Value): Either[String, Value] =
      ValueReVersioner.reVersionValue(Versioned(version, v), target).map(_.unversioned)

    for {
      reCreateArg <- reV(createArg)
      reContractKeyWithMaintainers <- (contractKeyWithMaintainers
        .fold[Either[String, Option[GlobalKeyWithMaintainers]]](
          Right[String, Option[GlobalKeyWithMaintainers]](None)
        )(ck =>
          for {
            reCkValue <- reV(ck.value)
            reCk <- GlobalKeyWithMaintainers.withRenormalizedValue(ck, reCkValue)
          } yield Some(reCk)
        ))
    } yield {
      copy(
        version = target,
        createArg = reCreateArg,
        contractKeyWithMaintainers = reContractKeyWithMaintainers,
      )
    }
  }

}

object FatContractInstance {

  def fromCreateNode(
      create: Node.Create,
      createTime: Time.Timestamp,
      cantonData: Bytes,
  ): FatContractInstance =
    FatContractInstanceImpl(
      version = create.version,
      contractId = create.coid,
      packageName = create.packageName,
      packageVersion = create.packageVersion,
      templateId = create.templateId,
      createArg = create.arg,
      signatories = TreeSet.from(create.signatories),
      stakeholders = TreeSet.from(create.stakeholders),
      contractKeyWithMaintainers =
        create.keyOpt.map(k => k.copy(maintainers = TreeSet.from(k.maintainers))),
      createdAt = createTime,
      cantonData = cantonData,
    )

}
