// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import data.{Bytes, Ref, Time}
import value.{CidContainer, Value}

// This should replace value.ContractInstance in the whole daml/canton codespace
// TODO: Rename to ContractInstance once value.ContractInstance is properly deprecated
sealed abstract class FatContractInstance extends Product with CidContainer[FatContractInstance] {
  val version: TransactionVersion
  val contractId: Value.ContractId
  val templateId: Ref.TypeConName
  val createArg: Value
  val contractKey: Option[GlobalKey]
  val maintainers: Set[Ref.Party]
  val nonMaintainerSignatories: Set[Ref.Party]
  val nonSignatoryStakeholders: Set[Ref.Party]
  val createTime: Time.Timestamp
  val cantonData: Bytes
  private[lf] def toImplementation: FatContractInstanceImpl =
    this.asInstanceOf[FatContractInstanceImpl]
  require(maintainers.isEmpty || contractKey.isDefined)
}

private[lf] final case class FatContractInstanceImpl(
    version: TransactionVersion,
    contractId: Value.ContractId,
    templateId: Ref.TypeConName,
    createArg: Value,
    contractKey: Option[GlobalKey],
    maintainers: Set[Ref.Party],
    nonMaintainerSignatories: Set[Ref.Party],
    nonSignatoryStakeholders: Set[Ref.Party],
    createTime: Time.Timestamp,
    cantonData: Bytes,
) extends FatContractInstance
    with CidContainer[FatContractInstanceImpl] {
  lazy val signatories = maintainers | nonMaintainerSignatories
  lazy val stakeholders = signatories | nonSignatoryStakeholders

  override protected def self: FatContractInstanceImpl = this

  override def mapCid(f: Value.ContractId => Value.ContractId): FatContractInstanceImpl = {
    copy(
      contractId = f(contractId),
      createArg = createArg.mapCid(f),
    )
  }

  def updateCreateTime(updatedTime: Time.Timestamp): FatContractInstance =
    copy(createTime = updatedTime)

  def setSalt(canton_data: Bytes): FatContractInstance = {
    assert(canton_data.nonEmpty)
    copy(cantonData = canton_data)
  }
}

object FatContractInstance {

  def fromCreateNode(
      create: Node.Create,
      createTime: Time.Timestamp,
      canton_data: Bytes,
  ): FatContractInstance = {
    val maintainers = create.keyOpt.fold(Set.empty[Ref.Party])(_.maintainers)
    val nonMaintainerSignatories = create.signatories -- maintainers
    val nonSignatoryStakeholders = create.stakeholders -- create.signatories
    FatContractInstanceImpl(
      version = create.version,
      contractId = create.coid,
      templateId = create.templateId,
      createArg = create.arg,
      contractKey = create.keyOpt.map(_.globalKey),
      maintainers = maintainers,
      nonMaintainerSignatories = nonMaintainerSignatories,
      nonSignatoryStakeholders = nonSignatoryStakeholders,
      createTime = createTime,
      cantonData = canton_data,
    )
  }

}
