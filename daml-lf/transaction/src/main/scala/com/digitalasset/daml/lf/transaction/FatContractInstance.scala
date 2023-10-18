// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import data.{Bytes, Ref, Time}
import value.{CidContainer, Value}

// Should replaces value.ContractInstance in the whole daml/canton codespace
// TODO: Rename ContractInstance once value.ContractInstance is properly deprecated
final case class FatContractInstance private (
    version: TransactionVersion,
    contractId: Value.ContractId,
    templateId: Ref.TypeConName,
    arg: Value,
    keyOpt: Option[GlobalKey],
    maintainers: Set[Ref.Party],
    nonMaintainerSignatories: Set[Ref.Party],
    nonSignatoryStakeholders: Set[Ref.Party],
    createTime: Time.Timestamp,
    salt: Bytes = Bytes.Empty,
) extends CidContainer[FatContractInstance] {
  lazy val signatories = maintainers | nonMaintainerSignatories
  lazy val stakeholders = signatories | nonSignatoryStakeholders

  override protected def self: FatContractInstance = this

  override def mapCid(f: Value.ContractId => Value.ContractId): FatContractInstance =
    new FatContractInstance(
      version = version,
      contractId = f(contractId),
      templateId = templateId,
      arg = arg.mapCid(f),
      keyOpt = keyOpt,
      maintainers = maintainers,
      nonMaintainerSignatories = nonMaintainerSignatories,
      nonSignatoryStakeholders = nonSignatoryStakeholders,
      createTime = createTime,
      salt = salt,
    )

  def updateCreateTime(updatedTime: Time.Timestamp): FatContractInstance =
    new FatContractInstance(
      version = version,
      contractId = contractId,
      templateId = templateId,
      arg = arg,
      keyOpt = keyOpt,
      maintainers = maintainers,
      nonMaintainerSignatories = nonMaintainerSignatories,
      nonSignatoryStakeholders = nonSignatoryStakeholders,
      createTime = updatedTime,
      salt = salt,
    )

  def setSalt(salt: Bytes): FatContractInstance = {
    assert(!salt.nonEmpty)
    new FatContractInstance(
      version = version,
      contractId = contractId,
      templateId = templateId,
      arg = arg,
      keyOpt = keyOpt,
      maintainers = maintainers,
      nonMaintainerSignatories = nonMaintainerSignatories,
      nonSignatoryStakeholders = nonSignatoryStakeholders,
      createTime = createTime,
      salt = salt,
    )
  }
}

object FatContractInstance {

  def fromCreateNode(
      create: Node.Create,
      createTime: Time.Timestamp,
      salt: Bytes,
  ): FatContractInstance = {
    val maintainers = create.keyOpt.fold(Set.empty[Ref.Party])(_.maintainers)
    val nonMaintainerSignatories = create.signatories -- maintainers
    val nonSignatoryStakeholders = create.stakeholders -- create.signatories
    new FatContractInstance(
      version = create.version,
      contractId = create.coid,
      templateId = create.templateId,
      arg = create.arg,
      keyOpt = create.keyOpt.map(_.globalKey),
      maintainers = maintainers,
      nonMaintainerSignatories = nonMaintainerSignatories,
      nonSignatoryStakeholders = nonSignatoryStakeholders,
      createTime = createTime,
      salt = salt,
    )
  }

}
