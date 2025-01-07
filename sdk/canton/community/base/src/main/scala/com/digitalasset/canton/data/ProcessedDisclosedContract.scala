// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{GlobalKeyWithMaintainers, Node}
import com.digitalasset.daml.lf.value.Value

/** An explicitly-disclosed contract that has been used during command interpretation
  * and enriched with additional contract metadata.
  *
  * @param create the create event of the contract
  * @param createdAt ledger effective time of the transaction that created the contract
  * @param driverMetadata opaque bytestring used by the underlying ledger implementation
  */
final case class ProcessedDisclosedContract(
    create: Node.Create,
    createdAt: Time.Timestamp,
    driverMetadata: Bytes,
) {
  def contractId: Value.ContractId = create.coid
  def templateId: Ref.TypeConName = create.templateId
}

object ProcessedDisclosedContract {
  // Helper builder for test
  def apply(
      templateId: Ref.Identifier,
      packageName: Ref.PackageName,
      packageVersion: Option[Ref.PackageVersion],
      contractId: Value.ContractId,
      argument: Value,
      createdAt: Time.Timestamp,
      driverMetadata: Bytes,
      signatories: Set[Ref.Party],
      stakeholders: Set[Ref.Party],
      keyOpt: Option[GlobalKeyWithMaintainers],
      version: LanguageVersion,
  ): ProcessedDisclosedContract =
    ProcessedDisclosedContract(
      create = Node.Create(
        templateId = templateId,
        packageName = packageName,
        packageVersion = packageVersion,
        coid = contractId,
        arg = argument,
        signatories = signatories,
        stakeholders = stakeholders,
        keyOpt = keyOpt,
        version = version,
      ),
      createdAt = createdAt,
      driverMetadata = driverMetadata,
    )
}
