// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.{FatContractInstance, GlobalKeyWithMaintainers, Node}
import com.digitalasset.daml.lf.value.Value

object FatContractInstanceHelper {

  def buildFatContractInstance(
      templateId: Ref.Identifier,
      packageName: Ref.PackageName,
      contractId: Value.ContractId,
      argument: Value,
      createdAt: Time.Timestamp,
      driverMetadata: Bytes,
      signatories: Set[Ref.Party],
      stakeholders: Set[Ref.Party],
      keyOpt: Option[GlobalKeyWithMaintainers],
      version: LanguageVersion,
  ): FatContractInstance = {
    val create = Node.Create(
      templateId = templateId,
      packageName = packageName,
      coid = contractId,
      arg = argument,
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = keyOpt,
      version = version,
    )
    FatContractInstance.fromCreateNode(create, createdAt, driverMetadata)
  }

}
