// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.backend.api.v1

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.{Party, TransactionIdString}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst, VersionedValue}

/**
  * Contracts from a consistent ACS snapshot
  *
  * @param contractId id of the contract
  * @param let ledger effective time of the submitted transaction
  * @param transactionId id of the transaction that created this contract
  * @param workflowId workflow id of the transaction that created this contract
  * @param contract actual contract data
  * @param witnesses set of parties who are authorized to see this contract.
  */
case class ActiveContract(
    contractId: AbsoluteContractId,
    let: Instant,
    transactionId: TransactionIdString,
    workflowId: Option[WorkflowId],
    contract: ContractInst[VersionedValue[AbsoluteContractId]],
    witnesses: Set[Party])
