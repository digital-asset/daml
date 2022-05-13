// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger
package participant.state.v2

import com.daml.lf.data.Bytes
import com.daml.lf.data.Time
import com.daml.lf.value.Value

/** Represents an existing contract that has been disclosed out of band to the submitter of a transaction,
  * and is being used as "explicit disclosure" as part of the submission.
  *
  * This allows the submitter to use contracts that are not visible to the submitter.
  * The use of disclosed contracts still needs to pass Daml authorization rules, and the ledger is responsible for
  * validating that the supplied contract data is correct.
  *
  * @param contractId:          The contract identifier.
  * @param contractInst:        The contract instance.
  * @param ledgerEffectiveTime: The submission time of the transaction that created the contract,
  *                             see [[TransactionMeta.ledgerEffectiveTime]].
  * @param driverMetadata:      Opaque contract metadata assigned by the [[ReadService]] implementation.
  *                             Must be equal to the metadata sent in [[Update.TransactionAccepted]].
  */
final case class DisclosedContract(
    contractId: Value.ContractId,
    contractInst: Value.VersionedContractInstance, // TODO DPP-1026: Only use a Value, not a VersionedContractInstance
    ledgerEffectiveTime: Time.Timestamp,
    driverMetadata: Bytes,
)
