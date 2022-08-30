// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.execution

import com.daml.ledger.participant.state.index.v2.{ContractStore, MaximumLedgerTime}
import com.daml.lf.command.DisclosedContract
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

/** Computes the maximum ledger time of all used contracts in a submission by:
  * * Using the client-provided disclosed contracts `createdAt` timestamp
  * * Falling back to contractStore lookups for contracts that have not been provided
  * as part of submissions' `disclosed_contracts`
  *
  * @param contractStore The contract store.
  */
class ResolveMaximumLedgerTime(contractStore: ContractStore) {
  def apply(
      usedDisclosedContracts: ImmArray[DisclosedContract],
      usedContractIds: Set[ContractId],
  )(implicit lc: LoggingContext): Future[MaximumLedgerTime] = {
    val usedDisclosedContractIds = usedDisclosedContracts.iterator.map(_.contractId).toSet

    val contractIdsToBeLookedUp = usedContractIds -- usedDisclosedContractIds

    contractStore
      .lookupMaximumLedgerTimeAfterInterpretation(contractIdsToBeLookedUp)
      .map(adjustTimeForDisclosedContracts(_, usedDisclosedContracts))(ExecutionContext.parasitic)
  }

  private def adjustTimeForDisclosedContracts(
      lookupMaximumLet: MaximumLedgerTime,
      disclosedContracts: ImmArray[DisclosedContract],
  ): MaximumLedgerTime =
    disclosedContracts.iterator
      .map(_.metadata.createdAt)
      .maxOption
      .fold(lookupMaximumLet)(adjust(lookupMaximumLet, _))

  private def adjust(
      lookedMaximumLet: MaximumLedgerTime,
      maxDisclosedContractTime: Timestamp,
  ): MaximumLedgerTime = lookedMaximumLet match {
    case MaximumLedgerTime.Max(maxUsedTime) =>
      MaximumLedgerTime.Max(Ordering[Timestamp].max(maxDisclosedContractTime, maxUsedTime))
    case MaximumLedgerTime.NotAvailable =>
      MaximumLedgerTime.Max(maxDisclosedContractTime)
    case other => other
  }
}
