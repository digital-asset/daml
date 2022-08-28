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

class ResolveMaximumLedgerTime(contractStore: ContractStore) {
  def apply(
      disclosedContracts: ImmArray[DisclosedContract],
      // TODO ED: Consider extracting from CER
      usedContractIds: Set[ContractId],
  )(implicit lc: LoggingContext): Future[MaximumLedgerTime] = {
    val usedDisclosedContracts = disclosedContracts.toSeq.toSet
    val usedDisclosedContractIds = usedDisclosedContracts.map(_.contractId)
    val usedLocalContractIds = usedContractIds -- usedDisclosedContractIds

    contractStore
      .lookupMaximumLedgerTimeAfterInterpretation(usedLocalContractIds)
      .map(adjustTimeForDisclosedContracts(_, usedDisclosedContracts))(ExecutionContext.parasitic)
  }

  private[this] def adjustTimeForDisclosedContracts(
      result: MaximumLedgerTime,
      disclosedContracts: Set[DisclosedContract],
  ): MaximumLedgerTime =
    if (disclosedContracts.isEmpty) {
      result
    } else {
      val maxDisclosedContractTime = disclosedContracts.map(_.metadata.createdAt).max
      result match {
        case MaximumLedgerTime.Max(maxUsedTime) =>
          MaximumLedgerTime.Max(
            implicitly[Ordering[Timestamp]].max(maxDisclosedContractTime, maxUsedTime)
          )
        case MaximumLedgerTime.NotAvailable =>
          MaximumLedgerTime.Max(maxDisclosedContractTime)
        case other => other
      }
    }
}
