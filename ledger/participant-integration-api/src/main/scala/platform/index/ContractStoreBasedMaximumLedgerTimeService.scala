// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.ledger.participant.state.index.v2.{
  ContractState,
  ContractStore,
  MaximumLedgerTime,
  MaximumLedgerTimeService,
}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.scalaUtilChainingOps

class ContractStoreBasedMaximumLedgerTimeService(contractStore: ContractStore)
    extends MaximumLedgerTimeService {

  override def lookupMaximumLedgerTimeAfterInterpretation(
      ids: Set[Value.ContractId]
  )(implicit loggingContext: LoggingContext): Future[MaximumLedgerTime] = {
    def goAsync(
        maximumLedgerTime: Option[Timestamp],
        contractIds: List[ContractId],
    ): Future[MaximumLedgerTime] =
      (maximumLedgerTime, contractIds) match {
        case (result, Nil) =>
          Future.successful(MaximumLedgerTime.from(result))

        case (resultSoFar, contractId :: otherContractIds) =>
          contractStore
            .lookupContractStateWithoutDivulgence(contractId)
            .flatMap {
              case ContractState.NotFound =>
                // If cannot be found: no create or archive event for the contract.
                // Since this contract is part of the input, it was able to be looked up once.
                // So this is the case of a divulged contract, which was not archived.
                // Divulged contract does not change maximumLedgerTime
                goAsync(maximumLedgerTime, otherContractIds)

              case ContractState.Archived =>
                // early termination on the first archived contract in sight
                Future.successful(MaximumLedgerTime.Archived(Set(contractId)))

              case active: ContractState.Active =>
                val newMaximumLedgerTime = resultSoFar
                  .getOrElse(Timestamp.MinValue)
                  .pipe(Ordering[Timestamp].max(_, active.ledgerEffectiveTime))
                  .pipe(Some(_))
                goAsync(newMaximumLedgerTime, otherContractIds)
            }(ExecutionContext.parasitic)
      }

    goAsync(None, ids.toList)
  }
}
