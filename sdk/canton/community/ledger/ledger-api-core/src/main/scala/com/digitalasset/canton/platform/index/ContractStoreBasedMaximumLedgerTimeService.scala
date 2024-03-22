// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import com.daml.lf.data.Time.Timestamp
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  ContractState,
  ContractStore,
  MaximumLedgerTime,
  MaximumLedgerTimeService,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}

import scala.concurrent.Future
import scala.util.chaining.scalaUtilChainingOps

class ContractStoreBasedMaximumLedgerTimeService(
    contractStore: ContractStore,
    override protected val loggerFactory: NamedLoggerFactory,
) extends MaximumLedgerTimeService
    with NamedLogging {

  private val directEc = DirectExecutionContext(noTracingLogger)

  override def lookupMaximumLedgerTimeAfterInterpretation(
      ids: Set[Value.ContractId]
  )(implicit loggingContext: LoggingContextWithTrace): Future[MaximumLedgerTime] = {
    def goAsync(
        maximumLedgerTime: Option[Timestamp],
        contractIds: List[ContractId],
    ): Future[MaximumLedgerTime] =
      (maximumLedgerTime, contractIds) match {
        case (result, Nil) =>
          Future.successful(MaximumLedgerTime.from(result))

        case (resultSoFar, contractId :: otherContractIds) =>
          contractStore
            .lookupContractState(contractId)
            .flatMap {
              case ContractState.Archived | ContractState.NotFound =>
                // early termination on the first archived contract in sight
                Future.successful(MaximumLedgerTime.Archived(Set(contractId)))

              case active: ContractState.Active =>
                val newMaximumLedgerTime = resultSoFar
                  .getOrElse(Timestamp.MinValue)
                  .pipe(Ordering[Timestamp].max(_, active.ledgerEffectiveTime))
                  .pipe(Some(_))
                goAsync(newMaximumLedgerTime, otherContractIds)
            }(directEc)
      }

    goAsync(None, ids.toList)
  }
}
