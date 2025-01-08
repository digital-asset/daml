// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.ProcessedDisclosedContract
import com.digitalasset.canton.ledger.participant.state.index.{
  MaximumLedgerTime,
  MaximumLedgerTimeService,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.value.Value.ContractId

import scala.concurrent.ExecutionContext

/** Computes the maximum ledger time of all used contracts in a submission by:
  * * Using the client-provided disclosed contracts `createdAt` timestamp
  * * Falling back to contractStore lookups for contracts that have not been provided
  * as part of submissions' `disclosed_contracts`
  *
  * @param maximumLedgerTimeService The MaximumLedgerTimeService.
  */
class ResolveMaximumLedgerTime(
    maximumLedgerTimeService: MaximumLedgerTimeService,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val directEc = DirectExecutionContext(noTracingLogger)

  def apply(
      processedDisclosedContracts: ImmArray[ProcessedDisclosedContract],
      usedContractIds: Set[ContractId],
  )(implicit
      lc: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[MaximumLedgerTime] = FutureUnlessShutdown.outcomeF {
    val usedDisclosedContractIds = processedDisclosedContracts.iterator.map(_.contractId).toSet

    val contractIdsToBeLookedUp = usedContractIds -- usedDisclosedContractIds

    maximumLedgerTimeService
      .lookupMaximumLedgerTimeAfterInterpretation(contractIdsToBeLookedUp)
      .map(adjustTimeForDisclosedContracts(_, processedDisclosedContracts))(directEc)
  }

  private def adjustTimeForDisclosedContracts(
      lookupMaximumLet: MaximumLedgerTime,
      processedDisclosedContracts: ImmArray[ProcessedDisclosedContract],
  ): MaximumLedgerTime =
    processedDisclosedContracts.iterator
      .map(_.createdAt)
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
