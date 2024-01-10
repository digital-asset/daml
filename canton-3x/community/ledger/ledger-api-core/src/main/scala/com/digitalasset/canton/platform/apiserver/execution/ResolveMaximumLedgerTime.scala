// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.ProcessedDisclosedContract
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  MaximumLedgerTime,
  MaximumLedgerTimeService,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}

import scala.concurrent.Future

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
  )(implicit lc: LoggingContextWithTrace): Future[MaximumLedgerTime] = {
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
