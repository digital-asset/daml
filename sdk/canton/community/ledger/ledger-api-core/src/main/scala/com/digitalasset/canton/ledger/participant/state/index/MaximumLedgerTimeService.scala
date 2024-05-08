// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.daml.lf.data.Time.Timestamp
import com.daml.lf.value.Value.ContractId
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

trait MaximumLedgerTimeService {

  /** This method serves two purposes:
    *   1 - Verify that none of the specified contracts are archived
    *   2 - Calculate the maximum ledger time of all the specified contracts
    * Important note: existence of the contracts is not checked, only the fact of archival, therefore this method
    * is intended to be used after interpretation, which guarantees that all the used ids were visible once.
    *
    * @return NotAvailable, if the specified set is empty.
    *         Max, if none of the specified contracts are archived, and the maximum ledger-time of all specified contracts is known
    *         Archived, if there was at least one contract specified which is archived (this list is not necessarily exhaustive)
    */
  def lookupMaximumLedgerTimeAfterInterpretation(ids: Set[ContractId])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[MaximumLedgerTime]
}

/** The outcome of determining the maximum ledger time of a set of contracts.
  */
sealed trait MaximumLedgerTime

object MaximumLedgerTime {

  /** None of the contracts is archived, but none has a known ledger time (also when no contracts specified). */
  case object NotAvailable extends MaximumLedgerTime

  /** None of the contracts is archived, and this is the maximum of the known ledger times. */
  final case class Max(ledgerTime: Timestamp) extends MaximumLedgerTime

  /** The given contracts are archived. The remaining contracts may or may not have a known ledger time.
    * At least one contract specified which is archived, but this Set is not necessarily exhaustive.
    */
  final case class Archived(contracts: Set[ContractId]) extends MaximumLedgerTime

  def from(optionalMaximumLedgerTime: Option[Timestamp]): MaximumLedgerTime =
    optionalMaximumLedgerTime.fold[MaximumLedgerTime](NotAvailable)(Max)
}
