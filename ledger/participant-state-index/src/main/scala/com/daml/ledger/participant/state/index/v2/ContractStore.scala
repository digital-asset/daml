// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import com.daml.logging.LoggingContext

import scala.concurrent.Future

/** Meant be used for optimistic contract lookups before command submission.
  */
trait ContractStore {
  def lookupActiveContract(
      readers: Set[Ref.Party],
      contractId: ContractId,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[VersionedContractInstance]]

  def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]]

  /** This method serves two purposes:
    *   1 - Verify that none of the specified contracts are archived (archival of divulged contracts also count)
    *   2 - Calculate the maximum ledger time of all the specified contracts (divulged contracts do not contribute)
    * Important note: existence of the contracts is not checked, only the fact of archival.
    *
    * @return NotAvailable, if none of the specified contracts are archived, and all of them are divulged contracts (no ledger-time available)
    *         NotAvailable, if the specified set is empty.
    *         Max, if none of the specified contracts are archived, and the maximum ledger-time of all specified non-divulged contracts is known
    *         Archived, if there was at least one contract specified which is archived (this list is not necessarily exhaustive)
    */
  def lookupMaximumLedgerTimeAfterInterpretation(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[MaximumLedgerTime]

  // TODO DPP-1026: Check whether this should move to a new trait. ContractStore is meant to be used for command interpretation, not transaction validation.
  // Consider keeping only `lookupContracts(ids: Set[ContractId]): Future[Map[ContractId, (VersionedContractInstance, Timestamp)]]`,
  // and implementing `lookupMaximumLedgerTimeAfterInterpretation` as a pure logic on top of that lookup.
  /** Look up an active contract, ignoring contract visibility.
    *  This lookup will not return contracts that have only been divulged to this store.
    *  This is useful for validating transactions after submission.
    */
  def lookupContractAfterInterpretation(
      contractId: ContractId
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[(VersionedContractInstance, Timestamp)]]
}

/** The outcome of determining the maximum ledger time of a set of contracts.
  * Note that the ledger time may not be known for divulged contracts.
  */
sealed trait MaximumLedgerTime

object MaximumLedgerTime {

  /** None of the contracts is archived, but none has a known ledger time (also when no contracts specified). */
  case object NotAvailable extends MaximumLedgerTime

  /** None of the contracts is archived, and this is the maximum of the known ledger times. */
  final case class Max(ledgerTime: Timestamp) extends MaximumLedgerTime

  /** The given contracts are archived. The remaining contracts may or may not have a known ledger time. */
  final case class Archived(contracts: Set[ContractId]) extends MaximumLedgerTime

  def from(optionalMaximumLedgerTime: Option[Timestamp]): MaximumLedgerTime =
    optionalMaximumLedgerTime.map[MaximumLedgerTime](Max).getOrElse(NotAvailable)
}
