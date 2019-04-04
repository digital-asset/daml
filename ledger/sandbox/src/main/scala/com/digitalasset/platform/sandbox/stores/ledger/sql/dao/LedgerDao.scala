// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.dao

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst, VersionedValue}
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry

import scala.collection.immutable
import scala.concurrent.Future

final case class Contract(
    contractId: AbsoluteContractId,
    let: Instant,
    transactionId: String,
    workflowId: String,
    witnesses: Set[Ref.Party],
    coinst: ContractInst[VersionedValue[AbsoluteContractId]])

trait LedgerDao {

  /** Looks up the ledger id */
  def lookupLedgerId(): Future[Option[String]]

  /** Looks up the current ledger end */
  def lookupLedgerEnd(): Future[Long]

  /** Looks up an active contract. Archived contracts must not be returned by this method */
  def lookupActiveContract(contractId: AbsoluteContractId): Future[Option[Contract]]

  /**
    * Looks up a LedgerEntry at a given offset
    *
    * @param offset the offset to look at
    * @return the optional LedgerEntry found
    */
  def lookupLedgerEntry(offset: Long): Future[Option[LedgerEntry]]

  /**
    * Looks up a LedgerEntry at a given offset
    *
    * @param offset the offset to look at
    * @return the LedgerEntry found, or throws an exception
    */
  def lookupLedgerEntryAssert(offset: Long): Future[LedgerEntry] =
    lookupLedgerEntry(offset).map(
      _.getOrElse(sys.error(s"ledger entry not found for offset: $offset")))(DirectExecutionContext)

  /**
    * Stores the initial ledger end. Can be called only once.
    *
    * @param ledgerEnd the ledger end to be stored
    */
  def storeInitialLedgerEnd(ledgerEnd: Long): Future[Unit]

  /**
    * Stores the ledger id. Can be called only once.
    *
    * @param ledgerId the ledger id to be stored
    */
  def storeLedgerId(ledgerId: String): Future[Unit]

  /**
    * Stores an active contract
    *
    * @param contract the contract to be stored
    */
  def storeContract(contract: Contract): Future[Unit]

  /**
    * Stores a collection of active contracts. The query is executed as a batch insert.
    *
    * @param contracts the collection of contracts to be stored
    */
  def storeContracts(contracts: immutable.Seq[Contract]): Future[Unit]

  /**
    * Stores a ledger entry. The ledger end gets updated as well in the same transaction.
    *
    * @param offset the offset to store the ledger entry
    * @param ledgerEntry the LedgerEntry to be stored
    */
  def storeLedgerEntry(offset: Long, ledgerEntry: LedgerEntry): Future[Unit]

}
