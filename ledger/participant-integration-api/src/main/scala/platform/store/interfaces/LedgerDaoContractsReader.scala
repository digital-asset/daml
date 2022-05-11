// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.interfaces

import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.daml.logging.LoggingContext
import com.daml.platform.store.interfaces.LedgerDaoContractsReader._
import com.daml.platform.Party
import com.daml.ledger.offset.Offset

import scala.concurrent.Future

private[platform] trait LedgerDaoContractsReader {

  /** Looks up an active or divulged contract if it is visible for the given party.
    *
    * @param readers a set of parties for one of which the contract must be visible
    * @param contractId the contract id to query
    * @return the optional [[Contract]] value
    */
  def lookupActiveContractAndLoadArgument(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]]

  /** Looks up an active or divulged contract if it is visible for the given party.
    * This method uses the provided create argument for building the [[Contract]] value
    * instead of decoding it again.
    *
    * @param readers a set of parties for one of which the contract must be visible
    * @param contractId the contract id to query
    * @param createArgument the contract create argument
    * @return the optional [[Contract]] value
    */
  def lookupActiveContractWithCachedArgument(
      readers: Set[Party],
      contractId: ContractId,
      createArgument: Value,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]]

  /** Looks up the contract by id at a specific ledger event sequential id.
    *
    * @param contractId the contract id to query
    * @param validAt the offset at which to resolve the contract state
    * @return the optional [[ContractState]]
    */
  def lookupContractState(contractId: ContractId, validAt: Offset)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractState]]

  /** Looks up the state of a contract key at a specific event sequential id.
    *
    * @param key the contract key to query
    * @param validAt the offset at which to resolve the key state
    * @return the [[KeyState]]
    */
  def lookupKeyState(key: GlobalKey, validAt: Offset)(implicit
      loggingContext: LoggingContext
  ): Future[KeyState]
}

object LedgerDaoContractsReader {
  import com.daml.lf.value.{Value => lfval}
  private type ContractId = lfval.ContractId
  private type Value = lfval.VersionedValue
  private type Contract = lfval.VersionedContractInstance

  sealed trait ContractState extends Product with Serializable {
    def stakeholders: Set[Party]
  }

  final case class ActiveContract(
      contract: Contract,
      stakeholders: Set[Party],
      ledgerEffectiveTime: Timestamp,
  ) extends ContractState

  final case class ArchivedContract(stakeholders: Set[Party]) extends ContractState

  sealed trait KeyState extends Product with Serializable

  final case class KeyAssigned(contractId: ContractId, stakeholders: Set[Party]) extends KeyState

  final case object KeyUnassigned extends KeyState
}
