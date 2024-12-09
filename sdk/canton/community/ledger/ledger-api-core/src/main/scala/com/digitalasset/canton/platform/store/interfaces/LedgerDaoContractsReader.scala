// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interfaces

import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.*
import com.google.common.annotations.VisibleForTesting

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
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[Contract]]

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
      packageName: Option[Ref.PackageName],
      createArgument: Value,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[Contract]]

  /** Looks up the contract by id
    *
    * Due to batching of several requests, we may return newer information than at the provided offset, but never
    * older information.
    *
    * @param contractId the contract id to query
    * @param notEarlierThanOffset the offset threshold to resolve the contract state (state can be newer, but not older)
    * @return the optional [[ContractState]]
    */
  def lookupContractState(contractId: ContractId, notEarlierThanOffset: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ContractState]]

  /** Looks up the state of a contract key
    *
    * Due to batching of several requests, we may return newer information than at the provided offset, but never
    * older information.
    *
    * @param key the contract key to query
    * @param notEarlierThanOffset the offset threshold to resolve the key state (state can be newer, but not older)
    * @return the [[KeyState]]
    */
  def lookupKeyState(key: GlobalKey, notEarlierThanOffset: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[KeyState]

  /** Batch lookup of contract keys
    *
    * Used to unit test the SQL queries for key lookups. Does not use batching.
    */
  @VisibleForTesting
  def lookupKeyStatesFromDb(keys: Seq[GlobalKey], notEarlierThanOffset: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Map[GlobalKey, KeyState]]

}

object LedgerDaoContractsReader {
  import com.daml.lf.value.Value as lfval
  private type ContractId = lfval.ContractId
  private type Value = lfval.VersionedValue
  private type Contract = lfval.VersionedContractInstance

  sealed trait ContractState extends Product with Serializable {
    def stakeholders: Set[Party]
  }

  // Note that for TransactionVersion <= V15 maintainers may not be populated even where globalKey is
  final case class ActiveContract(
      contract: Contract,
      stakeholders: Set[Party],
      ledgerEffectiveTime: Timestamp,
      agreementText: Option[String],
      signatories: Set[Party],
      globalKey: Option[GlobalKey],
      keyMaintainers: Option[Set[Party]],
      driverMetadata: Option[Array[Byte]],
  ) extends ContractState

  final case class ArchivedContract(stakeholders: Set[Party]) extends ContractState

  sealed trait KeyState extends Product with Serializable

  final case class KeyAssigned(contractId: ContractId, stakeholders: Set[Party]) extends KeyState

  final case object KeyUnassigned extends KeyState
}
