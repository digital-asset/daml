// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interfaces

import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.*

import scala.concurrent.Future

private[platform] trait LedgerDaoContractsReader {

  /** Looks up the contract by id at a specific ledger event sequential id.
    *
    * @param contractId the contract id to query
    * @param validAt the offset at which to resolve the contract state
    * @return the optional [[ContractState]]
    */
  def lookupContractState(contractId: ContractId, validAt: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ContractState]]

  /** Looks up the state of a contract key at a specific event sequential id.
    *
    * @param key the contract key to query
    * @param validAt the offset at which to resolve the key state
    * @return the [[KeyState]]
    */
  def lookupKeyState(key: GlobalKey, validAt: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[KeyState]
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
