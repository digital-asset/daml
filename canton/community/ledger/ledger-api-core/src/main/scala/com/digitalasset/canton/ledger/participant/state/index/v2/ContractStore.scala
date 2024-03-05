// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

/** Meant be used for optimistic contract lookups before command submission.
  */
trait ContractStore {

  /** Looking up an active contract.
    */
  def lookupActiveContract(
      readers: Set[Ref.Party],
      contractId: ContractId,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[VersionedContractInstance]]

  def lookupContractKey(readers: Set[Party], key: GlobalKey)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ContractId]]

  /** Querying the state of the contracts.
    */
  def lookupContractState(
      contractId: ContractId
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[ContractState]
}

sealed trait ContractState

object ContractState {
  case object NotFound extends ContractState
  case object Archived extends ContractState
  final case class Active(
      contractInstance: VersionedContractInstance,
      ledgerEffectiveTime: Timestamp,
      stakeholders: Set[Party],
      signatories: Set[Party],
      globalKey: Option[GlobalKey],
      maintainers: Option[Set[Party]],
      driverMetadata: Option[Array[Byte]],
  ) extends ContractState
}
