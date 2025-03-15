// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
}
import com.digitalasset.daml.lf.value.Value.{ContractId, VersionedContractInstance}

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
      driverMetadata: Array[Byte],
  ) extends ContractState {
    def toFatContractInstance(coid: ContractId): FatContractInstance = {
      val ci = contractInstance.unversioned
      val globalKeyWithMaintainers = for {
        gk <- globalKey
        m <- maintainers
      } yield GlobalKeyWithMaintainers(gk, m)

      FatContractInstance.fromCreateNode(
        Node.Create(
          coid = coid,
          packageName = ci.packageName,
          packageVersion = None,
          templateId = ci.template,
          arg = ci.arg,
          signatories = signatories,
          stakeholders = stakeholders,
          keyOpt = globalKeyWithMaintainers,
          version = contractInstance.version,
        ),
        createTime = ledgerEffectiveTime,
        cantonData = Bytes.fromByteArray(driverMetadata),
      )
    }
  }
}
