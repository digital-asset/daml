// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.transaction.Transaction.NodeId
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.EventId
import com.digitalasset.platform.events.EventIdFormatter
import com.digitalasset.platform.store.ReadOnlyLedger

import scala.concurrent.Future

trait Ledger extends ReadOnlyLedger {

  def publishHeartbeat(time: Instant): Future[Unit]

  def publishTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction
  ): Future[SubmissionResult]

  // Party management
  def publishPartyAllocation(
      submissionId: SubmissionId,
      party: Party,
      displayName: Option[String]
  ): Future[SubmissionResult]

  // Package management
  def uploadPackages(
      submissionId: SubmissionId,
      knownSince: Instant,
      sourceDescription: Option[String],
      payload: List[Archive]
  ): Future[SubmissionResult]

  // Configuration management
  def publishConfiguration(
      maxRecordTime: Timestamp,
      submissionId: String,
      config: Configuration
  ): Future[SubmissionResult]

}

object Ledger {

  type TransactionForIndex =
    GenTransaction[EventId, AbsoluteContractId, Value.VersionedValue[AbsoluteContractId]]
  type DisclosureForIndex = Map[EventId, Set[Party]]
  type GlobalDivulgence = Relation[AbsoluteContractId, Party]

  def convertToCommittedTransaction(transactionId: TransactionId, transaction: SubmittedTransaction)
    : (TransactionForIndex, DisclosureForIndex, GlobalDivulgence) = {

    // First we "commit" the transaction by converting all relative contractIds to absolute ones
    val committedTransaction: GenTransaction.WithTxValue[NodeId, AbsoluteContractId] =
      transaction.resolveRelCid(EventIdFormatter.makeAbs(transactionId))

    // here we just need to align the type for blinding
    val blindingInfo = Blinding.blind(committedTransaction)

    // At this point there should be no local-divulgences
    assert(
      blindingInfo.localDivulgence.isEmpty,
      s"Encountered non-empty local divulgence. This is a bug! [transactionId={$transactionId}, blindingInfo={${blindingInfo.localDivulgence}}"
    )

    // convert LF NodeId to Index EventId
    val disclosureForIndex: Map[EventId, Set[Party]] = blindingInfo.disclosure.map {
      case (nodeId, parties) =>
        EventIdFormatter.fromTransactionId(transactionId, nodeId) -> parties
    }

    val transactionForIndex: TransactionForIndex =
      committedTransaction.mapNodeId(EventIdFormatter.fromTransactionId(transactionId, _))

    (transactionForIndex, disclosureForIndex, blindingInfo.globalDivulgence)
  }
}
