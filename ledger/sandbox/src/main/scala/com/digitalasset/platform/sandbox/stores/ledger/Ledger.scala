// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import java.time.Instant

import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation.Relation
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.{GenTransaction, TransactionCommitter}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.EventId
import com.daml.platform.events.EventIdFormatter
import com.daml.platform.store.ReadOnlyLedger

import scala.concurrent.Future

trait Ledger extends ReadOnlyLedger {

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

  def convertToCommittedTransaction(
      committer: TransactionCommitter,
      transactionId: TransactionId,
      transaction: SubmittedTransaction
  ): (TransactionForIndex, DisclosureForIndex, GlobalDivulgence) = {

    // First we "commit" the transaction by converting all relative contractIds to absolute ones
    val committedTransaction = committer.commitTransaction(transactionId, transaction)

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
