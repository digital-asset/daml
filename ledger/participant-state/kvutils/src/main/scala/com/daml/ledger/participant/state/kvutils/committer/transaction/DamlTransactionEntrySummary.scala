// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.Conversions.parseTimestamp
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlSubmitterInfo,
  DamlTransactionEntry,
}
import com.daml.lf.crypto
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Time.Timestamp

import scala.jdk.CollectionConverters._

private[kvutils] final class DamlTransactionEntrySummary(
    val submission: DamlTransactionEntry,
    tx: => com.google.protobuf.Any,
) {
  val ledgerEffectiveTime: Timestamp = parseTimestamp(submission.getLedgerEffectiveTime)
  val submitterInfo: DamlSubmitterInfo = submission.getSubmitterInfo
  val commandId: String = submitterInfo.getCommandId
  val submitters: List[Party] =
    submitterInfo.getSubmittersList.asScala.toList.map(Party.assertFromString)
  lazy val transaction: com.google.protobuf.Any = tx
  val submissionTime: Timestamp =
    Conversions.parseTimestamp(submission.getSubmissionTime)
  val submissionSeed: crypto.Hash =
    Conversions.parseHash(submission.getSubmissionSeed)

  // On copy, avoid decoding the transaction again if not needed
  def copyPreservingDecodedTransaction(
      submission: DamlTransactionEntry
  ): DamlTransactionEntrySummary =
    new DamlTransactionEntrySummary(submission, transaction)
}

private[transaction] object DamlTransactionEntrySummary {
  def apply(
      submission: DamlTransactionEntry
  ): DamlTransactionEntrySummary =
    new DamlTransactionEntrySummary(
      submission,
      submission.getTransaction,
    )
}
