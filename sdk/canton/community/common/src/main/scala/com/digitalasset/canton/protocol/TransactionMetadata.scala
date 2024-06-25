// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{ImmArray, Time}
import com.digitalasset.daml.lf.transaction.NodeId
import com.digitalasset.daml.lf.transaction.Transaction.Metadata
import com.digitalasset.canton.data.CantonTimestamp

/** Collects the metadata of a LF transaction to the extent that is needed in Canton
  *
  * @param ledgerTime The ledger time of the transaction
  * @param submissionTime The submission time of the transaction
  * @param seeds The node seeds by node ID
  */
final case class TransactionMetadata(
    ledgerTime: CantonTimestamp,
    submissionTime: CantonTimestamp,
    seeds: Map[LfNodeId, LfHash],
)

object TransactionMetadata {
  def fromLf(ledgerTime: CantonTimestamp, metadata: Metadata): TransactionMetadata =
    TransactionMetadata(
      ledgerTime = ledgerTime,
      submissionTime = CantonTimestamp(metadata.submissionTime),
      seeds = metadata.nodeSeeds.toSeq.toMap,
    )

  def fromTransactionMeta(
      metaLedgerEffectiveTime: Time.Timestamp,
      metaSubmissionTime: Time.Timestamp,
      metaOptNodeSeeds: Option[ImmArray[(NodeId, crypto.Hash)]],
  ): Either[String, TransactionMetadata] = {
    for {
      seeds <- metaOptNodeSeeds.toRight("Node seeds must be specified")
    } yield TransactionMetadata(
      CantonTimestamp(metaLedgerEffectiveTime),
      CantonTimestamp(metaSubmissionTime),
      seeds.toSeq.toMap,
    )
  }

}
