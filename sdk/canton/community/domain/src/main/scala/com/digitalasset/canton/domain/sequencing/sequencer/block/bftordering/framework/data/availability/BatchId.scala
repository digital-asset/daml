// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{DeserializationError, HasCryptographicEvidence}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

final case class BatchId private (hash: Hash) extends HasCryptographicEvidence {

  override def getCryptographicEvidence: ByteString = hash.getCryptographicEvidence
}

object BatchId {
  implicit val batchIdOrdering: Ordering[BatchId] =
    Ordering.by(_.hash)

  private val hashPurpose = HashPurpose.BftBatchId

  def fromProto(value: ByteString): ParsingResult[BatchId] =
    Hash.fromProtoPrimitive(value).map(BatchId(_))

  def from(orderingRequestBatch: OrderingRequestBatch): BatchId = {
    val builder = Hash
      .build(hashPurpose, HashAlgorithm.Sha256)
      .add(
        orderingRequestBatch.toProto.toProtoString
      )

    BatchId(builder.finish())
  }

  def fromHexString(hashString: String): Either[DeserializationError, BatchId] =
    Hash.fromHexString(hashString).map(BatchId(_))

  @VisibleForTesting
  def createForTesting(value: String): BatchId =
    BatchId(Hash.digest(hashPurpose, ByteString.copyFromUtf8(value), HashAlgorithm.Sha256))

}
