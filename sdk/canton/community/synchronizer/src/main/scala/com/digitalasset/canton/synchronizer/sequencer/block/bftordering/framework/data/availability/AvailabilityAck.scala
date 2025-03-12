// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose, Signature, v30}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.AvailabilityAck as ProtoAvailabilityAck

final case class AvailabilityAck(from: BftNodeId, signature: Signature)

object AvailabilityAck {
  def fromProto(
      ack: ProtoAvailabilityAck,
      signature: v30.Signature,
  ): ParsingResult[AvailabilityAck] =
    for {
      sig <- Signature.fromProtoV30(signature)
      originatingNode = BftNodeId(ack.from)
    } yield AvailabilityAck(originatingNode, sig)

  def hashFor(batchId: BatchId, expirationTime: CantonTimestamp, from: BftNodeId): Hash = Hash
    .build(
      HashPurpose.BftAvailabilityAck,
      HashAlgorithm.Sha256,
    )
    .add(batchId.getCryptographicEvidence)
    .add(from)
    .add(expirationTime.toMicros)
    .finish()
}
