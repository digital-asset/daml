// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose, Signature, v30}
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.AvailabilityAck as ProtoAvailabilityAck
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{SequencerId, UniqueIdentifier}

final case class AvailabilityAck(from: SequencerId, signature: Signature)

object AvailabilityAck {
  def fromProto(
      ack: ProtoAvailabilityAck,
      signature: v30.Signature,
  ): ParsingResult[AvailabilityAck] =
    for {
      sig <- Signature.fromProtoV30(signature)
      originatingSequencerId <- UniqueIdentifier
        .fromProtoPrimitive(ack.fromSequencerUid, "from_sequencer_uid")
        .map(SequencerId(_))
    } yield AvailabilityAck(originatingSequencerId, sig)

  def hashFor(batchId: BatchId, from: SequencerId): Hash = Hash
    .build(
      HashPurpose.BftAvailabilityAck,
      HashAlgorithm.Sha256,
    )
    .add(batchId.getCryptographicEvidence)
    .add(from.toString.length)
    .add(from.toString)
    .finish()
}
