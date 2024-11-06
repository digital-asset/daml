// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability

import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.{
  AvailabilityAck as ProtoAvailabilityAck,
  OrderingBlock as ProtoOrderingBlock,
  ProofOfAvailability as ProtoProofOfAvailability,
}

final case class OrderingBlock(proofs: Seq[ProofOfAvailability]) {
  def toProto: ProtoOrderingBlock =
    ProtoOrderingBlock.of(proofs.map { proof =>
      ProtoProofOfAvailability.of(
        proof.batchId.hash.getCryptographicEvidence,
        proof.acks.map { ack =>
          ProtoAvailabilityAck.of(
            ack.from.uid.toProtoPrimitive,
            Some(ack.signature.toProtoV30),
          )
        },
      )
    })
}
