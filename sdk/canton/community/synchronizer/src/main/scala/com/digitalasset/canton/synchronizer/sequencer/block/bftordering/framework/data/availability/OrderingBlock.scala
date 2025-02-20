// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability

import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
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

object OrderingBlock {
  val empty: OrderingBlock = OrderingBlock(proofs = Seq.empty)
}
