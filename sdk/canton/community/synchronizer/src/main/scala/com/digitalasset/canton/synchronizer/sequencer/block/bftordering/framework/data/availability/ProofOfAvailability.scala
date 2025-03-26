// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability

import cats.syntax.traverse.*
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.ProofOfAvailability as ProtoProofOfAvailability

final case class ProofOfAvailability(
    batchId: BatchId,
    acks: Seq[AvailabilityAck],
    epochNumber: EpochNumber,
)

object ProofOfAvailability {
  def fromProto(
      value: ProtoProofOfAvailability
  ): ParsingResult[ProofOfAvailability] =
    for {
      id <- BatchId.fromProto(value.batchId)
      acks <- value.acks.traverse { ack =>
        ProtoConverter.parseRequired(AvailabilityAck.fromProto(ack, _), "signature", ack.signature)
      }
      epochNumber = EpochNumber(value.epochNumber)
    } yield ProofOfAvailability(id, acks, epochNumber)
}
