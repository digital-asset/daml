// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability

import cats.syntax.traverse.*
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.ProofOfAvailability as ProtoProofOfAvailability
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

final case class ProofOfAvailability(
    batchId: BatchId,
    acks: Seq[AvailabilityAck],
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
    } yield ProofOfAvailability(id, acks)
}
