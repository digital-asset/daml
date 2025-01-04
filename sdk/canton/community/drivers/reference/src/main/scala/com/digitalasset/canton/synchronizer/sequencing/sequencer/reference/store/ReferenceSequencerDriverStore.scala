// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.store

import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.block.BlockFormat.{AcknowledgeTag, SendTag}
import com.google.protobuf.ByteString

private[reference] object ReferenceSequencerDriverStore {

  def sequencedSend(
      payload: ByteString,
      microsecondsSinceEpoch: Long,
  ): BlockFormat.OrderedRequest =
    BlockFormat.OrderedRequest(
      microsecondsSinceEpoch,
      SendTag,
      payload,
    )

  def sequencedAcknowledgement(
      payload: ByteString,
      microsecondsSinceEpoch: Long,
  ): BlockFormat.OrderedRequest =
    BlockFormat.OrderedRequest(
      microsecondsSinceEpoch,
      AcknowledgeTag,
      payload,
    )
}
