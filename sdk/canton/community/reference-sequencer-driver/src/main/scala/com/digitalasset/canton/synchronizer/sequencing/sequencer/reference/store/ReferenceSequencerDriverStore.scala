// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.store

import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.block.BlockFormat.{AcknowledgeTag, SendTag}
import com.google.protobuf.ByteString

private[reference] object ReferenceSequencerDriverStore {

  def sequencedSend(
      payload: ByteString,
      microsecondsSinceEpoch: Long,
      sequencerId: String,
  ): BlockFormat.OrderedRequest =
    BlockFormat.OrderedRequest(
      microsecondsSinceEpoch,
      SendTag,
      payload,
      sequencerId,
    )

  def sequencedAcknowledgement(
      payload: ByteString,
      microsecondsSinceEpoch: Long,
      sequencerId: String,
  ): BlockFormat.OrderedRequest =
    BlockFormat.OrderedRequest(
      microsecondsSinceEpoch,
      AcknowledgeTag,
      payload,
      sequencerId,
    )
}
