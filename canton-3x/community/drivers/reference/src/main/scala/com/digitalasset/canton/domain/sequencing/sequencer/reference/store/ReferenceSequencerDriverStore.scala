// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.domain.sequencing.sequencer.reference.store

import com.digitalasset.canton.domain.block.BlockOrderer
import com.digitalasset.canton.domain.block.BlockOrderingSequencer.{
  AcknowledgeTag,
  RegisterMemberTag,
  SendTag,
}
import com.google.protobuf.ByteString

private[reference] object ReferenceSequencerDriverStore {

  def sequencedSend(
      payload: ByteString,
      microsecondsSinceEpoch: Long,
  ): BlockOrderer.OrderedRequest =
    BlockOrderer.OrderedRequest(
      microsecondsSinceEpoch,
      SendTag,
      payload,
    )

  def sequencedRegisterMember(
      payload: ByteString,
      microsecondsSinceEpoch: Long,
  ): BlockOrderer.OrderedRequest =
    BlockOrderer.OrderedRequest(
      microsecondsSinceEpoch,
      RegisterMemberTag,
      payload,
    )

  def sequencedAcknowledgement(
      payload: ByteString,
      microsecondsSinceEpoch: Long,
  ): BlockOrderer.OrderedRequest =
    BlockOrderer.OrderedRequest(
      microsecondsSinceEpoch,
      AcknowledgeTag,
      payload,
    )
}
