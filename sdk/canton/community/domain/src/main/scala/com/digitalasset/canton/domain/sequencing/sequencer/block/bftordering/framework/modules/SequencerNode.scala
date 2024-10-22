// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.sequencer.admin.v30

object SequencerNode {

  sealed trait Message extends Product

  final case object RequestAccepted extends Message

  final case class RequestRejected(reason: String) extends Message

  sealed trait SnapshotMessage extends Message
  object SnapshotMessage {
    final case class AdditionalInfo(info: v30.BftSequencerSnapshotAdditionalInfo)
        extends SnapshotMessage

    final case class AdditionalInfoRetrievalError(
        errorMessage: String
    ) extends SnapshotMessage
  }
}
