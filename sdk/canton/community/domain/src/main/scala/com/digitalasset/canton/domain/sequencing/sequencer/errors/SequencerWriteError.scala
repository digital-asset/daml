// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.errors

import io.grpc.Status

/** Error caused by an attempt to update the sequencer.
  * Some of these errors may be raised immediately when enqueuing the write  ([[WriteRequestRefused]]).
  * If the write is accepted to the Sequencer queue it may later be rejected by a validation of the particular operation
  * and will return an [[OperationError]].
  */
sealed trait SequencerWriteError[+E]

/** The write request was not accepted by the Sequencer and was not enqueued. */
sealed trait WriteRequestRefused extends SequencerWriteError[Nothing] {

  /** These errors are typically returned to the grpc clients as a status error rather than a serialized operation response. */
  def asGrpcStatus: io.grpc.Status
}

object WriteRequestRefused {
  case object SequencerOverloaded extends WriteRequestRefused {
    override def asGrpcStatus: Status =
      Status.RESOURCE_EXHAUSTED.withDescription("Sequencer is overloaded")
  }
}

/** When the write was attempted the request was rejected by the operation itself.
  * Typically due to a validation failing with the sequencer state when attempting to apply the write.
  */
final case class OperationError[E](error: E) extends SequencerWriteError[E]
