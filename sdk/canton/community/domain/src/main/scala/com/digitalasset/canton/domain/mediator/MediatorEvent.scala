// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, Recipients}

/** The [[MediatorEventsProcessor]] looks through all sequencer events provided by the sequencer client in a batch
  * to pick out events for the Mediator with the same request-id while also scheduling timeouts and running
  * topology transactions at appropriate times. We map all the mediator events we generate into this simplified
  * structure so the [[ConfirmationResponseProcessor]] processes these events without having to perform the same extraction
  * and error handling of the original SequencerEvent.
  */
private[mediator] sealed trait MediatorEvent {
  val requestId: RequestId
  val counter: SequencerCounter
  val timestamp: CantonTimestamp
}

private[mediator] object MediatorEvent {
  final case class Request(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      request: MediatorRequest,
      rootHashMessages: List[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      batchAlsoContainsTopologyXTransaction: Boolean,
  ) extends MediatorEvent {
    override val requestId: RequestId = RequestId(timestamp)
  }

  /** A response to a mediator request.
    * Currently each response is processed independently even if they arrive within the same batch.
    */
  final case class Response(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      response: SignedProtocolMessage[MediatorResponse],
      recipients: Recipients,
  ) extends MediatorEvent {
    override val requestId: RequestId = response.message.requestId
  }

  final case class Timeout(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      requestId: RequestId,
  ) extends MediatorEvent
}
