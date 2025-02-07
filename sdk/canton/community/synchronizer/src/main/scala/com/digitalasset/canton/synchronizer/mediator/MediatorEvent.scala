// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, Recipients}

/** The [[MediatorEventsProcessor]] looks through all sequencer events provided by the sequencer client in a batch
  * to pick out events for the Mediator with the same request-id while also scheduling timeouts and running
  * topology transactions at appropriate times. We map all the mediator events we generate into this simplified
  * structure so the [[ConfirmationRequestAndResponseProcessor]] processes these events without having to perform the same extraction
  * and error handling of the original SequencerEvent.
  */
private[mediator] sealed trait MediatorEvent extends PrettyPrinting {
  val requestId: RequestId
  val counter: SequencerCounter
  val sequencingTimestamp: CantonTimestamp
}

private[mediator] object MediatorEvent {
  final case class Request(
      counter: SequencerCounter,
      sequencingTimestamp: CantonTimestamp,
      requestEnvelope: OpenEnvelope[MediatorConfirmationRequest],
      rootHashMessages: List[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      batchAlsoContainsTopologyTransaction: Boolean,
  ) extends MediatorEvent {
    override val requestId: RequestId = RequestId(sequencingTimestamp)

    def request: MediatorConfirmationRequest = requestEnvelope.protocolMessage

    override protected def pretty: Pretty[Request] = prettyOfClass(
      param("sequencing timestamp", _.sequencingTimestamp),
      param("requestEnvelope", _.requestEnvelope),
    )
  }

  /** A response to a mediator confirmation request.
    * Currently each response is processed independently even if they arrive within the same batch.
    */
  final case class Response(
      counter: SequencerCounter,
      sequencingTimestamp: CantonTimestamp,
      response: SignedProtocolMessage[ConfirmationResponse],
      topologyTimestamp: Option[CantonTimestamp],
      recipients: Recipients,
  ) extends MediatorEvent {
    override val requestId: RequestId = response.message.requestId

    override protected def pretty: Pretty[Response] = prettyOfClass(
      param("sequencing timestamp", _.sequencingTimestamp),
      param("response", _.response),
      param("recipient", _.recipients),
    )
  }
}
