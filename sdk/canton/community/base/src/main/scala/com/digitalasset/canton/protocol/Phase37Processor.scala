// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.HandlerResult
import com.digitalasset.canton.sequencing.protocol.{
  Deliver,
  MediatorGroupRecipient,
  SignedContent,
  WithOpeningErrors,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{RequestCounter, SequencerCounter}

trait Phase37Processor[RequestBatch] {

  /** Processes a request (Phase 3) and sends the response to the mediator if appropriate.
    *
    * @param ts    The timestamp on the request
    * @param rc    The request counter of the request
    * @param sc    The sequencer counter of the request
    * @param batch The batch in the request
    * @return The returned future completes when request has reached the confirmed state
    *         and the response has been sent, or if an error aborts processing.
    */
  def processRequest(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      batch: RequestBatch,
  )(implicit
      traceContext: TraceContext
  ): HandlerResult

  /** Processes a result message, commits the changes or rolls them back and emits events via the
    * [[com.digitalasset.canton.participant.event.RecordOrderPublisher]].
    *
    * @param event The signed result batch to process. The batch must contain exactly one message.
    * @return The [[com.digitalasset.canton.sequencing.HandlerResult]] completes when the request has reached the state
    *         [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Clean]]
    *         and the event has been sent to the [[com.digitalasset.canton.participant.event.RecordOrderPublisher]],
    *         or if the processing aborts with an error.
    */
  def processResult(
      event: WithOpeningErrors[SignedContent[Deliver[DefaultOpenEnvelope]]]
  )(implicit
      traceContext: TraceContext
  ): HandlerResult
}

/** Request messages, along with the root hash message, the mediator ID that received the root hash message,
  * and whether the delivery was a receipt or not (i.e. contained a message ID).
  */
final case class RequestAndRootHashMessage[RequestEnvelope](
    requestEnvelopes: NonEmpty[Seq[RequestEnvelope]],
    rootHashMessage: RootHashMessage[SerializedRootHashMessagePayload],
    mediator: MediatorGroupRecipient,
    isReceipt: Boolean,
)
