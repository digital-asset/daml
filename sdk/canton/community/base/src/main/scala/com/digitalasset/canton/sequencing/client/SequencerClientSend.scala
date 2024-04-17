// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.protocol.{AggregationRule, Batch, MessageId}
import com.digitalasset.canton.tracing.TraceContext

trait SequencerClientSend {

  /** Sends a request to sequence a deliver event to the sequencer.
    * If we fail to make the request to the sequencer and are certain that it was not received by the sequencer an
    * error is returned. In this circumstance it is safe for the caller to retry the request without causing a duplicate
    * request.
    * A successful response however does not mean that the request will be successfully sequenced. Instead the caller
    * must subscribe to the sequencer and can observe one of the following outcomes:
    *   1. A deliver event is sequenced with a messageId matching this send.
    *   2. A deliver error is sequenced with a messageId matching this send.
    *   3. The sequencing time progresses beyond the provided max-sequencing-time. The caller can assume that the send
    *      will now never be sequenced.
    * Callers should be aware that a message-id can be reused once one of these outcomes is observed so cannot assume
    * that an event with a matching message-id at any point in the future matches their send. Use the `sendTracker` to
    * aid tracking timeouts for events (if useful this could be enriched in the future to provide send completion
    * callbacks alongside the existing timeout notifications).
    * For convenience callers can provide a callback that the SendTracker will invoke when the outcome of the send
    * is known. However this convenience comes with significant limitations that a caller must understand:
    *  - the callback has no ability to be persisted so will be lost after a restart or recreation of the SequencerClient
    *  - the callback is called by the send tracker while handling an event from a SequencerSubscription.
    *    If the callback returns an error this will be returned to the underlying subscription handler and shutdown the sequencer
    *    client. If handlers do not want to halt the sequencer subscription errors should be appropriately handled
    *    (particularly logged) and a successful value returned from the callback.
    *  - If witnessing an event causes many prior sends to timeout there is no guaranteed order in which the
    *    callbacks of these sends will be notified.
    *  - If replay is enabled, the callback will be called immediately with a fake `SendResult`.
    *  For more robust send result tracking callers should persist metadata about the send they will make and
    *  monitor the sequenced events when read, so actions can be taken even if in-memory state is lost.
    */
  def sendAsync(
      batch: Batch[DefaultOpenEnvelope],
      sendType: SendType = SendType.Other,
      topologyTimestamp: Option[CantonTimestamp] = None,
      maxSequencingTime: CantonTimestamp = generateMaxSequencingTime,
      messageId: MessageId = generateMessageId,
      aggregationRule: Option[AggregationRule] = None,
      callback: SendCallback = SendCallback.empty,
      amplify: Boolean = false,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SendAsyncClientError, Unit]

  /** Provides a value for max-sequencing-time to use for `sendAsync` if no better application provided timeout is available.
    * Is currently a configurable offset from our clock.
    */
  def generateMaxSequencingTime: CantonTimestamp

  /** Generates a message id.
    * The message id is only for correlation within this client and does not need to be globally unique.
    */
  def generateMessageId: MessageId = MessageId.randomMessageId()
}
