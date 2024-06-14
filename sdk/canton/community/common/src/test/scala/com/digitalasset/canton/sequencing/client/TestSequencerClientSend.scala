// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.client.TestSequencerClientSend.Request
import com.digitalasset.canton.sequencing.protocol.{
  AggregationRule,
  Batch,
  MessageId,
  SequencingSubmissionCost,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.jdk.CollectionConverters.*

/** Test implementation that stores all requests in a queue.
  */
class TestSequencerClientSend extends SequencerClientSend {

  val requestsQueue: java.util.concurrent.BlockingQueue[Request] =
    new java.util.concurrent.LinkedBlockingQueue()

  def requests: Iterable[Request] = requestsQueue.asScala

  override def sendAsync(
      batch: Batch[DefaultOpenEnvelope],
      topologyTimestamp: Option[CantonTimestamp],
      maxSequencingTime: CantonTimestamp,
      messageId: MessageId,
      aggregationRule: Option[AggregationRule],
      callback: SendCallback,
      amplify: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientError, Unit] = {
    requestsQueue.add(
      Request(batch, topologyTimestamp, maxSequencingTime, messageId, aggregationRule, None)
    )
    EitherTUtil.unitUS[SendAsyncClientError]
  }

  override def generateMaxSequencingTime: CantonTimestamp =
    CantonTimestamp.MaxValue
}

object TestSequencerClientSend {
  final case class Request(
      batch: Batch[DefaultOpenEnvelope],
      topologyTimestamp: Option[CantonTimestamp],
      maxSequencingTime: CantonTimestamp,
      messageId: MessageId,
      aggregationRule: Option[AggregationRule],
      submissionCost: Option[SequencingSubmissionCost],
  )(implicit val traceContext: TraceContext)
}
