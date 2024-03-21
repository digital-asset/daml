// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.client.TestSequencerClientSend.Request
import com.digitalasset.canton.sequencing.protocol.{Batch, MessageId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future
import scala.jdk.CollectionConverters.*

/** Test implementation that stores all requests in a queue.
  */
class TestSequencerClientSend extends SequencerClientSend {

  val requestsQueue: java.util.concurrent.BlockingQueue[Request] =
    new java.util.concurrent.LinkedBlockingQueue()

  def requests: Iterable[Request] = requestsQueue.asScala

  override def sendAsync(
      batch: Batch[DefaultOpenEnvelope],
      sendType: SendType,
      timestampOfSigningKey: Option[CantonTimestamp],
      maxSequencingTime: CantonTimestamp,
      messageId: MessageId,
      callback: SendCallback,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] = {
    requestsQueue.add(
      Request(batch, sendType, timestampOfSigningKey, maxSequencingTime, messageId)
    )
    EitherT[Future, SendAsyncClientError, Unit](Future.successful(Right(())))
  }

  override def generateMaxSequencingTime: CantonTimestamp =
    CantonTimestamp.MaxValue
}

object TestSequencerClientSend {
  final case class Request(
      batch: Batch[DefaultOpenEnvelope],
      sendType: SendType,
      timestampOfSigningKey: Option[CantonTimestamp],
      maxSequencingTime: CantonTimestamp,
      messageId: MessageId,
  )(implicit val traceContext: TraceContext)
}
