// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LoggerUtil
import com.google.protobuf.ByteString
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/** Interface for purpose-specific channel-users to interact with the sequencer channel client.
  * The SequencerChannelProtocolProcessor provides implementations with methods to send and receive
  * messages via a sequencer channel and requires implementations to handle incoming messages.
  */
trait SequencerChannelProtocolProcessor extends FlagCloseable with NamedLogging {
  implicit def executionContext: ExecutionContext

  /** The send adapter is set by the SequencerChannelClientEndpoint only once the channel is ready and safe to use.
    * Delayed initialization of the send adapter allows channel client callers to create the processor and
    * initialize the processor with caller-specific state before calling [[SequencerChannelClient.connectToSequencerChannel]].
    */
  private[channel] val sendAdapter =
    new SequencerChannelProtocolProcessorSendAdapter(timeouts, loggerFactory)

  // Whether the processor has at some point been connected to the channel, i.e. remains true after hasCompleted is set.
  private[channel] val hasConnected = new AtomicBoolean(false)
  private val hasCompleted = new AtomicBoolean(false)

  // These flags enable tests to observe and interact with all protocol processor implementations.
  def hasChannelConnected: Boolean = this.hasConnected.get()
  def hasChannelCompleted: Boolean = this.hasCompleted.get()

  /** Notification that the processor is now connected and can begin sending and receiving messages.
    */
  def onConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]

  /** Handles payload from the channel. */
  def handlePayload(payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]

  /** Notification that the processor has been disconnected, either cleanly by the remote processor completing or
    * with the specified error if status is a left.
    */
  def onDisconnected(status: Either[String, Unit])(implicit traceContext: TraceContext): Unit

  /** Sends payload to channel */
  final protected def sendPayload(operation: String, payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = sendAdapter.sendPayload(operation, payload)

  /** Sends channel completion thereby ending the ability to send subsequent messages. */
  final protected def sendCompleted(status: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    sendAdapter.sendCompleted(status).map(_ => hasCompleted.set(true))

  /** Sends channel error thereby ending the ability to send subsequent messages. */
  final protected def sendError(error: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    sendAdapter.sendError(error).map(_ => hasCompleted.set(true))

  /** Handles end of channel endpoint (successful or error) */
  final private[channel] def handleClose(
      channel: String,
      closeReasonTry: Try[SubscriptionCloseReason[String]],
  )(implicit traceContext: TraceContext): Unit = {
    val errorE = closeReasonTry match {
      case Success(SubscriptionCloseReason.Closed) =>
        logger.debug(s"$channel is being closed")
        Either.unit
      case Success(reason) =>
        logger.warn(s"$channel is being closed with $reason")
        Left(reason.toString)
      case Failure(t) =>
        LoggerUtil.logThrowableAtLevel(Level.WARN, channel, t)
        Left(t.getMessage)
    }
    onDisconnected(errorE)
    hasCompleted.set(true)
  }
}
