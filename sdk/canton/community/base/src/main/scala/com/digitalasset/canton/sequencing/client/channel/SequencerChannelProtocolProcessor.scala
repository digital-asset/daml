// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason
import com.digitalasset.canton.sequencing.client.channel.endpoint.SequencerChannelClientEndpoint
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{LoggerUtil, SingleUseCell}
import com.digitalasset.canton.version.ProtocolVersion
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

  /** The channel endpoint is set once the sequencer channel client connects to the channel.
    */
  private val channelEndpoint = new SingleUseCell[SequencerChannelClientEndpoint]

  private[channel] def setChannelEndpoint(
      endpoint: SequencerChannelClientEndpoint
  ): Either[String, Unit] =
    channelEndpoint
      .putIfAbsent(endpoint)
      .fold(Either.unit[String])(endpointAlreadySet =>
        Either.cond(
          endpointAlreadySet == endpoint,
          (),
          "Channel protocol processor previously connected to a different channel endpoint",
        )
      )

  protected def protocolVersion: ProtocolVersion

  // Whether the processor has at some point been connected to the channel, i.e. remains true after hasCompleted is set.
  private[channel] val hasConnected = new AtomicBoolean(false)
  private val hasCompleted = new AtomicBoolean(false)

  // These accessors enable tests to observe and interact with all protocol processor implementations.
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
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    performUnlessClosingEitherUSF(operation) {
      channelEndpoint.get match {
        case None =>
          val err = s"Attempt to send $operation before channel endpoint set"
          logger.warn(err)
          EitherT.leftT[FutureUnlessShutdown, Unit](err)
        case Some(_) if !hasConnected.get() =>
          val err = s"Attempt to send $operation before processor is connected"
          logger.warn(err)
          EitherT.leftT[FutureUnlessShutdown, Unit](err)
        case Some(endpoint) =>
          endpoint.sendPayload(operation, payload)
      }
    }

  /** Sends channel completion thereby ending the ability to send subsequent messages. */
  final protected def sendCompleted(status: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    performUnlessClosingEitherUSF(s"complete with $status") {
      channelEndpoint.get.fold {
        val err = s"Attempt to send complete with $status before channel endpoint set"
        logger.warn(err)
        EitherT.leftT[FutureUnlessShutdown, Unit](err)
      }(_.sendCompleted(status).map(_ => hasCompleted.set(true)))
    }

  /** Sends channel error thereby ending the ability to send subsequent messages. */
  final protected def sendError(error: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    performUnlessClosingEitherUSF(s"send error $error") {
      channelEndpoint.get.fold {
        val errSend = s"Attempt to send error $error before channel endpoint set"
        logger.warn(errSend)
        EitherT.leftT[FutureUnlessShutdown, Unit](errSend)
      }(_.sendError(error).map(_ => hasCompleted.set(true)))
    }

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
