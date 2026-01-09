// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.networking.grpc.GrpcError.GrpcServiceUnavailable
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason
import com.digitalasset.canton.sequencing.client.channel.endpoint.SequencerChannelClientEndpoint
import com.digitalasset.canton.sequencing.client.transports.GrpcSubscriptionError
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, LoggerUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/** Interface for purpose-specific channel-users to interact with the sequencer channel client. The
  * SequencerChannelProtocolProcessor provides implementations with methods to send and receive
  * messages via a sequencer channel and requires implementations to handle incoming messages.
  */
trait SequencerChannelProtocolProcessor extends FlagCloseable with NamedLogging {
  implicit def executionContext: ExecutionContext

  /** The channel endpoint is set once the sequencer channel client connects to the channel or
    * reconnects to a new channel after a disconnect. Clear upon disconnect.
    */
  private val channelEndpoint = new AtomicReference[Option[SequencerChannelClientEndpoint]](None)

  private[channel] def setChannelEndpoint(
      endpoint: SequencerChannelClientEndpoint
  ): Either[String, Unit] =
    Either
      .cond(
        channelEndpoint
          .compareAndSet(None, Some(endpoint)),
        (),
        "Channel protocol processor already connected to a different channel endpoint, but expect to disconnect first. Coding bug",
      )
      .map(_ => hasCompleted.set(false).discard)

  protected def psid: PhysicalSynchronizerId
  protected def protocolVersion: ProtocolVersion = psid.protocolVersion

  // Whether the processor is currently connected to the channel, set to false after completion or disconnect.
  private[channel] val isConnected = new AtomicBoolean(false)
  private val hasCompleted = new AtomicBoolean(false)

  // These accessors enable tests to observe and interact with all protocol processor implementations.
  def isChannelConnected: Boolean = this.isConnected.get()
  def hasChannelCompleted: Boolean = this.hasCompleted.get()

  /** Notification that the processor is now connected and can begin sending and receiving messages.
    */
  def onConnected()(implicit
      @scala.annotation.unused // unused trace context used by inheritors
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherTUtil
      .condUnitET[FutureUnlessShutdown](!isConnected.getAndSet(true), "Channel already connected")

  /** Handles payload from the channel. */
  def handlePayload(payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]

  /** Notification that the processor has been disconnected, either cleanly by the remote processor
    * completing or with the specified error if status is a left.
    *
    * Returns if the channel endpoint has been removed.
    */
  def onDisconnected(
      @scala.annotation.unused // unused parameters used by inheritors
      status: Either[String, Unit]
  )(implicit
      @scala.annotation.unused
      traceContext: TraceContext
  ): Boolean = {
    isConnected.set(false)
    channelEndpoint.getAndSet(None).nonEmpty
  }

  /** Sends payload to channel
    *
    * Overrideable for testing
    */
  protected def sendPayload(operation: String, payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    synchronizeWithClosing(operation) {
      channelEndpoint.get match {
        case None =>
          val err = s"Attempt to send payload \"$operation\" before channel endpoint set"
          logger.warn(err)
          EitherT.leftT[FutureUnlessShutdown, Unit](err)
        case Some(_) if !isConnected.get() =>
          val err = s"Attempt to send payload \"$operation\" before processor is connected"
          logger.warn(err)
          EitherT.leftT[FutureUnlessShutdown, Unit](err)
        case Some(endpoint) =>
          endpoint.sendPayload(operation, payload)
      }
    }

  /** Sends channel completion thereby ending the ability to send subsequent messages.
    *
    * Overrideable for testing
    */
  protected def sendCompleted(status: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    synchronizeWithClosing(s"complete with $status") {
      channelEndpoint.get.fold {
        val err = s"Attempt to send complete with status \"$status\" before channel endpoint set"
        logger.warn(err)
        EitherT.leftT[FutureUnlessShutdown, Unit](err)
      }(_.sendCompleted(status).map(_ => hasCompleted.set(true)))
    }

  /** Sends channel error thereby ending the ability to send subsequent messages.
    *
    * Overrideable for testing
    */
  protected def sendError(error: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    synchronizeWithClosing(s"send error $error") {
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
      case Success(GrpcSubscriptionError(sequencerUnavailable: GrpcServiceUnavailable)) =>
        // Log GrpcServiceUnavailable as info which happens when a reconnect after a disconnect fails
        // because the sequencer is not ready yet.
        logger.info(s"$channel is being closed with $sequencerUnavailable")
        Left(s"$channel closed with GrpcServiceUnavailable")
      case Success(reason) =>
        logger.warn(s"$channel is being closed with $reason")
        Left(reason.toString)
      case Failure(t) =>
        LoggerUtil.logThrowableAtLevel(Level.WARN, channel, t)
        Left(t.getMessage)
    }
    onDisconnected(errorE).discard
    hasCompleted.set(true)
  }
}
