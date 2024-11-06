// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelClientEndpoint.versionedResponseTraceContext
import com.digitalasset.canton.sequencing.client.transports.{
  ConsumesCancellableGrpcStreamObserver,
  HasProtoTraceContext,
}
import com.digitalasset.canton.sequencing.protocol.{
  SequencerChannelConnectedToAllEndpoints,
  SequencerChannelId,
  SequencerChannelMetadata,
}
import com.digitalasset.canton.topology.{Member, SequencerId}
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, Traced}
import com.digitalasset.canton.util.{EitherTUtil, SingleUseCell}
import com.google.protobuf.ByteString
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}

/** The sequencer channel client endpoint encapsulates all client-side state needed for the lifetime of a
  * sequencer channel and handles the interaction with:
  * 1. the sequencer channel service to set up the channel by exchanging metadata and
  * 2. the SequencerChannelProtocolProcessor provided by the SequencerChannelClient caller
  *
  * @param sequencerId SequencerId of the sequencer hosting the channel.
  * @param channelId   Unique channel identifier known to both channel endpoints.
  * @param member      Sequencer channel client member initiating the channel connection.
  * @param connectTo   The member to interact with via the channel.
  * @param processor   The processor provided by the SequencerChannelClient caller that interacts with the channel
  *                    once this channel endpoint has finished setting up the channel.
  */
private[channel] final class SequencerChannelClientEndpoint(
    val sequencerId: SequencerId,
    val channelId: SequencerChannelId,
    member: Member,
    connectTo: Member,
    processor: SequencerChannelProtocolProcessor,
    domainParameters: StaticDomainParameters,
    context: CancellableContext,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ConsumesCancellableGrpcStreamObserver[
      String,
      v30.ConnectToSequencerChannelResponse,
    ](context, timeouts) {
  private val areAllEndpointsConnected = new AtomicBoolean(false)
  private val requestObserver =
    new SingleUseCell[StreamObserver[v30.ConnectToSequencerChannelRequest]]

  private def trySetRequestObserver(
      observer: StreamObserver[v30.ConnectToSequencerChannelRequest]
  ): Unit =
    requestObserver
      .putIfAbsent(observer)
      .foreach(observerAlreadySet =>
        if (observerAlreadySet != observer) {
          throw new IllegalStateException(
            "Request observer already set to a different observer - coding bug"
          )
        }
      )

  /** Set the request observer for the channel that only becomes available after the channel client transport
    * issues the GRPC request that connects to the sequencer channel.
    */
  private[channel] def setRequestObserver(
      observer: StreamObserver[v30.ConnectToSequencerChannelRequest]
  )(implicit traceContext: TraceContext): Unit = {
    val observerRecordingCompletion = new StreamObserver[v30.ConnectToSequencerChannelRequest] {
      override def onNext(value: v30.ConnectToSequencerChannelRequest): Unit =
        observer.onNext(value)

      override def onError(t: Throwable): Unit = {
        observer.onError(t)
        // TODO(#22135): Report as error instead of completion once EndpointCloseReason exists.
        complete(SubscriptionCloseReason.Closed)
      }

      override def onCompleted(): Unit = {
        observer.onCompleted()
        complete(SubscriptionCloseReason.Closed)
      }
    }
    trySetRequestObserver(observerRecordingCompletion)
    // As the first message after the request observer is set, let the sequencer know the channel metadata,
    // so that it can connect the two channel endpoints.
    val metadataPayload = SequencerChannelMetadata(channelId, member, connectTo)(
      SequencerChannelMetadata.protocolVersionRepresentativeFor(domainParameters.protocolVersion)
    ).toByteString
    val metadataRequest = v30.ConnectToSequencerChannelRequest(
      metadataPayload,
      Some(SerializableTraceContext(traceContext).toProtoV30),
    )
    observerRecordingCompletion.onNext(metadataRequest)
  }

  /** Forwards responses received via the channel to the processor, once the channel is connected to all endpoints.
    */
  override protected def callHandler: Traced[v30.ConnectToSequencerChannelResponse] => Future[
    Either[String, Unit]
  ] = { case Traced(v30.ConnectToSequencerChannelResponse(payload, traceContextO)) =>
    (for {
      traceContext <- EitherT.fromEither[FutureUnlessShutdown](
        SerializableTraceContext.fromProtoV30Opt(traceContextO).leftMap(_.message)
      )
      _ <-
        if (areAllEndpointsConnected.getAndSet(true)) {
          processor.handlePayload(payload)(traceContext.unwrap)
        } else {
          processOnChannelReadyForProcessor(payload)(traceContext.unwrap)
        }
    } yield ()).value.onShutdown(Right(()))
  }

  /** Sends payloads to channel */
  private[channel] def sendPayload(operation: String, payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    requestObserver.get match {
      case None =>
        val err = s"Attempt to send $operation before request observer set"
        logger.warn(err)
        EitherT.leftT[FutureUnlessShutdown, Unit](err)
      case Some(_) if !areAllEndpointsConnected.get() =>
        val err = s"Attempt to send $operation before channel is ready to send payloads"
        logger.warn(err)
        EitherT.leftT[FutureUnlessShutdown, Unit](err)
      case Some(payloadObserver) =>
        logger.debug(s"Sending $operation")
        val request = v30.ConnectToSequencerChannelRequest(
          payload,
          Some(SerializableTraceContext(traceContext).toProtoV30),
        )
        payloadObserver.onNext(request)
        logger.debug(s"Sent $operation")
        EitherTUtil.unitUS[String]
    }

  /** Sends channel completion */
  private[channel] def sendCompleted(status: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    requestObserver.get.fold {
      val err = s"Attempt to send complete with $status before request observer set"
      logger.warn(err)
      EitherT.leftT[FutureUnlessShutdown, Unit](err)
    } { completionObserver =>
      logger.info(s"Sending channel completion with $status")
      completionObserver.onCompleted()
      logger.info(s"Sent channel completion with $status")
      EitherTUtil.unitUS
    }

  /** Sends channel error */
  private[channel] def sendError(error: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    requestObserver.get.fold {
      val errSend = s"Attempt to send error $error before request observer set"
      logger.warn(errSend)
      EitherT.leftT[FutureUnlessShutdown, Unit](errSend)
    } { errorObserver =>
      logger.warn(s"Sending channel error $error")
      errorObserver.onError(new IllegalStateException(error))
      logger.info(s"Sent channel error $error")
      EitherTUtil.unitUS
    }

  /** Initializes the processor once the channel is ready for use.
    *
    * Initialization consists of:
    * - verifying that the sequencer channel service has connected the channel to all endpoints,
    * - notifying the processor/"client" code that the processor may begin sending channel payload messages, and
    * - setting the "hasConnected" flag to forward payload messages to the sequencer.
    *
    * @param channelConnectedToAllEndpointsPayload The payload that signals the channel is connected to both members.
    */
  private def processOnChannelReadyForProcessor(
      channelConnectedToAllEndpointsPayload: ByteString
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    // Check if payload is SequencerChannelConnectedToMembers as expected
    for {
      _ <-
        EitherT.fromEither[FutureUnlessShutdown](
          SequencerChannelConnectedToAllEndpoints
            .fromByteString(domainParameters.protocolVersion)(channelConnectedToAllEndpointsPayload)
            .leftMap(_.toString)
        )
      // Allow the processor to send payloads to the channel.
      _ = processor.hasConnected.set(true)
      _ <- processor.onConnected()
    } yield ()

  // Channel subscriptions legitimately close when the server closes the channel.
  override protected lazy val onCompleteCloseReason: SubscriptionCloseReason[String] =
    SubscriptionCloseReason.Closed
}

object SequencerChannelClientEndpoint {
  implicit val versionedResponseTraceContext
      : HasProtoTraceContext[v30.ConnectToSequencerChannelResponse] =
    (value: v30.ConnectToSequencerChannelResponse) => value.traceContext
}
