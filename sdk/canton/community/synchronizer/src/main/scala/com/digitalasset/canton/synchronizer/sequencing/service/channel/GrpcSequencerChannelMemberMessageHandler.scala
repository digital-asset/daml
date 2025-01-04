// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service.channel

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.channel.ConnectToSequencerChannelResponse
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, TraceContextGrpc}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.StatusRuntimeException
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import v30.ConnectToSequencerChannelRequest.Request as v30_ChannelRequest
import v30.ConnectToSequencerChannelResponse.Response as v30_ChannelResponse

/** The member message handler represents one side/member of a sequencer channel on the sequencer channel service
  * and forwards messages from the member to another "recipient" member message handler.
  *
  * To prevent log noise arising from GRPC StreamObserver exceptions, the handler tracks the lifetime of its
  * bidirectional GRPC request according to GRPC semantics:
  * - an inbound onError call on the member observer stops the ability to send messages via the response observer
  * - an outbound onComplete call on the member observer (forwarded to the recipient member message handler) limits the time
  *   for the recipient member message handler to complete the opposite direction. (Even when the recipient's channel client
  *   completes the opposite side of the channel immediately in response to an inbound onComplete, the recipient's
  *   onComplete reaction is not forwarded to the sequencer channel service "about half the time".) This in part
  *   motivates the decision to complete the overall sequencer channel the moment either side of the channel calls
  *   onComplete.
  *
  * Lifetime tracking also notifies the GrpcSequencerChannel when the handler has completed its work.
  *
  * @param member                         The member associated with this member message handler.
  * @param responseObserver               The GRPC response observer sending responses to this member message handler's client.
  * @param recipientMemberMessageHandler  The "recipient" member message handler to whom this handler forwards requests.
  * @param onHandlerCompleted             Callback to notify the sequencer channel when this handler has completed.
  */
private[channel] final class GrpcSequencerChannelMemberMessageHandler(
    val member: Member,
    private[channel] val responseObserver: ServerCallStreamObserver[
      v30.ConnectToSequencerChannelResponse
    ],
    recipientMemberMessageHandler: => Option[GrpcSequencerChannelMemberMessageHandler],
    onHandlerCompleted: GrpcSequencerChannelMemberMessageHandler => Unit,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
) extends FlagCloseable
    with CompletesGrpcResponseObserver[v30.ConnectToSequencerChannelResponse]
    with NamedLogging {

  private[channel] val requestObserver =
    new StreamObserver[v30.ConnectToSequencerChannelRequest] {
      override def onNext(requestP: v30.ConnectToSequencerChannelRequest): Unit = {
        val responseE = requestP.request match {
          case v30_ChannelRequest.SessionKey(keyP) =>
            Right(v30_ChannelResponse.SessionKey(keyP): v30_ChannelResponse)
          case v30_ChannelRequest.SessionKeyAcknowledgement(ackP) =>
            Right(v30_ChannelResponse.SessionKeyAcknowledgement(ackP): v30_ChannelResponse)
          case v30_ChannelRequest.Payload(payloadP) =>
            Right(v30_ChannelResponse.Payload(payloadP): v30_ChannelResponse)
          case v30_ChannelRequest.Metadata(metadataP) =>
            Left(s"Unexpectedly asked to forward sequencer channel metadata $metadataP")
          case v30_ChannelRequest.Empty =>
            Left("Unexpectedly asked to forward empty request")
        }
        implicit val traceContext: TraceContext = SerializableTraceContext
          .fromProtoSafeV30Opt(noTracingLogger)(requestP.traceContext)
          .unwrap
        responseE.fold(
          error => {
            val errorMsg = s"Error converting request: $error"
            logger.warn(errorMsg)
            val throwable = new StatusRuntimeException(
              io.grpc.Status.INVALID_ARGUMENT.withDescription(errorMsg)
            )
            // Propagate unexpected sequencer channel message as error to both response observers.
            forwardToRecipient(_.receiveOnError(throwable), errorMsg)
            complete(_.onError(throwable))
          },
          responseP => {
            if (logger.underlying.isDebugEnabled()) {
              logger.debug(msg("Received payload", "payload"))
            }
            forwardToRecipient(
              _.receiveOnNext(
                v30.ConnectToSequencerChannelResponse(responseP, requestP.traceContext)
              ),
              s"payload ${traceContext.traceId}",
              () => {
                val error =
                  s"Sequencer channel error: Sequencer channel service received payload before recipient is ready"
                logger.warn(error)
                complete(
                  _.onError(
                    new StatusRuntimeException(io.grpc.Status.UNAVAILABLE.withDescription(error))
                  )
                )
              },
            )
          },
        )
      }

      override def onError(t: Throwable): Unit = {
        implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
        logger.warn(msg(s"Member message handler received error ${t.getMessage}.", "error"))
        forwardToRecipient(_.receiveOnError(t), s"error ${t.getMessage}")

        // An error on the request observer implies that this handler's response observer is already closed
        // so mark the member message handler as complete without sending a response.
        complete(_ => ())
      }

      override def onCompleted(): Unit = {
        implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
        logger.info(msg("Completed request stream.", "completion"))
        forwardToRecipient(_.receiveOnCompleted(), s"completion")

        // Completing the request stream on a channel request implies that the response stream is also complete.
        // This helps avoid complexity related to having to track two separate directions for closing a channel
        // and also simplifies the interaction of bidirectional GRPC requests.
        logger.info("Completing response stream after request stream completion")
        complete()
      }

      // Helper to produce log message adding suffix only if recipient is available.
      private def msg(message: String, operation: String): String =
        recipientMemberMessageHandler.fold(message)(recipient =>
          message + s" Forwarding $operation to recipient ${recipient.member}."
        )
    }

  /** Generate response notification that both members are connected.
    */
  private[channel] def notifyMembersConnected()(implicit traceContext: TraceContext): Unit = {
    val connectedToMembersResponse =
      ConnectToSequencerChannelResponse.connectedToAllEndpoints(protocolVersion).toProtoV30
    responseObserver.onNext(connectedToMembersResponse)
  }

  /** Forwards a message to the recipient member message handler if the recipient is available.
    *
    * If the recipient is not available, invoke optional onMissingRecipientHandler.
    */
  private def forwardToRecipient(
      callToForward: GrpcSequencerChannelMemberMessageHandler => Unit,
      message: String,
      onMissingRecipientHandler: () => Unit = () => (),
  )(implicit traceContext: TraceContext): Unit = recipientMemberMessageHandler.fold {
    onMissingRecipientHandler()
  } { recipientRequestHandler =>
    performUnlessClosing(s"forward $message to recipient")(
      callToForward(recipientRequestHandler)
    ).onShutdown(())
  }

  // Methods to receive onNext/onCompleted/onError calls from the other member message handler to
  // this member message handler's response observer.
  private[channel] def receiveOnNext(response: v30.ConnectToSequencerChannelResponse): Unit =
    responseObserver.onNext(response)

  private[channel] def receiveOnCompleted(): Unit = {
    logger.info("Completing response stream.")(TraceContext.empty)
    complete()
  }

  private[channel] def receiveOnError(t: Throwable): Unit = {
    logger.warn(s"Request stream error ${t.getMessage} has terminated connection")(
      TraceContext.empty
    )
    complete(_.onError(t))
  }

  override protected def notifyOnComplete(): Unit = onHandlerCompleted(this)

  override def onClosed(): Unit =
    complete()
}
