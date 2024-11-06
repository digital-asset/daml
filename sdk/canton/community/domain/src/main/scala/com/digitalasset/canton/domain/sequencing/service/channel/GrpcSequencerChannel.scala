// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service.channel

import cats.syntax.either.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.domain.sequencing.service.CloseNotification
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.{SequencerChannelId, SequencerChannelMetadata}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import io.grpc.{Status, StatusRuntimeException}

import java.util.concurrent.atomic.AtomicReference

/** The [[GrpcSequencerChannel]] represents a sequencer channel on the sequencer server between two members.
  * On the sequencer channel service side, the two members are handled symmetrically in a bidirectional communication
  * with each member communicating via its [[GrpcSequencerChannelMemberMessageHandler]].
  *
  * To the sequencer server, the only attribute distinguishing the two members is the order in which the members
  * connect to the channel.
  *
  * The first member to connect initiates the creation of a channel, and when the second member connects, the
  * GrpcSequencerChannel links the two member message handlers in such a way that messages, the completion, and a
  * termination error are forwarded from one handler to the other.
  *
  * @param channelId                   Channel id unique to the sequencer
  * @param firstMember                 Initial member that creates and connects to the channel
  * @param secondMember                Subsequent member that connects to the channel
  * @param firstMemberResponseObserver Initial members GRPC response StreamObserver to pass to the handler
  */
private[channel] final class GrpcSequencerChannel(
    val channelId: SequencerChannelId,
    val firstMember: Member,
    val secondMember: Member,
    firstMemberResponseObserver: ServerCallStreamObserver[v30.ConnectToSequencerChannelResponse],
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    baseLoggerFactory: NamedLoggerFactory,
) extends FlagCloseable
    with CloseNotification
    with NamedLogging {
  protected val loggerFactory: NamedLoggerFactory =
    baseLoggerFactory.append("channel", channelId.unwrap)

  private val secondMemberHandler = new SingleUseCell[GrpcSequencerChannelMemberMessageHandler]
  private val closedMemberHandlers = new AtomicReference(
    Set.empty[GrpcSequencerChannelMemberMessageHandler]
  )

  private[channel] val firstMemberHandler = new GrpcSequencerChannelMemberMessageHandler(
    firstMember,
    firstMemberResponseObserver,
    // Let the first member message handler know how to forward messages to the second handler,
    // once the second member connects.
    secondMemberHandler.get,
    onHandlerClosed,
    protocolVersion,
    timeouts,
    loggerFactory.append("member", firstMember.toProtoPrimitive),
  )

  /** When the second member connects, build the second handler and link the two handlers.
    */
  def addSecondMemberToConnectHandler(
      secondMemberResponseObserver: ServerCallStreamObserver[v30.ConnectToSequencerChannelResponse]
  )(implicit
      traceContext: TraceContext
  ): Either[String, GrpcSequencerChannelMemberMessageHandler] = {
    val secondHandler = new GrpcSequencerChannelMemberMessageHandler(
      secondMember,
      secondMemberResponseObserver,
      Some(firstMemberHandler),
      onHandlerClosed,
      protocolVersion,
      timeouts,
      loggerFactory.append("member", secondMember.toProtoPrimitive),
    )
    for {
      _ <- secondMemberHandler
        .putIfAbsent(secondHandler)
        .fold(Either.unit[String])(handler =>
          Either.cond(
            handler == secondHandler,
            (),
            s"Different second member message handler for $secondMember already added",
          )
        )

      // Notify both members that the respective other member has connected to the channel.
      _ = Seq(firstMemberHandler, secondHandler).foreach(_.notifyMembersConnected())
    } yield secondHandler
  }

  private def onHandlerClosed(handler: GrpcSequencerChannelMemberMessageHandler): Unit = {
    val closedHandlers = closedMemberHandlers.updateAndGet(_ + handler)
    // If the second member has not connected yet, only wait for the first member handler to close.
    val expectedCloses = secondMemberHandler.get
      .fold(Set(firstMemberHandler))(secondHandler => Set(firstMemberHandler, secondHandler))
    val awaitingCloses = expectedCloses -- closedHandlers
    logger.info(s"Closed handler ${handler.member} - down to $awaitingCloses")(
      TraceContext.empty
    )
    if (awaitingCloses.isEmpty) {
      // Let the sequencer channel pool know to stop tracking this channel.
      notifyClosed()
    }
  }

  override def onClosed(): Unit = {
    secondMemberHandler.get.foreach(_.close())
    firstMemberHandler.close()
  }
}

/* Helper to look up the existing channel in the pool or create the channel if we are the first member to connect.
 * Returns the member message handler for the channel needed to initialize the GRPC request observer.
 */
private[channel] trait CreatesSequencerChannelMemberMessageHandler {
  protected def ensureChannelAndConnect(
      metadata: SequencerChannelMetadata,
      uninitializedChannel: UninitializedGrpcSequencerChannel,
      tokenExpiresAt: Option[CantonTimestamp],
      createChannel: SequencerChannelMetadata => GrpcSequencerChannel,
      traceContext: TraceContext,
  ): Either[String, GrpcSequencerChannelMemberMessageHandler]
}

/** Represents an "uninitialized" GRPC sequencer channel to bridge the gap until the channel metadata
  * is received in the first message of the request stream. This helps separate initialization-phase specific logic
  * from the initialized GrpcSequencerChannel.
  *
  * @param responseObserver        The response GRPC StreamObserver used to send messages to the channel client
  * @param authTokenExpiresAtO     The expiration time of the authentication token, if "sequencer-authentication-token" available on the GRPC context
  * @param authenticationCheck     Checks that the initiating member matches the "sequencer-authentication-member" on the GRPC context
  */
private[channel] abstract class UninitializedGrpcSequencerChannel(
    private[channel] val responseObserver: ServerCallStreamObserver[
      v30.ConnectToSequencerChannelResponse
    ],
    authTokenExpiresAtO: Option[CantonTimestamp],
    authenticationCheck: Member => Either[String, Unit],
    onUninitializedChannelCompleted: UninitializedGrpcSequencerChannel => Unit,
    protocolVersion: ProtocolVersion,
    timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit traceContext: TraceContext)
    extends CompletesGrpcResponseObserver[v30.ConnectToSequencerChannelResponse]
    with CreatesSequencerChannelMemberMessageHandler
    with NamedLogging
    with AutoCloseable {
  // This adapter bridges the gap between an uninitialized and initialized channel because channel metadata
  // is not available initially and is only sent in the first streamed request message.
  private[service] val requestObserverAdapter
      : StreamObserver[v30.ConnectToSequencerChannelRequest] =
    new StreamObserver[v30.ConnectToSequencerChannelRequest] {
      val initializedRequestStreamObserver =
        new SingleUseCell[StreamObserver[v30.ConnectToSequencerChannelRequest]]

      override def onNext(value: v30.ConnectToSequencerChannelRequest): Unit =
        initializedRequestStreamObserver.get.fold(value match {
          case v30.ConnectToSequencerChannelRequest(payload, traceContextO) =>
            (for {
              traceContext <-
                SerializableTraceContext
                  .fromProtoV30Opt(traceContextO)
                  .bimap(err => (err.message, Status.INVALID_ARGUMENT), _.unwrap)
              metadata <-
                SequencerChannelMetadata
                  .fromByteString(protocolVersion)(payload)
                  .leftMap(err => (err.toString, Status.INVALID_ARGUMENT))
              _ <- authenticationCheck(metadata.initiatingMember).leftMap(
                (_, Status.PERMISSION_DENIED)
              )
              channelMemberHandler <- ensureChannelAndConnect(
                metadata,
                UninitializedGrpcSequencerChannel.this,
                authTokenExpiresAtO,
                createChannel,
                traceContext,
              ).leftMap((_, Status.INVALID_ARGUMENT))
              _ = logger.info(
                s"Successfully initialized channel ${metadata.channelId} from ${metadata.initiatingMember} to ${metadata.receivingMember}"
              )(traceContext)
              _ = initializedRequestStreamObserver
                .putIfAbsent(channelMemberHandler.requestObserver)
                .discard
            } yield ()).leftMap { case (err, status) =>
              propagateRequestStreamErrorToResponseStream(
                s"Sequencer channel initialization error: $err",
                status,
              )(traceContext)
            }.merge
        })(_.onNext(value))

      override def onError(t: Throwable): Unit =
        initializedRequestStreamObserver.get.fold {
          logger.warn(s"Request stream error ${t.getMessage} before initialization.")
          // Flag response stream as complete as we cannot send anything after an error.
          complete(_ => ())
        }(_.onError(t))

      override def onCompleted(): Unit = initializedRequestStreamObserver.get.fold {
        logger.info(
          "Request stream completed before initialization. Completing response stream on assumption channel not needed anymore."
        )
        complete()
      }(_.onCompleted())
    }

  private def propagateRequestStreamErrorToResponseStream(
      error: String,
      errorStatus: Status = io.grpc.Status.INVALID_ARGUMENT,
  )(implicit traceContext: TraceContext): Unit = {
    logger.warn(error)
    complete(_.onError(new StatusRuntimeException(errorStatus.withDescription(error))))
  }

  private def createChannel(metadata: SequencerChannelMetadata): GrpcSequencerChannel = {
    val SequencerChannelMetadata(channelId, initiatingMember, receivingMember) = metadata
    logger.debug(
      s"Received initial channel request $channelId from $initiatingMember to $receivingMember"
    )
    new GrpcSequencerChannel(
      channelId,
      initiatingMember,
      receivingMember,
      responseObserver,
      protocolVersion,
      timeouts,
      loggerFactory,
    )
  }

  override protected def notifyOnComplete(): Unit = onUninitializedChannelCompleted(this)

  override def close(): Unit = complete()
}
