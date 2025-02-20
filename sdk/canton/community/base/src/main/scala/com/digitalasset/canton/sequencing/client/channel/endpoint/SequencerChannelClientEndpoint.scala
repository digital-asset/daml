// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel.endpoint

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, OnShutdownRunner}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencing.channel.ConnectToSequencerChannelRequest
import com.digitalasset.canton.sequencing.channel.ConnectToSequencerChannelRequest.Payload
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor
import com.digitalasset.canton.sequencing.client.channel.endpoint.SequencerChannelClientEndpoint.{
  OnSentMessageForTesting,
  versionedResponseTraceContext,
}
import com.digitalasset.canton.sequencing.client.transports.{
  ConsumesCancellableGrpcStreamObserver,
  HasProtoTraceContext,
}
import com.digitalasset.canton.sequencing.protocol.channel.SequencerChannelId
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, MonadUtil, SingleUseCell}
import com.digitalasset.canton.version.{HasToByteString, ProtocolVersion}
import com.google.protobuf.ByteString
import io.grpc.Context.CancellableContext
import io.grpc.stub.StreamObserver

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

/** The sequencer channel client endpoint encapsulates all client-side state needed for the lifetime
  * of a sequencer channel and handles the interaction with:
  *   1. the sequencer channel service to set up the channel by exchanging metadata,
  *   1. the sequencer channel client (connectTo) member endpoint to establish a secure channel, and
  *   1. the SequencerChannelProtocolProcessor provided by the SequencerChannelClient caller
  *
  * To that end, this channel endpoint transitions through several stages: Bootstrapping the channel
  * which establishes unsecured member-to-member communication (the connected stage), followed by
  * setting up a session key to secure it (the securely connected stage). Next, the sequencer
  * channel client user such as the Online Party Replication can start exchanging their messages
  * transparently and securely through the sequencer channel protocol processor.
  *
  * @param channelId
  *   Unique channel identifier known to both channel endpoints.
  * @param member
  *   Sequencer channel client member initiating the channel connection.
  * @param connectTo
  *   The member to interact with via the channel.
  * @param processor
  *   The processor provided by the SequencerChannelClient caller that interacts with the channel
  *   once this channel endpoint has finished setting up the channel.
  * @param isSessionKeyOwner
  *   Whether this endpoint is responsible for generating the session key.
  * @param synchronizerCryptoApi
  *   Provides the crypto API for symmetric and asymmetric encryption operations.
  * @param protocolVersion
  *   Used for the proto messages versioning.
  * @param timestamp
  *   Determines the public key for asymmetric encryption.
  * @param onSentMessage
  *   Message notification for testing purposes only; None for production.
  */
private[channel] final class SequencerChannelClientEndpoint(
    val channelId: SequencerChannelId,
    member: Member,
    connectTo: Member,
    processor: SequencerChannelProtocolProcessor,
    synchronizerCryptoApi: SynchronizerCryptoClient,
    isSessionKeyOwner: Boolean,
    timestamp: CantonTimestamp,
    protocolVersion: ProtocolVersion,
    context: CancellableContext,
    parentOnShutdownRunner: OnShutdownRunner,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    onSentMessage: Option[OnSentMessageForTesting] = None,
)(implicit executionContext: ExecutionContext)
    extends ConsumesCancellableGrpcStreamObserver[
      String,
      v30.ConnectToSequencerChannelResponse,
    ](context, parentOnShutdownRunner, timeouts) {

  private val security: SequencerChannelSecurity =
    new SequencerChannelSecurity(synchronizerCryptoApi, protocolVersion, timestamp)

  /** Keeps track of this endpoint's channel stage. */
  private val stage: AtomicReference[ChannelStage] = {
    val initialStage = new ChannelStageBootstrap(
      isSessionKeyOwner,
      connectTo,
      ChannelStage.InternalData(security, protocolVersion, processor, loggerFactory),
    )
    new AtomicReference[ChannelStage](initialStage)
  }

  private val requestObserver =
    new SingleUseCell[StreamObserver[v30.ConnectToSequencerChannelRequest]]

  private def trySetRequestObserver(
      observer: StreamObserver[v30.ConnectToSequencerChannelRequest]
  )(implicit traceContext: TraceContext): Unit =
    requestObserver
      .putIfAbsent(observer)
      .foreach(observerAlreadySet =>
        ErrorUtil.requireState(
          observerAlreadySet == observer,
          "Request observer already set to a different observer - coding bug",
        )
      )

  /** Set the request observer for the channel that only becomes available after the channel client
    * transport issues the GRPC request that connects to the sequencer channel.
    */
  private[channel] def setRequestObserver(
      observer: StreamObserver[v30.ConnectToSequencerChannelRequest]
  )(implicit traceContext: TraceContext): Unit = {
    val observerRecordingCompletion = new StreamObserver[v30.ConnectToSequencerChannelRequest] {
      override def onNext(value: v30.ConnectToSequencerChannelRequest): Unit =
        observer.onNext(value)

      override def onError(t: Throwable): Unit = {
        // In bidirectional GRPC streaming, a client request observer onError call results in a response observer onError
        // call. (Details: The io.grpc.internal.ClientCallImpl.ClientStreamListenerImpl.closedInternal call made
        // via the observer.onError call below remembers in an asynchronously executed Runnable the fact that
        // the "call" has ended with an error along with the throwable "cause". When the Runnable indirectly
        // invokes io.grpc.stub.ClientCalls.StreamObserverToCallListenerAdapter.onClose, a non-OK status results
        // in the response observer.onError call.)
        // Set the `cancelledByClient` flag so that `complete` does not cancel the context
        // and trigger an error in the request observer.
        cancelledByClient.set(true)
        observer.onError(t)
        // TODO(#22135): Report as error instead of completion once EndpointCloseReason exists.
        complete(SubscriptionCloseReason.Closed)
      }

      override def onCompleted(): Unit = {
        // Like in `onError`, set the `cancelledByClient` flag so that `complete` does not cancel the context
        // and trigger an error in the request observer.
        cancelledByClient.set(true)
        observer.onCompleted()
        complete(SubscriptionCloseReason.Closed)
      }
    }
    trySetRequestObserver(observerRecordingCompletion)
    // As the first message after the request observer is set, let the sequencer know the channel metadata,
    // so that it can connect the two channel endpoints.
    val metadataRequest =
      ConnectToSequencerChannelRequest
        .metadata(channelId, member, connectTo, protocolVersion)
        .toProtoV30
    observerRecordingCompletion.onNext(metadataRequest)
  }

  /** This endpoint's receiving end of the channel – it handles response messages.
    *
    * Forwards responses received via the channel to the processor, once the channel is connected
    * and secured between the member endpoints.
    *
    * The actual message handling depends on this endpoint's channel stage.
    *
    * This method is assumed to be thread-safe, meaning it is invoked once per message and completes
    * fully. State that gets mutated through this method alone is supposed to be safe.
    */
  override protected def callHandler: Traced[v30.ConnectToSequencerChannelResponse] => EitherT[
    FutureUnlessShutdown,
    String,
    Unit,
  ] = _.withTraceContext { implicit traceContext => responseP =>
    val v30.ConnectToSequencerChannelResponse(response, _traceContextO) = responseP
    val currentStage = stage.get()
    for {
      result <- currentStage.handleMessage(response)
      (messages, newStage) = result

      _ <- MonadUtil.sequentialTraverse_(messages) { case (operation, message) =>
        sendMessage(operation, message)
      }

      _ = stage.set(newStage)

      _ <- EitherTUtil.ifThenET(currentStage != newStage)(newStage.initialization())

    } yield ()
  }

  /** This endpoint's sending end of the channel.
    */
  private def sendMessage(operation: String, message: => ConnectToSequencerChannelRequest)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    (requestObserver.get, stage.get) match {
      case (None, _) =>
        val err = s"Attempt to send $operation before request observer set"
        logger.warn(err)
        EitherT.leftT[FutureUnlessShutdown, Unit](err)

      case (Some(_), currentStage: ChannelStageConnected)
          if message.request.isInstanceOf[Payload] =>
        val err =
          s"Attempt to send $operation before channel is ready while in stage $currentStage}"
        logger.warn(err)
        EitherT.leftT[FutureUnlessShutdown, Unit](err)

      case (Some(payloadObserver), _) =>
        logger.debug(s"Sending $operation")
        onSentMessage.foreach(_(message)) // optional, for testing purposes only!
        payloadObserver.onNext(message.toProtoV30)
        logger.debug(s"Sent $operation")
        EitherTUtil.unitUS[String]
    }

  /** Sends payloads to channel.
    *
    * This method is intended to be used by the sequencer channel client user through the processor.
    */
  private[channel] def sendPayload(operation: String, payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val isSecurelyConnected = stage.get() match {
      case _: ChannelStageSecurelyConnected => true
      case _ => false
    }
    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        isSecurelyConnected,
        (), {
          val err =
            s"Attempt to send payload with $operation before members have been securely connected"
          logger.warn(err)
          err
        },
      )
      message = new HasToByteString {
        override def toByteString: ByteString = payload
      }
      encrypted <- security.encrypt(message).leftMap(_.toString)
      _ <- sendMessage(
        operation,
        ConnectToSequencerChannelRequest.payload(
          encrypted.ciphertext,
          protocolVersion,
        ),
      )
    } yield ()
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

  // Channel subscriptions legitimately close when the server closes the channel.
  override protected lazy val onCompleteCloseReason: SubscriptionCloseReason[String] =
    SubscriptionCloseReason.Closed
}

private[channel] object SequencerChannelClientEndpoint {
  implicit val versionedResponseTraceContext
      : HasProtoTraceContext[v30.ConnectToSequencerChannelResponse] =
    (value: v30.ConnectToSequencerChannelResponse) => value.traceContext

  type OnSentMessageForTesting = ConnectToSequencerChannelRequest => Unit
}
