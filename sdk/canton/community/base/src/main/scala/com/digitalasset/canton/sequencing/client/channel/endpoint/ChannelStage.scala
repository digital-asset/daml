// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel.endpoint

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.{AsymmetricEncrypted, Encrypted, SymmetricKey}
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.domain.api.v30.ConnectToSequencerChannelResponse.Response
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.channel.ConnectToSequencerChannelRequest
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor
import com.digitalasset.canton.sequencing.client.channel.endpoint.ChannelStage.{
  InternalData,
  StageTransition,
}
import com.digitalasset.canton.sequencing.protocol.channel.SequencerChannelSessionKey
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** Represents a sequencer channel client endpoint stage.
  *
  * @param data Parameter object containing configuration and APIs.
  * @param name The stage name.
  */
private[endpoint] sealed abstract class ChannelStage(
    data: InternalData,
    val name: String,
)(implicit
    executionContext: ExecutionContext
) extends NamedLogging {

  override protected def loggerFactory: NamedLoggerFactory = data.loggerFactory

  protected def wrongMessageError(
      response: v30.ConnectToSequencerChannelResponse.Response
  ): EitherT[FutureUnlessShutdown, String, StageTransition] = EitherT.leftT(
    s"In stage $name, cannot process message ${ChannelStage.getName(response)}"
  )

  /** Sequencer channel client endpoint stage dependent response message handling.
    */
  def handleMessage(response: v30.ConnectToSequencerChannelResponse.Response)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition]

  def initialization()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]
}

private[endpoint] object ChannelStage {
  type StageTransition = (List[(String, ConnectToSequencerChannelRequest)], ChannelStage)

  def getName(response: v30.ConnectToSequencerChannelResponse.Response): String =
    response match {
      case Response.Empty => "Empty"
      case _: Response.Connected => "Connected"
      case _: Response.SessionKey => "SessionKey"
      case _: Response.SessionKeyAcknowledgement => "SessionKeyAcknowledgement"
      case _: Response.Payload => "Payload"
    }

  // Data that need to be known by all the stages
  final case class InternalData(
      security: SequencerChannelSecurity,
      protocolVersion: ProtocolVersion,
      processor: SequencerChannelProtocolProcessor,
      loggerFactory: NamedLoggerFactory,
  )
}

private[endpoint] class ChannelStageBootstrap(
    sessionKeyOwner: Boolean,
    connectTo: Member,
    data: InternalData,
)(implicit executionContext: ExecutionContext)
    extends ChannelStage(data, "Bootstrap")(
      executionContext: ExecutionContext
    ) {
  private def newStage = new ChannelStageConnected(data)

  override def initialization()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherT.rightT(())

  override def handleMessage(response: Response)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] = response match {
    case _: Response.Connected => handleConnected()
    case other => wrongMessageError(other)
  }

  private def handleConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] =
    if (sessionKeyOwner) {
      for {
        encryptedKey <- createSessionKey
        m = ConnectToSequencerChannelRequest.sessionKey(
          encryptedKey,
          data.protocolVersion,
        )
      } yield (List(("session key", m)), newStage)
    } else {
      EitherT.rightT[FutureUnlessShutdown, String]((Nil, newStage))
    }

  private def createSessionKey(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, AsymmetricEncrypted[SymmetricKey]] =
    data.security.generateSessionKey(connectTo)
}

private[endpoint] class ChannelStageConnected(data: InternalData)(implicit
    executionContext: ExecutionContext
) extends ChannelStage(data, "Connected")(
      executionContext
    ) {
  private def newStage = new ChannelStageSecurelyConnected(data)

  override def initialization()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherT.rightT(())

  override def handleMessage(response: Response)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] = response match {
    case sessionKey: Response.SessionKey => handleSessionKey(sessionKey)
    case _: Response.SessionKeyAcknowledgement => handleSessionKeyAcknowledgement()
    case other => wrongMessageError(other)
  }

  private def handleSessionKey(message: Response.SessionKey)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] = {
    val encryptedSessionKeyP = message.sessionKey.flatMap(_.encryptedSessionKey)

    for {
      encryptedSymmetricKey <- EitherT.fromEither[FutureUnlessShutdown](
        SequencerChannelSessionKey
          .parseEncryptedSessionKey(encryptedSessionKeyP)
          .leftMap(_.message)
      )
      _ <- data.security.registerSessionKey(encryptedSymmetricKey)
    } yield {

      val messages = List(
        ("session key ack", ConnectToSequencerChannelRequest.sessionKeyAck(data.protocolVersion))
      )

      (messages, newStage)
    }
  }

  private def handleSessionKeyAcknowledgement()
      : EitherT[FutureUnlessShutdown, String, StageTransition] =
    EitherT.rightT((Nil, newStage))
}

private[endpoint] class ChannelStageSecurelyConnected(data: InternalData)(implicit
    executionContext: ExecutionContext
) extends ChannelStage(data, "SecurelyConnected")(
      executionContext
    ) {
  private def newStage = this

  override def initialization()(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    String,
    Unit,
  ] = processOnChannelReadyForProcessor()(traceContext)

  /** Notify the processor that the channel is ready for use, in particular for sending payloads.
    */
  private def processOnChannelReadyForProcessor()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    data.processor.hasConnected.set(true)
    data.processor.onConnected()
  }

  override def handleMessage(response: Response)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] = response match {
    case payload: Response.Payload => handlePayload(payload)
    case other => wrongMessageError(other)
  }

  private def handlePayload(payload: Response.Payload)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StageTransition] = {
    val encryptedPayload = Encrypted.fromByteString(payload.value)
    for {
      decrypted <- decrypt(encryptedPayload)(Right(_))
      _ <- data.processor.handlePayload(decrypted)(traceContext)
    } yield (Nil, newStage)
  }

  private def decrypt[M](encryptedMessage: Encrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  ): EitherT[FutureUnlessShutdown, String, M] =
    data.security.decrypt(encryptedMessage)(deserialize).mapK(FutureUnlessShutdown.outcomeK)

}
