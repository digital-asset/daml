// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.error.ErrorCategory
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member

/** Synchronous error returned by a sequencer. */
sealed trait SendAsyncError extends PrettyPrinting {

  val message: String

  protected def toResponseReasonProto: v0.SendAsyncResponse.Error.Reason
  protected def toSignedResponseReasonProto: v0.SendAsyncSignedResponse.Error.Reason

  private[protocol] def toSendAsyncResponseProto: v0.SendAsyncResponse.Error =
    v0.SendAsyncResponse.Error(toResponseReasonProto)

  private[protocol] def toSendAsyncSignedResponseProto: v0.SendAsyncSignedResponse.Error =
    v0.SendAsyncSignedResponse.Error(toSignedResponseReasonProto)

  override def pretty: Pretty[SendAsyncError] = prettyOfClass(unnamedParam(_.message.unquoted))

  def category: ErrorCategory

}

object SendAsyncError {

  /** The request could not be deserialized to be processed */
  final case class RequestInvalid(message: String) extends SendAsyncError {
    protected def toResponseReasonProto: v0.SendAsyncResponse.Error.Reason =
      v0.SendAsyncResponse.Error.Reason.RequestInvalid(message)
    protected def toSignedResponseReasonProto: v0.SendAsyncSignedResponse.Error.Reason =
      v0.SendAsyncSignedResponse.Error.Reason.RequestInvalid(message)
    override def category: ErrorCategory = ErrorCategory.InvalidIndependentOfSystemState
  }

  /** The request server could read the request but refused to accept it */
  final case class RequestRefused(message: String) extends SendAsyncError {
    protected def toResponseReasonProto: v0.SendAsyncResponse.Error.Reason =
      v0.SendAsyncResponse.Error.Reason.RequestRefused(message)
    protected def toSignedResponseReasonProto: v0.SendAsyncSignedResponse.Error.Reason =
      v0.SendAsyncSignedResponse.Error.Reason.RequestRefused(message)
    override def category: ErrorCategory = ErrorCategory.InvalidGivenCurrentSystemStateOther
  }

  /** The Sequencer is overloaded and declined to handle the request */
  final case class Overloaded(message: String) extends SendAsyncError {
    protected def toResponseReasonProto: v0.SendAsyncResponse.Error.Reason =
      v0.SendAsyncResponse.Error.Reason.Overloaded(message)
    protected def toSignedResponseReasonProto: v0.SendAsyncSignedResponse.Error.Reason =
      v0.SendAsyncSignedResponse.Error.Reason.Overloaded(message)
    override def category: ErrorCategory = ErrorCategory.ContentionOnSharedResources
  }

  /** The sequencer is unable to process requests (if the service is running it could mean the sequencer is going through a crash recovery process) */
  final case class Unavailable(message: String) extends SendAsyncError {
    protected def toResponseReasonProto: v0.SendAsyncResponse.Error.Reason =
      v0.SendAsyncResponse.Error.Reason.Unavailable(message)
    protected def toSignedResponseReasonProto: v0.SendAsyncSignedResponse.Error.Reason =
      v0.SendAsyncSignedResponse.Error.Reason.Unavailable(message)
    override def category: ErrorCategory = ErrorCategory.TransientServerFailure
  }

  /** The Sequencer was unable to handle the send as the sender was unknown so could not asynchronously deliver them a deliver event or error */
  final case class SenderUnknown(message: String) extends SendAsyncError {
    protected def toResponseReasonProto: v0.SendAsyncResponse.Error.Reason =
      v0.SendAsyncResponse.Error.Reason.SenderUnknown(message)
    protected def toSignedResponseReasonProto: v0.SendAsyncSignedResponse.Error.Reason =
      v0.SendAsyncSignedResponse.Error.Reason.SenderUnknown(message)
    override def category: ErrorCategory = ErrorCategory.InvalidGivenCurrentSystemStateOther
  }

  final case class UnknownRecipients(message: String) extends SendAsyncError {
    protected def toResponseReasonProto: v0.SendAsyncResponse.Error.Reason =
      v0.SendAsyncResponse.Error.Reason.UnknownRecipients(message)
    protected def toSignedResponseReasonProto: v0.SendAsyncSignedResponse.Error.Reason =
      v0.SendAsyncSignedResponse.Error.Reason.UnknownRecipients(message)
    override def category: ErrorCategory = ErrorCategory.InvalidGivenCurrentSystemStateOther
  }

  object UnknownRecipients {
    def apply(unknownMembers: List[Member]): UnknownRecipients = UnknownRecipients(
      s"The following recipients are invalid: ${unknownMembers.mkString(",")}"
    )
  }

  /** The sequencer declined to process new requests as it is shutting down */
  final case class ShuttingDown(message: String = "Sequencer shutting down")
      extends SendAsyncError {
    protected def toResponseReasonProto: v0.SendAsyncResponse.Error.Reason =
      v0.SendAsyncResponse.Error.Reason.ShuttingDown(message)
    protected def toSignedResponseReasonProto: v0.SendAsyncSignedResponse.Error.Reason =
      v0.SendAsyncSignedResponse.Error.Reason.ShuttingDown(message)
    override def category: ErrorCategory = ErrorCategory.TransientServerFailure
  }

  final case class Internal(message: String) extends SendAsyncError {
    protected def toResponseReasonProto: v0.SendAsyncResponse.Error.Reason =
      throw new IllegalStateException(
        "Message `Internal` introduced with protocol version 4 should not be included in `v0.SendAsyncResponse`"
      )

    protected def toSignedResponseReasonProto: v0.SendAsyncSignedResponse.Error.Reason =
      v0.SendAsyncSignedResponse.Error.Reason.Internal(message)

    override def category: ErrorCategory = ErrorCategory.TransientServerFailure
  }

  final case class Generic(message: String) extends SendAsyncError {
    protected def toResponseReasonProto: v0.SendAsyncResponse.Error.Reason =
      throw new IllegalStateException(
        "Message `Generic` introduced with protocol version 4 should not be included in `v0.SendAsyncResponse`"
      )
    protected def toSignedResponseReasonProto: v0.SendAsyncSignedResponse.Error.Reason =
      v0.SendAsyncSignedResponse.Error.Reason.Generic(message)

    override def category: ErrorCategory = ErrorCategory.TransientServerFailure
  }

  private[protocol] def fromErrorProto(
      error: v0.SendAsyncResponse.Error
  ): ParsingResult[SendAsyncError] =
    error.reason match {
      case v0.SendAsyncResponse.Error.Reason.Empty =>
        ProtoDeserializationError.FieldNotSet("SendAsyncResponse.error.reason").asLeft
      case v0.SendAsyncResponse.Error.Reason.RequestInvalid(message) =>
        RequestInvalid(message).asRight
      case v0.SendAsyncResponse.Error.Reason.RequestRefused(message) =>
        RequestRefused(message).asRight
      case v0.SendAsyncResponse.Error.Reason.Overloaded(message) => Overloaded(message).asRight
      case v0.SendAsyncResponse.Error.Reason.Unavailable(message) => Unavailable(message).asRight
      case v0.SendAsyncResponse.Error.Reason.SenderUnknown(message) =>
        SenderUnknown(message).asRight
      case v0.SendAsyncResponse.Error.Reason.UnknownRecipients(message) =>
        UnknownRecipients(message).asRight
      case v0.SendAsyncResponse.Error.Reason.ShuttingDown(message) => ShuttingDown(message).asRight
    }

  private[protocol] def fromSignedErrorProto(
      error: v0.SendAsyncSignedResponse.Error
  ): ParsingResult[SendAsyncError] =
    error.reason match {
      case v0.SendAsyncSignedResponse.Error.Reason.Empty =>
        ProtoDeserializationError.FieldNotSet("SendAsyncResponse.error.reason").asLeft
      case v0.SendAsyncSignedResponse.Error.Reason.RequestInvalid(message) =>
        RequestInvalid(message).asRight
      case v0.SendAsyncSignedResponse.Error.Reason.RequestRefused(message) =>
        RequestRefused(message).asRight
      case v0.SendAsyncSignedResponse.Error.Reason.Overloaded(message) =>
        Overloaded(message).asRight
      case v0.SendAsyncSignedResponse.Error.Reason.Unavailable(message) =>
        Unavailable(message).asRight
      case v0.SendAsyncSignedResponse.Error.Reason.SenderUnknown(message) =>
        SenderUnknown(message).asRight
      case v0.SendAsyncSignedResponse.Error.Reason.UnknownRecipients(message) =>
        UnknownRecipients(message).asRight
      case v0.SendAsyncSignedResponse.Error.Reason.ShuttingDown(message) =>
        ShuttingDown(message).asRight
      case v0.SendAsyncSignedResponse.Error.Reason.Internal(message) => Internal(message).asRight
      case v0.SendAsyncSignedResponse.Error.Reason.Generic(message) => Generic(message).asRight
    }
}

final case class SendAsyncResponse(error: Option[SendAsyncError]) {
  def toSendAsyncResponseProto: v0.SendAsyncResponse =
    v0.SendAsyncResponse(error.map(_.toSendAsyncResponseProto))

  def toSendAsyncSignedResponseProto: v0.SendAsyncSignedResponse =
    v0.SendAsyncSignedResponse(error.map(_.toSendAsyncSignedResponseProto))
}

object SendAsyncResponse {
  def fromSendAsyncResponseProto(
      responseP: v0.SendAsyncResponse
  ): ParsingResult[SendAsyncResponse] =
    for {
      error <- responseP.error.traverse(SendAsyncError.fromErrorProto)
    } yield SendAsyncResponse(error)

  def fromSendAsyncSignedResponseProto(
      responseP: v0.SendAsyncSignedResponse
  ): ParsingResult[SendAsyncResponse] =
    for {
      error <- responseP.error.traverse(SendAsyncError.fromSignedErrorProto)
    } yield SendAsyncResponse(error)
}
