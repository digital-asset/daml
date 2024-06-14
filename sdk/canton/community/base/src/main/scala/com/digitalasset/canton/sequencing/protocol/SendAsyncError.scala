// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.error.ErrorCategory
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.domain.api.v30.TrafficControlErrorReason
import com.digitalasset.canton.domain.api.v30.TrafficControlErrorReason.Error.Reason
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member

/** Synchronous error returned by a sequencer. */
sealed trait SendAsyncError extends PrettyPrinting {

  val message: String

  protected def toProtoV30: v30.SendAsyncVersionedResponse.Error.Reason

  private[protocol] def toResponseProtoV30: v30.SendAsyncVersionedResponse.Error =
    v30.SendAsyncVersionedResponse.Error(toProtoV30)

  override def pretty: Pretty[SendAsyncError] = prettyOfClass(unnamedParam(_.message.unquoted))

  def category: ErrorCategory

}

object SendAsyncError {

  /** The request could not be deserialized to be processed */
  final case class RequestInvalid(message: String) extends SendAsyncError {
    protected def toProtoV30: v30.SendAsyncVersionedResponse.Error.Reason =
      v30.SendAsyncVersionedResponse.Error.Reason.RequestInvalid(message)
    override def category: ErrorCategory = ErrorCategory.InvalidIndependentOfSystemState
  }

  /** The request server could read the request but refused to accept it */
  final case class RequestRefused(message: String) extends SendAsyncError {
    protected def toProtoV30: v30.SendAsyncVersionedResponse.Error.Reason =
      v30.SendAsyncVersionedResponse.Error.Reason.RequestRefused(message)
    override def category: ErrorCategory = ErrorCategory.InvalidGivenCurrentSystemStateOther
  }

  /** The Sequencer is overloaded and declined to handle the request */
  final case class Overloaded(message: String) extends SendAsyncError {
    protected def toProtoV30: v30.SendAsyncVersionedResponse.Error.Reason =
      v30.SendAsyncVersionedResponse.Error.Reason.Overloaded(message)
    override def category: ErrorCategory = ErrorCategory.ContentionOnSharedResources
  }

  /** The sequencer is unable to process requests (if the service is running it could mean the sequencer is going through a crash recovery process) */
  final case class Unavailable(message: String) extends SendAsyncError {
    protected def toProtoV30: v30.SendAsyncVersionedResponse.Error.Reason =
      v30.SendAsyncVersionedResponse.Error.Reason.Unavailable(message)
    override def category: ErrorCategory = ErrorCategory.TransientServerFailure
  }

  /** The Sequencer was unable to handle the send as the sender was unknown so could not asynchronously deliver them a deliver event or error */
  final case class SenderUnknown(message: String) extends SendAsyncError {
    protected def toProtoV30: v30.SendAsyncVersionedResponse.Error.Reason =
      v30.SendAsyncVersionedResponse.Error.Reason.SenderUnknown(message)
    override def category: ErrorCategory = ErrorCategory.InvalidGivenCurrentSystemStateOther
  }

  final case class UnknownRecipients(message: String) extends SendAsyncError {
    protected def toProtoV30: v30.SendAsyncVersionedResponse.Error.Reason =
      v30.SendAsyncVersionedResponse.Error.Reason.UnknownRecipients(message)
    override def category: ErrorCategory = ErrorCategory.InvalidGivenCurrentSystemStateOther
  }

  final case class TrafficControlError(error: TrafficControlErrorReason.Error)
      extends SendAsyncError {
    override val message = error.reason match {
      case Reason.Empty => "unknown"
      case Reason.InsufficientTraffic(reason) => s"Insufficient traffic: $reason"
      case Reason.OutdatedTrafficCost(reason) => s"Outdated traffic cost: $reason"
    }
    protected def toProtoV30: v30.SendAsyncVersionedResponse.Error.Reason =
      v30.SendAsyncVersionedResponse.Error.Reason.TrafficControl(
        TrafficControlErrorReason(Some(error))
      )
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
    protected def toProtoV30: v30.SendAsyncVersionedResponse.Error.Reason =
      v30.SendAsyncVersionedResponse.Error.Reason.ShuttingDown(message)
    override def category: ErrorCategory = ErrorCategory.TransientServerFailure
  }

  final case class Internal(message: String) extends SendAsyncError {

    protected def toProtoV30: v30.SendAsyncVersionedResponse.Error.Reason =
      v30.SendAsyncVersionedResponse.Error.Reason.Internal(message)

    override def category: ErrorCategory = ErrorCategory.TransientServerFailure
  }

  final case class Generic(message: String) extends SendAsyncError {
    protected def toProtoV30: v30.SendAsyncVersionedResponse.Error.Reason =
      v30.SendAsyncVersionedResponse.Error.Reason.Generic(message)

    override def category: ErrorCategory = ErrorCategory.TransientServerFailure
  }

  private[protocol] def fromSignedErrorProto(
      error: v30.SendAsyncVersionedResponse.Error
  ): ParsingResult[SendAsyncError] =
    error.reason match {
      case v30.SendAsyncVersionedResponse.Error.Reason.Empty =>
        ProtoDeserializationError
          .FieldNotSet("SendAsyncVersionedResponse.error.reason")
          .asLeft
      case v30.SendAsyncVersionedResponse.Error.Reason.RequestInvalid(message) =>
        RequestInvalid(message).asRight
      case v30.SendAsyncVersionedResponse.Error.Reason.RequestRefused(message) =>
        RequestRefused(message).asRight
      case v30.SendAsyncVersionedResponse.Error.Reason.Overloaded(message) =>
        Overloaded(message).asRight
      case v30.SendAsyncVersionedResponse.Error.Reason.Unavailable(message) =>
        Unavailable(message).asRight
      case v30.SendAsyncVersionedResponse.Error.Reason.SenderUnknown(message) =>
        SenderUnknown(message).asRight
      case v30.SendAsyncVersionedResponse.Error.Reason.UnknownRecipients(message) =>
        UnknownRecipients(message).asRight
      case v30.SendAsyncVersionedResponse.Error.Reason.ShuttingDown(message) =>
        ShuttingDown(message).asRight
      case v30.SendAsyncVersionedResponse.Error.Reason.Internal(message) =>
        Internal(message).asRight
      case v30.SendAsyncVersionedResponse.Error.Reason.Generic(message) => Generic(message).asRight
      case v30.SendAsyncVersionedResponse.Error.Reason
            .TrafficControl(TrafficControlErrorReason(errorP)) =>
        for {
          error <- ProtoConverter.required("error", errorP)
        } yield TrafficControlError(error)
    }
}

final case class SendAsyncVersionedResponse(error: Option[SendAsyncError]) {

  def toProtoV30: v30.SendAsyncVersionedResponse =
    v30.SendAsyncVersionedResponse(error.map(_.toResponseProtoV30))
}

object SendAsyncVersionedResponse {
  def fromProtoV30(
      responseP: v30.SendAsyncVersionedResponse
  ): ParsingResult[SendAsyncVersionedResponse] =
    for {
      error <- responseP.error.traverse(SendAsyncError.fromSignedErrorProto)
    } yield SendAsyncVersionedResponse(error)
}
