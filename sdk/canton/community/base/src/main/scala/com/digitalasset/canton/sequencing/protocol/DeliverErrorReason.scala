// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.rpc.status.Status

// TODO(#15153): Remove the sealed trait and kids
/** Why was the sequencer unable to sequence the requested send */
sealed trait DeliverErrorReason extends PrettyPrinting {

  /** Printable message explaining error */
  val message: String

  def toProtoV0: v0.DeliverErrorReason =
    v0.DeliverErrorReason(toProtoReason)

  protected def toProtoReason: v0.DeliverErrorReason.Reason
}

object DeliverErrorReason {

  /** The batch was not suitable for the sequencer. Likely a serialization or conversion issue. */
  final case class BatchInvalid(message: String) extends DeliverErrorReason {
    override protected def toProtoReason: v0.DeliverErrorReason.Reason =
      v0.DeliverErrorReason.Reason.BatchInvalid(message)

    override def pretty: Pretty[BatchInvalid] = prettyOfClass(unnamedParam(_.message.unquoted))
  }

  /** The batch could be read but the request was not suitable for the current state.
    * E.g. references recipients that are not registered.
    */
  final case class BatchRefused(message: String) extends DeliverErrorReason {
    override protected def toProtoReason: v0.DeliverErrorReason.Reason =
      v0.DeliverErrorReason.Reason.BatchRefused(message)

    override def pretty: Pretty[BatchRefused] = prettyOfClass(unnamedParam(_.message.unquoted))
  }

  def fromProtoV0(
      deliverErrorReasonP: v0.DeliverErrorReason
  ): ParsingResult[DeliverErrorReason] =
    deliverErrorReasonP.reason match {
      case v0.DeliverErrorReason.Reason.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("DeliverErrorReason.reason"))
      case v0.DeliverErrorReason.Reason.BatchInvalid(message) =>
        Right(DeliverErrorReason.BatchInvalid(message))
      case v0.DeliverErrorReason.Reason.BatchRefused(message) =>
        Right(DeliverErrorReason.BatchRefused(message))
    }

  private def getCauseFromMessage(message: String): Either[Unit, String] = {
    if (message.contains(":")) {
      Right(message.split(": ", 2)(1))
    } else {
      Left(())
    }
  }
  private[protocol] def tryFromStatus(status: Status): DeliverErrorReason = {
    def throwOnError() = {
      throw new IllegalArgumentException(
        s"The message of status $status doesn't follow the expected structure `ERROR_CODE(<category>, <trace-id>): <message>`. DeliverErrorReason only supports ${SequencerErrors.SubmissionRequestMalformed.id} and ${SequencerErrors.SubmissionRequestRefused.id} error codes and statuses generated from them."
      )
    }

    status match {
      case SequencerErrors.SubmissionRequestRefused(message) =>
        DeliverErrorReason.BatchRefused(getCauseFromMessage(message).getOrElse(throwOnError()))
      case SequencerErrors.SubmissionRequestMalformed(message) =>
        DeliverErrorReason.BatchInvalid(getCauseFromMessage(message).getOrElse(throwOnError()))
      case _ =>
        throw new IllegalArgumentException(
          s"DeliverErrorReason only supports ${SequencerErrors.SubmissionRequestMalformed.id} and ${SequencerErrors.SubmissionRequestRefused.id} statuses, could not match against the status = $status"
        )
    }
  }

  private[protocol] def mkStatus(
      deliverErrorReason: v0.DeliverErrorReason.Reason
  ): ParsingResult[Status] = {
    deliverErrorReason match {
      case v0.DeliverErrorReason.Reason.Empty =>
        Left(ProtoDeserializationError.FieldNotSet("DeliverErrorReason.reason"))
      case v0.DeliverErrorReason.Reason.BatchInvalid(message) =>
        Right(SequencerErrors.SubmissionRequestMalformed(message).rpcStatusWithoutLoggingContext())
      case v0.DeliverErrorReason.Reason.BatchRefused(message) =>
        Right(SequencerErrors.SubmissionRequestRefused(message).rpcStatusWithoutLoggingContext())
    }
  }
}
