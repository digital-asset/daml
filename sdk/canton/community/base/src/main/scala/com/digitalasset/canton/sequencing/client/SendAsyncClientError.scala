// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.BaseCantonError
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.protocol.SendAsyncError

/** Errors returned from the AsyncSend where we are sure the request has not potentially been accepted by the server
  * so may be retried using a new message id (as a tracked message id for the failed request may remain in the pending
  * send set).
  * If a technical error is encountered by the sequencer client where there is a chance that the send will be sequenced
  * it should not be returned to the caller through this error.
  */
sealed trait SendAsyncClientError extends Product with Serializable with PrettyPrinting

object SendAsyncClientError extends SequencerErrorGroup {

  @Explanation("This error indicates that a message could not be sent through the sequencer.")
  @Resolution("Inspect the error details")
  object ErrorCode
      extends ErrorCode(
        id = "SEQUENCER_SEND_ASYNC_CLIENT_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Wrap(reason: SendAsyncClientError)
        extends BaseCantonError.Impl(cause = "Unable to send through the sequencer")
  }

  /** The [[SequencerClient]] decided that the request is invalid so did not attempt to send it to the sequencer */
  final case class RequestInvalid(message: String) extends SendAsyncClientError {
    override def pretty: Pretty[RequestInvalid] = prettyOfClass(unnamedParam(_.message.unquoted))
  }

  /** A send with the supplied message id is already being tracked */
  case object DuplicateMessageId extends SendAsyncClientError {
    override def pretty: Pretty[DuplicateMessageId.type] = prettyOfObject[DuplicateMessageId.type]
  }

  /** Errors that the client can get back from the sequencer synchronously */
  sealed trait SendAsyncClientResponseError extends SendAsyncClientError

  /** We were unable to make the request for a technical reason */
  final case class RequestFailed(message: String) extends SendAsyncClientResponseError {
    override def pretty: Pretty[RequestFailed] = prettyOfClass(unnamedParam(_.message.unquoted))
  }

  /** We were able to contact the server but the request was declined */
  final case class RequestRefused(error: SendAsyncError) extends SendAsyncClientResponseError {
    override def pretty: Pretty[RequestRefused] = prettyOfClass(unnamedParam(_.error))
  }
}
