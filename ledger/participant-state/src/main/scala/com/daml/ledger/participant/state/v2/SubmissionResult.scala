// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import io.grpc.StatusRuntimeException

sealed abstract class SubmissionResult extends Product with Serializable {
  def description: String
}

object SubmissionResult {

  /** The request has been received */
  case object Acknowledged extends SubmissionResult {
    override val description: String = "The request has been received"
  }

  /** The submission has failed with a synchronous error.
    *
    * Asynchronous errors are reported via the command completion stream as a [[Update.CommandRejected]]
    *
    * See the documentation in `error.proto` for how to report common submission errors.
    */
  case class SynchronousError(grpcError: com.google.rpc.status.Status) extends SubmissionResult {
    override val description: String = s"Submission failed with error ${grpcError.message}"
  }

  /** Temporary method to tunnel new error codes through the ledger-api server */
  final case class SynchronousReject(failure: StatusRuntimeException) extends SubmissionResult {
    override def description: String = failure.getStatus.getDescription
  }

}
