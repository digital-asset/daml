// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

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
    * TODO(v2) I don't think there's much point in fleshing out a detailed error model at this point
    *  because the error conditions will change as we implement new features. To support error refinement in
    *  the [[WriteService]] clients, we may later flesh out a more detailed error model or rely on conventions in
    *  how to report refinable error conditions
    */
  case class SynchronousError(grpcError: com.google.rpc.status.Status) extends SubmissionResult {
    override val description: String = s"Submission failed with error ${grpcError.message}"
  }
}
