// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import io.grpc.StatusRuntimeException

sealed abstract class SubmissionResult extends Product with Serializable {
  def description: String
}

object SubmissionResult {

  /** The request has been received */
  case object Acknowledged extends SubmissionResult {
    override val description: String = "The request has been received"
  }

  /** The system is overloaded, clients should back off exponentially */
  case object Overloaded extends SubmissionResult {
    override val description: String = "System is overloaded, please try again later"
  }

  /** Submission is not supported */
  case object NotSupported extends SubmissionResult {
    override val description: String = "Submission is not supported"
  }

  /** Submission ended up with internal error */
  final case class InternalError(reason: String) extends SubmissionResult {
    override val description: String = s"Failed with an internal error, reason=$reason"
  }

  /** Temporary method to tunnel new error codes through the ledger-api server */
  final case class SynchronousReject(failure: StatusRuntimeException) extends SubmissionResult {
    override def description: String = failure.getStatus.getDescription
  }

}
