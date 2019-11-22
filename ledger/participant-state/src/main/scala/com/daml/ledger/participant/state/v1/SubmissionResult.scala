// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

sealed abstract class SubmissionResult extends Product with Serializable {
  def description: String
}

object SubmissionResult {

  /** The request has been received */
  final case object Acknowledged extends SubmissionResult {
    override def description: String =
      s"The request has been received"
  }

  /** The system is overloaded, clients should back off exponentially */
  final case object Overloaded extends SubmissionResult {
    override val description: String = "System is overloaded, please try again later"
  }

  /** Submission is not supported */
  final case object NotSupported extends SubmissionResult {
    override def description: String =
      s"Submission is not supported"
  }

  /** Submission ended up with internal error */
  final case class InternalError(reason: String) extends SubmissionResult {
    override def description: String =
      s"Failed with an internal error, reason=$reason"
  }
}
