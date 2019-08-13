// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

sealed abstract class SubmissionResult extends Product with Serializable

object SubmissionResult {

  /** The request has been received */
  final case object Acknowledged extends SubmissionResult

  /** The system is overloaded, clients should back off exponentially */
  final case object Overloaded extends SubmissionResult

  /** Submission is not supported */
  final case object NotSupported extends SubmissionResult

}
