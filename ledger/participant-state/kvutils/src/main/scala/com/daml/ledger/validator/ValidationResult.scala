// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

sealed trait ValidationResult

object ValidationResult {

  case object SubmissionValidated extends ValidationResult

  sealed trait ValidationFailed extends RuntimeException with ValidationResult

  final case class MissingInputState(keys: Seq[Array[Byte]]) extends ValidationFailed

  final case class ValidationError(reason: String) extends ValidationFailed

  final case class TransformedSubmission[T](value: T)

}
