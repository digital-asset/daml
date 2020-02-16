// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

sealed trait ValidationResult[+T]

object ValidationResult {

  final case class SubmissionValidated[T](value: T) extends ValidationResult[T]

  object SubmissionValidated {
    val unit: SubmissionValidated[Unit] = SubmissionValidated(())
  }

  sealed trait ValidationFailed extends RuntimeException with ValidationResult[Nothing]

  final case class MissingInputState(keys: Seq[Array[Byte]]) extends ValidationFailed

  final case class ValidationError(reason: String) extends ValidationFailed

}
