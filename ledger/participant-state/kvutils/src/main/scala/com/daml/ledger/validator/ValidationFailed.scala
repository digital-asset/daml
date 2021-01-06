// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import scala.util.control.NoStackTrace

sealed trait ValidationFailed extends RuntimeException with NoStackTrace

object ValidationFailed {

  final case class MissingInputState(keys: Seq[Raw.Key]) extends ValidationFailed

  final case class ValidationError(reason: String) extends ValidationFailed

}
