// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.time.Duration

private[testtool] sealed trait Result {
  def failure: Boolean
}

private[testtool] object Result {

  final case class Succeeded(duration: Duration) extends Result {
    val failure: Boolean = false
  }

  final case class Skipped(reason: String) extends Result {
    val failure: Boolean = false
  }

  case object TimedOut extends Result {
    val failure: Boolean = true
  }

  final case class Failed(cause: AssertionError) extends Result {
    val failure: Boolean = true
  }

  final case class FailedUnexpectedly(cause: Throwable) extends Result {
    val failure: Boolean = true
  }

}
