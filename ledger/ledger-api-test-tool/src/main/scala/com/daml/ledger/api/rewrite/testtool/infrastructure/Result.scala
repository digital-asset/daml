// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool.infrastructure

private[testtool] sealed trait Result {
  def failure: Boolean
}

private[testtool] object Result {

  case object Succeeded extends Result {
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
