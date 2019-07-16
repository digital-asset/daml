// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.infrastructure

private[acceptance] sealed trait Result

private[acceptance] object Result {

  case object Succeeded extends Result

  case object TimedOut extends Result

  final case class Skipped(reason: String) extends Result

  final case class Failed(cause: AssertionError) extends Result

  final case class FailedUnexpectedly(cause: Throwable) extends Result

}
