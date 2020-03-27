// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import scala.concurrent.duration.Duration

private[testtool] object Result {

  sealed trait Success

  final case class Succeeded(duration: Duration) extends Success
  final case class Skipped(reason: String) extends Success

  sealed trait Failure

  case object TimedOut extends Failure
  final case class Failed(cause: AssertionError) extends Failure
  final case class FailedUnexpectedly(cause: Throwable) extends Failure

}
