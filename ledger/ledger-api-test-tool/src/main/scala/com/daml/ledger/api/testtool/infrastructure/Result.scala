// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import scala.concurrent.duration.Duration
import scala.util.control.NoStackTrace

private[testtool] object Result {

  sealed trait Success

  final case class Succeeded(duration: Duration) extends Success
  case object Retired extends RuntimeException with NoStackTrace with Success

  sealed trait Failure

  case object TimedOut extends Failure
  final case class Failed(cause: AssertionError) extends Failure
  final case class FailedUnexpectedly(cause: Throwable) extends Failure

}
