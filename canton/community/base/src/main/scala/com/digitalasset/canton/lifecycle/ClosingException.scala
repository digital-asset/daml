// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import java.util.concurrent.RejectedExecutionException

abstract class CancellationException(message: String) extends RuntimeException(message)

/** An operation has been cancelled due to shutdown/closing of a component. */
class ClosedCancellationException(message: String) extends CancellationException(message)

/** Helper to pattern match for exceptions that may happen during shutdown/closing. */
object ClosingException {
  def apply(t: Throwable): Boolean = t match {
    case _: RejectedExecutionException | _: CancellationException | _: InterruptedException => true
    case _ => false
  }

  def unapply(t: Throwable): Option[Throwable] = Some(t).filter(apply)
}
