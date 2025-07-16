// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils

import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable
import scala.concurrent.blocking

private[bftordering] object Miscellaneous {

  def abort(logger: TracedLogger, message: String)(implicit traceContext: TraceContext): Nothing = {
    logger.error(s"FATAL: $message", new RuntimeException(message))
    throw new RuntimeException(message)
  }

  def mutex[T](lock: AnyRef)(action: => T): T =
    blocking(lock.synchronized(action))

  def dequeueN[ElementT, NumberT](
      queue: mutable.Queue[ElementT],
      n: NumberT,
  )(implicit num: Numeric[NumberT]): Seq[ElementT] = {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var remaining = n
    queue.dequeueWhile { _ =>
      import num.*
      val left = remaining
      remaining = remaining - num.one
      left > num.zero
    }.toSeq
  }
}
