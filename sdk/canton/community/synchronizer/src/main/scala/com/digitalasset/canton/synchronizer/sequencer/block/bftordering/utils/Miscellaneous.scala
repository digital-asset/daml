// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils

import cats.data.OptionT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level

import scala.collection.mutable
import scala.concurrent.ExecutionContext

private[bftordering] object Miscellaneous {

  def abort(logger: TracedLogger, message: String)(implicit traceContext: TraceContext): Nothing = {
    logger.error(s"FATAL: $message", new RuntimeException(message))
    throw new RuntimeException(message)
  }

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

  def objId(obj: Any): Int = System.identityHashCode(obj)

  def objIdC(obj: Any): String = s"[${obj.getClass.getName}@${objId(obj)}]"

  final case class ResultWithLogs[T](
      result: T,
      logs: (Level, () => String)*
  ) {

    def logAndExtract(logger: TracedLogger, prefix: => String)(implicit
        traceContext: TraceContext
    ): T = {
      logs.foreach { case (level, createAnnotation) =>
        lazy val text = prefix + createAnnotation()
        level match {
          case Level.ERROR => logger.error(s"[FATAL] $text")
          case Level.WARN => logger.warn(s"[UNEXPECTED] $text")
          case Level.INFO => logger.info(text)
          case Level.DEBUG => logger.debug(text)
          case Level.TRACE => logger.trace(text)
        }
      }
      result
    }
  }
  object ResultWithLogs {

    def prefixLogsWith(
        prefix: => String,
        logs: Seq[(Level, () => String)],
    ): Seq[(Level, () => String)] =
      logs.map { case (level, log) =>
        level -> (() => prefix + log())
      }
  }

  def toUnitFutureUS[X](optionT: OptionT[FutureUnlessShutdown, X])(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[Unit] =
    optionT.value.map(_ => ())
}
