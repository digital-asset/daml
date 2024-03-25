// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

object StackTraceUtil {

  def formatStackTrace(filter: Thread => Boolean = _ => true): String = {
    import scala.jdk.CollectionConverters.*
    Thread.getAllStackTraces.asScala.toMap
      .filter { case (thread, _) => filter(thread) }
      .map { case (thread, stackTrace) =>
        formatThread(thread) + formatStackTrace(stackTrace)
      }
      .mkString("\n")
  }

  def formatThread(thread: Thread): String =
    s"  ${thread.toString} is-daemon=${thread.isDaemon} state=${thread.getState.toString}"

  def formatStackTrace(stackTrace: Array[StackTraceElement]): String = if (stackTrace.isEmpty) ""
  else stackTrace.mkString("\n    ", "\n    ", "\n")

  def caller(offset: Int = 1): String = {
    val stack = Thread.currentThread().getStackTrace
    if (stack.lengthCompare(offset) > 0) {
      val cal = stack(offset)
      s"${cal.getFileName}:${cal.getLineNumber}"
    } else "unknown"
  }

}
