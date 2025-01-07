// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.ErrorLoggingContext
import org.slf4j.event.Level

object FutureUnlessShutdownUtil {

  /** If the future fails, log the associated error and re-throw. The returned future completes after logging.
    * @param logPassiveInstanceAtInfo: If true, log [[PassiveInstanceException]] at INFO instead of ERROR level. Default is false.
    */
  def logOnFailureUnlessShutdown[T](
      future: FutureUnlessShutdown[T],
      failureMessage: => String,
      onFailure: Throwable => Unit = _ => (),
      level: => Level = Level.ERROR,
      closeContext: Option[CloseContext] = None,
      logPassiveInstanceAtInfo: Boolean = false,
  )(implicit loggingContext: ErrorLoggingContext): FutureUnlessShutdown[T] =
    FutureUnlessShutdown(
      FutureUtil.logOnFailure(
        future.unwrap,
        failureMessage,
        onFailure,
        level,
        closeContext,
        logPassiveInstanceAtInfo,
      )
    )

  /** [[FutureUtil.doNotAwait]] but for FUS
    */
  def doNotAwaitUnlessShutdown[A](
      future: FutureUnlessShutdown[A],
      failureMessage: => String,
      onFailure: Throwable => Unit = _ => (),
      level: => Level = Level.ERROR,
      closeContext: Option[CloseContext] = None,
  )(implicit loggingContext: ErrorLoggingContext): Unit =
    FutureUtil.doNotAwait(future.unwrap, failureMessage, onFailure, level, closeContext)

}
