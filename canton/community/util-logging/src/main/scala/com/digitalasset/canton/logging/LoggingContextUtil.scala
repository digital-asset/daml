// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.daml.logging.LoggingContext
import com.daml.logging.entries.{LoggingEntry, LoggingValue}

object LoggingContextUtil {

  /** Propagates canton logger factory properties to daml LoggingContext
    */
  def createLoggingContext[A](
      loggerFactory: NamedLoggerFactory
  )(code: LoggingContext => A): A =
    loggerFactory.properties.toList match {
      case h :: t =>
        def damlLogEntryFromCantonKeyValue: ((String, String)) => LoggingEntry = { case (k, v) =>
          (k, LoggingValue.OfString(v))
        }
        LoggingContext
          .newLoggingContextWith[A](
            damlLogEntryFromCantonKeyValue(h),
            t.map(damlLogEntryFromCantonKeyValue)*
          )(code)
      case Nil => LoggingContext.newLoggingContext[A](code)
    }
}
