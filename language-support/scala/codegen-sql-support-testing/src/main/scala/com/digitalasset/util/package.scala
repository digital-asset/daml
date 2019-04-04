// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

package object util {

  private val logger = LoggerFactory.getLogger(getClass)

  def retryingAfter[A](retryPeriods: Duration*)(process: () => A): A = {
    try {
      process()
    } catch {
      case NonFatal(e) =>
        retryPeriods match {
          case period +: tail =>
            logger.warn(
              s"Exception occurred, waiting $period before retrying, here is the error",
              e)
            Thread.sleep(period.toMillis)
            retryingAfter(tail: _*)(process)
          case _ =>
            throw e
        }
    }
  }

}
