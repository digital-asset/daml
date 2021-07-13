// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import org.slf4j.Logger
import scala.collection.mutable.ArrayBuffer

private[lf] final case class WarningLog(logger: Logger) {
  private[this] val buffer = new ArrayBuffer[String](initialSize = 10)
  def add(message: String): Unit = {
    if (logger.isWarnEnabled) {
      logger.warn(message)
    }
    buffer.addOne(message)
  }

  def iterator: Iterator[String] = buffer.iterator
}
