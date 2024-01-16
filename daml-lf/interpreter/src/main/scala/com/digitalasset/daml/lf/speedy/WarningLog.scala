// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import scala.collection.mutable.ArrayBuffer
import com.daml.lf.data.Ref.Location
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.scalautil.Statement.discard

private[lf] final case class Warning(
    commitLocation: Option[Location],
    message: String,
) {
  def messageWithLocation: String =
    s"${Pretty.prettyLoc(commitLocation).renderWideStream.mkString}: $message"
}

private[lf] final class WarningLog(logger: ContextualizedLogger) {
  private[this] val buffer = new ArrayBuffer[Warning](initialSize = 10)

  private[speedy] def add(warning: Warning)(implicit context: LoggingContext): Unit = {
    logger.warn(warning.messageWithLocation)
    discard(buffer += warning)
  }

  def iterator: Iterator[Warning] = buffer.iterator
}
