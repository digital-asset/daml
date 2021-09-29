// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import org.slf4j.Logger
import scala.collection.mutable.ArrayBuffer
import com.daml.lf.data.Ref.Location
import com.daml.scalautil.Statement.discard

private[lf] final case class Warning(
    commitLocation: Option[Location],
    message: String,
) {
  def messageWithLocation: String =
    s"${Pretty.prettyLoc(commitLocation).renderWideStream.mkString}: $message"
}

private[lf] final class WarningLog(logger: Logger) {
  private[this] val buffer = new ArrayBuffer[Warning](initialSize = 10)

  def add(warning: Warning): Unit = {
    logger.warn(warning.messageWithLocation)
    discard(buffer += warning)
  }

  def iterator: Iterator[Warning] = buffer.iterator
}
