// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import data.Ref

private[lf] final case class StackTrace(frames: Vector[Ref.Location]) {
  // Return the most recent frame
  def topFrame: Option[Ref.Location] =
    frames.headOption
  def pretty(l: Ref.Location) =
    s"${l.definition} at ${l.packageId}:${l.module}:${l.start._1}"
  def pretty(): String =
    frames.view.map(pretty(_)).mkString("\n")
}

private[lf] object StackTrace {
  val Empty = StackTrace(Vector.empty)
}
