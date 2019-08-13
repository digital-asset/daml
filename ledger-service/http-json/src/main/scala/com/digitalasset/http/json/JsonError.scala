// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import scalaz.Show
import scalaz.syntax.show._

final case class JsonError(message: String)

object JsonError {
  def toJsonError(e: String) = JsonError(e)

  def toJsonError[E: Show](e: E) = JsonError(e.shows)

  def toJsonError(e: Throwable) = JsonError(e.getMessage)

  implicit val ShowInstance: Show[JsonError] = Show shows { f =>
    s"JsonError: ${f.message}"
  }
}
