// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.json

import scalaz.Show

final case class JsonError(message: String)

object JsonError extends (String => JsonError) {
  implicit val ShowInstance: Show[JsonError] = Show shows { f =>
    s"JsonError: ${f.message}"
  }
}
