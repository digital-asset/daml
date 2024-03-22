// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.json

import scalaz.Show

final case class JsonError(message: String)

object JsonError extends (String => JsonError) {
  implicit val ShowInstance: Show[JsonError] = Show shows { f =>
    s"JsonError: ${f.message}"
  }
}
