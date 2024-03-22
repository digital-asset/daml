// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import scalaz.Show

final case class Error(what: Symbol, message: String)

object Error {
  implicit val showInstance: Show[Error] =
    Show.shows(e => s"JwtVerifier.Error: ${e.what}, ${e.message}")
}
