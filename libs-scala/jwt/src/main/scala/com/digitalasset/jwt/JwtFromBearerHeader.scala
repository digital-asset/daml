// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

object JwtFromBearerHeader {
  private val BearerTokenRegex = "Bearer (.+)".r

  def apply(header: String): Either[Error, String] = BearerTokenRegex
    .findFirstMatchIn(header)
    .map(_.group(1))
    .toRight(
      Error(Symbol("JwtFromBearerHeader"), "Authorization header does not use Bearer format")
    )

}
