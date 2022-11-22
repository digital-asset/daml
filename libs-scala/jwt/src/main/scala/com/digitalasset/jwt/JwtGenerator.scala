// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import scala.util.Try

object JwtGenerator {
  def generate: Try[domain.Jwt] = Try(domain.Jwt("dummy"))
}
