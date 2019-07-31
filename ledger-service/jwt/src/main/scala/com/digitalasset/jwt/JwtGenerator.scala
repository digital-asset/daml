// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.jwt

import scala.util.Try

object JwtGenerator {
  def generate(keys: domain.KeyPair): Try[domain.Jwt] = Try(domain.Jwt("dummy"))
}
