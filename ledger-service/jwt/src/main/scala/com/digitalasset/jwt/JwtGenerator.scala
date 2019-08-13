// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.jwt

import scala.util.Try

object JwtGenerator {
  def generate(keys: domain.KeyPair[Seq[Byte]]): Try[domain.Jwt] = Try(domain.Jwt("dummy"))
}
