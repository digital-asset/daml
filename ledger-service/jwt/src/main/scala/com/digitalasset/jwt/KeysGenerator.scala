// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.jwt

import java.io.File

import scala.util.Try

object KeysGenerator {
  def generate(secret: String): Try[domain.Keys] =
    Try(domain.Keys(new File("dummy-publicKey"), new File("dummy-privateKey")))

}
