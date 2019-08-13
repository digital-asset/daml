// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.grpc.helpers

import scala.util.Try

object TestConfiguration {

  lazy val timeoutInSeconds: Long = {
    val key: String = "JAVA_BINDINGS_API_TESTS_TIMEOUT_SECONDS"
    val default: Long = 5
    sys.env.get(key).fold(default)(value => Try(value.toLong).getOrElse(default))
  }

}
