// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging.entries

trait ToLoggingKey[-T] {
  def toLoggingKey(key: T): LoggingKey
}

object ToLoggingKey {
  final implicit class ConvertToLoggingKey[T: ToLoggingKey](key: T) {
    def toLoggingKey: LoggingKey = implicitly[ToLoggingKey[T]].toLoggingKey(key)
  }
}
