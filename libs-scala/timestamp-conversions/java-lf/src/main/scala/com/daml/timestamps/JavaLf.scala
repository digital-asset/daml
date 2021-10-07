// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import com.daml.lf.data.Time.{Timestamp => LfTimestamp}

import java.time.{Instant => JavaTimestamp}

object JavaLf {
  implicit class JavaToLfTimestampConversions(timestamp: JavaTimestamp) {
    def asLf: LfTimestamp = LfTimestamp.assertFromInstant(timestamp)
  }

  implicit class LfToJavaTimestampConversions(timestamp: LfTimestamp) {
    def asJava: JavaTimestamp = timestamp.toInstant
  }
}
