// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.util

import com.google.protobuf.timestamp.Timestamp

import java.time.{Duration, Instant}

object TimeUtil {

  def timestampToInstant(timestamp: Timestamp): Instant =
    Instant.ofEpochSecond(timestamp.seconds.toLong, timestamp.nanos.toLong)

  def durationBetween(before: Timestamp, after: Instant): Duration =
    Duration.between(timestampToInstant(before), after)

  def durationBetween(before: Instant, after: Instant): Duration =
    Duration.between(before, after)
}
