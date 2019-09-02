// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import java.time.Instant

import com.google.protobuf.timestamp.Timestamp

package object infrastructure {

  def timestampToInstant(t: Timestamp): Instant =
    Instant.EPOCH.plusSeconds(t.seconds).plusNanos(t.nanos.toLong)

  def instantToTimestamp(t: Instant): Timestamp =
    new Timestamp(t.getEpochSecond, t.getNano)

}
