// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.api.grpc
import java.time
import java.time.Duration

import com.google.protobuf.duration

object GrpcApiUtil {

  def durationToProto(d: time.Duration): duration.Duration =
    duration.Duration(d.getSeconds, d.getNano)

  def durationFromProto(d: duration.Duration): time.Duration =
    Duration.ofSeconds(d.seconds, d.nanos.toLong)
}
