// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.api.util

import java.time.{Duration => JDuration}

import com.google.protobuf.duration.{Duration => PDuration}

object DurationConversion {

  def toProto(jDuration: JDuration): PDuration = PDuration(jDuration.getSeconds, jDuration.getNano)

  def fromProto(pDuration: PDuration): JDuration =
    JDuration.ofSeconds(pDuration.seconds, pDuration.nanos.toLong)
}
