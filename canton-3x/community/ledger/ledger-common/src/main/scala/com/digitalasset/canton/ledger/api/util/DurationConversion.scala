// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.util

import com.google.protobuf.duration.{Duration as PDuration}

import java.time.{Duration as JDuration}

object DurationConversion {

  def toProto(jDuration: JDuration): PDuration = PDuration(jDuration.getSeconds, jDuration.getNano)

  def fromProto(pDuration: PDuration): JDuration =
    JDuration.ofSeconds(pDuration.seconds, pDuration.nanos.toLong)
}
