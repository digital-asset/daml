// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import java.math.BigDecimal

object BigDecimalImplicits {

  implicit class IntToBigDecimal(value: Int) {
    def toBigDecimal: BigDecimal = BigDecimal.valueOf(value.toLong)
  }

  implicit class DoubleToBigDecimal(value: Double) {
    def toBigDecimal: BigDecimal = BigDecimal.valueOf(value)
  }

}
