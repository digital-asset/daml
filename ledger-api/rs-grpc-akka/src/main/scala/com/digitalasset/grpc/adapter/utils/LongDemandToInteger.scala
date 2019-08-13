// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter.utils

object LongDemandToInteger {
  private val intMaxAsLong = Int.MaxValue.toLong

  def apply(l: Long): Int = {
    l match {
      // TODO: should we even support this?
      case Long.MaxValue => Int.MaxValue // According to specification, this means unbounded demand
      case doesNotFitInInteger if doesNotFitInInteger > intMaxAsLong =>
        throw new IllegalArgumentException("Failing fast as demanded is higher than Int.MaxValue")
      case fitsInAnInteger => fitsInAnInteger.toInt
    }
  }
}
