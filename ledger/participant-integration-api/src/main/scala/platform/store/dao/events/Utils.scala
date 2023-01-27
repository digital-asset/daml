// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

object Utils {

  /** @param n needs to be positive
    */
  def largestSmallerPowerOfTwo(n: Int): Int =
    Integer.highestOneBit(n)

}
