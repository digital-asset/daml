// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

object Utils {

  /** @param n needs to be positive
    */
  def largestSmallerOrEqualPowerOfTwo(n: Int): Int =
    Integer.highestOneBit(n)

}
