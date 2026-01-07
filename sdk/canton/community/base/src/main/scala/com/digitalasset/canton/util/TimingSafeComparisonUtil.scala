// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

object TimingSafeComparisonUtil {

  /** Timing-safe String comparison implementation so that the runtime is independent of the
    * position of the first differing character.
    */
  def constantTimeEquals(a: String, b: String): Boolean =
    if (a.length != b.length) false
    else {
      // runtime depends on length only, as foldLeft does not short-circuit
      a.zip(b).foldLeft(true) { case (r, (ac, bc)) => r & (ac == bc) }
    }
}
