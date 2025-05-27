// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

object BooleanUtil {

  object implicits {
    import scala.language.implicitConversions

    class BooleanOps(b: Boolean) {
      def toInt: Int =
        // Delegates to java.lang.Boolean#compare that is implemented as `return x == y ? 0 : (x ? 1 : -1)`
        b.compare(false)
    }

    implicit def booleanToBooleanOps(b: Boolean): BooleanOps = new BooleanOps(b)
  }
}
