// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt

final case class MaxBytesToDecompress(limit: NonNegativeInt) extends AnyVal

object MaxBytesToDecompress {
  // TODO(i10428): Can we remove these?
  val HardcodedDefault: MaxBytesToDecompress = MaxBytesToDecompress(
    NonNegativeInt.tryCreate(10 * 1024 * 1024)
  )

  val MaxValueUnsafe: MaxBytesToDecompress = MaxBytesToDecompress(NonNegativeInt.maxValue)
}
