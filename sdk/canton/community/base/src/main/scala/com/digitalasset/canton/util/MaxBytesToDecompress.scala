// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt

final case class MaxBytesToDecompress(limit: NonNegativeInt) extends AnyVal
object MaxBytesToDecompress {
  // TODO(i10428): Move into BaseTest. In prod code this should be drawn from config.
  val Default = MaxBytesToDecompress(NonNegativeInt.tryCreate(10 * 1024 * 1024))
}
