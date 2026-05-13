// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

import scala.concurrent.ExecutionContext

trait ConcurrentCacheSpecBase {
  protected def name: String

  protected def newCache()(implicit
      executionContext: ExecutionContext
  ): ConcurrentCache[Integer, String]
}
