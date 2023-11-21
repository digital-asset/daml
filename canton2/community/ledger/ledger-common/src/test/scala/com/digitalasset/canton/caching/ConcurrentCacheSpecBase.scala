// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.caching

trait ConcurrentCacheSpecBase {
  protected def name: String

  protected def newCache(): ConcurrentCache[Integer, String]
}
