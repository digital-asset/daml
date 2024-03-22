// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

trait ConcurrentCacheSpecBase {
  protected def name: String

  protected def newCache(): ConcurrentCache[Integer, String]
}
