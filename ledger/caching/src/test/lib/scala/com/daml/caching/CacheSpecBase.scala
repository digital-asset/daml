// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.caching

trait CacheSpecBase {
  protected def name: String

  protected def newCache(): Cache[Integer, String]
}
