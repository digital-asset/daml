// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

object NeverCacheUpdatePolicy extends CacheUpdatePolicy[Any] {
  override def shouldCacheOnWrite(key: Any): Boolean = false

  override def shouldCacheOnRead(key: Any): Boolean = false
}
