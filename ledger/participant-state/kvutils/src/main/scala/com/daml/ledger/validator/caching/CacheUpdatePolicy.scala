// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey

trait CacheUpdatePolicy {
  def shouldCacheOnWrite(key: DamlStateKey): Boolean

  def shouldCacheOnRead(key: DamlStateKey): Boolean
}
