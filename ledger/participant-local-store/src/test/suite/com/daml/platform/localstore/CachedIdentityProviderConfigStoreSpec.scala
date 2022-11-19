// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.metrics.Metrics
import com.daml.platform.localstore.api.IdentityProviderConfigStore
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.freespec.AsyncFreeSpec

class CachedIdentityProviderConfigStoreSpec
    extends AsyncFreeSpec
    with IdentityProviderConfigStoreTests
    with MockitoSugar
    with ArgumentMatchersSugar {

  override def newStore(): IdentityProviderConfigStore = new CachedIdentityProviderConfigStore(
    new InMemoryIdentityProviderConfigStore(),
    1,
    10,
    Metrics.ForTesting,
  )

}
