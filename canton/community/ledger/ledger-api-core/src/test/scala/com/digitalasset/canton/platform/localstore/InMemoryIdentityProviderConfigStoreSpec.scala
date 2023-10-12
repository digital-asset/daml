// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore

import com.digitalasset.canton.BaseTest
import org.scalatest.freespec.AsyncFreeSpec

class InMemoryIdentityProviderConfigStoreSpec
    extends AsyncFreeSpec
    with IdentityProviderConfigStoreTests
    with BaseTest {

  override def newStore() =
    new InMemoryIdentityProviderConfigStore(loggerFactory, MaxIdentityProviderConfigs)
}
