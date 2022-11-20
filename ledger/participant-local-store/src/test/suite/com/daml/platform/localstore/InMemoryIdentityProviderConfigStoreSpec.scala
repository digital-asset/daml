// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import org.scalatest.freespec.AsyncFreeSpec

class InMemoryIdentityProviderConfigStoreSpec
    extends AsyncFreeSpec
    with IdentityProviderConfigStoreTests {

  override def newStore() = new InMemoryIdentityProviderConfigStore()

}
