// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.platform.localstore.PartyRecordStoreSpecBase.StoreContainer
import org.scalatest.freespec.AsyncFreeSpec

class InMemoryPartyRecordStoreSpec extends AsyncFreeSpec with PartyRecordStoreTests {

  override def newStore(): StoreContainer = StoreContainer(
    new InMemoryPartyRecordStore(
      executionContext = executionContext
    ),
    new InMemoryIdentityProviderConfigStore(),
  )

}
