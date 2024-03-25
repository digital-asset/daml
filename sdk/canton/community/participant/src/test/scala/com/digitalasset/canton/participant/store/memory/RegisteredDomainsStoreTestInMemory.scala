// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.RegisteredDomainsStoreTest
import org.scalatest.wordspec.AsyncWordSpec

class RegisteredDomainsStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with RegisteredDomainsStoreTest {
  "InMemoryRegisteredDomainsStore" should {
    behave like registeredDomainsStore(() => new InMemoryRegisteredDomainsStore(loggerFactory))
  }
}
