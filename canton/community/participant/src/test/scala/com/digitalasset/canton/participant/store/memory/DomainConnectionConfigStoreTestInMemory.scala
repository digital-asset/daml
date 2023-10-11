// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.DomainConnectionConfigStoreTest
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class DomainConnectionConfigStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with DomainConnectionConfigStoreTest {
  "InMemoryDomainConnectionConfigStore" should {
    behave like domainConnectionConfigStore(
      Future.successful(new InMemoryDomainConnectionConfigStore(loggerFactory))
    )
  }
}
