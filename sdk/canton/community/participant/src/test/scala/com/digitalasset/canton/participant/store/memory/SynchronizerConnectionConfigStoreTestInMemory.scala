// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStoreTest
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class SynchronizerConnectionConfigStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with SynchronizerConnectionConfigStoreTest {
  "InMemorySynchronizerConnectionConfigStore" should {
    behave like synchronizerConnectionConfigStore(
      Future.successful(new InMemorySynchronizerConnectionConfigStore(loggerFactory))
    )
  }
}
