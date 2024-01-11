// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import com.digitalasset.canton.store.SequencedEventStoreTest
import com.digitalasset.canton.{BaseTest, TestMetrics}
import org.scalatest.wordspec.AsyncWordSpec

class SequencedEventStoreTestInMemory
    extends AsyncWordSpec
    with BaseTest
    with SequencedEventStoreTest
    with TestMetrics {

  "MessageStoreTestInMemory" should {
    behave like sequencedEventStore(ec => new InMemorySequencedEventStore(loggerFactory)(ec))
  }

}
