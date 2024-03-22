// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.InFlightSubmissionStoreTest

class InFlightSubmissionStoreTestInMemory extends InFlightSubmissionStoreTest {
  "InMemoryInFlightSubmissionStore" should {
    behave like (inFlightSubmissionStore(() => new InMemoryInFlightSubmissionStore(loggerFactory)))
  }
}
