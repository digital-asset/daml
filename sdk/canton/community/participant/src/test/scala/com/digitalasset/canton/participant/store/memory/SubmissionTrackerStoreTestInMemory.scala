// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.store.SubmissionTrackerStoreTest

class SubmissionTrackerStoreTestInMemory extends SubmissionTrackerStoreTest {
  "InMemorySubmissionTrackerStore" should {
    behave like submissionTrackerStore(() => new InMemorySubmissionTrackerStore(loggerFactory))
  }
}
