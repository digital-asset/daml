// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.store.{
  AcsCommitmentStoreTest,
  CommitmentQueueTest,
  IncrementalCommitmentStoreTest,
}

import scala.concurrent.ExecutionContext

class AcsCommitmentStoreTestInMemory extends AcsCommitmentStoreTest {

  "InMemoryAcsCommitmentStore" should {
    behave like acsCommitmentStore((ec: ExecutionContext) =>
      new InMemoryAcsCommitmentStore(
        synchronizerId,
        new InMemoryAcsCommitmentConfigStore(),
        loggerFactory,
      )(ec)
    )
  }
}

class AcsSnapshotTestInMemory extends IncrementalCommitmentStoreTest {
  "InMemoryAcsCommitmentStore" should {
    behave like commitmentSnapshotStore(_ =>
      new InMemoryIncrementalCommitments(RecordTime.MinValue, Map.empty)
    )
  }
}

class CommitmentQueueTestInMemory extends CommitmentQueueTest {
  "InMemoryAcsCommitmentStore" should {
    behave like commitmentQueue(_ => new InMemoryCommitmentQueue())
  }
}
