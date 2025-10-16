// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import com.digitalasset.canton.store.PendingOperationStoreTest.TestPendingOperationMessage
import com.digitalasset.canton.store.{
  PendingOperation,
  PendingOperationStore,
  PendingOperationStoreTest,
}
import com.google.protobuf.ByteString

import scala.concurrent.Future

class InMemoryPendingOperationStoreTest
    extends PendingOperationStoreTest[TestPendingOperationMessage] {

  override protected def insertCorruptedData(
      op: PendingOperation[TestPendingOperationMessage],
      store: Option[PendingOperationStore[TestPendingOperationMessage]],
      corruptOperationBytes: Option[ByteString] = None,
  ): Future[Unit] = {
    // Cast the store to its concrete type to access its internal state
    val inMemoryStore =
      store.value.asInstanceOf[InMemoryPendingOperationStore[TestPendingOperationMessage]]

    val corruptStoredOp = InMemoryPendingOperationStore.StoredPendingOperation(
      trigger = op.trigger.asString,
      serializedSynchronizerId = op.synchronizerId.toProtoPrimitive,
      key = op.key,
      name = op.name.unwrap,
      serializedOperation = corruptOperationBytes.getOrElse(op.operation.toByteString),
    )

    // Insert the corrupt data into the storage (the internal map)
    inMemoryStore.store.put(op.compositeKey, corruptStoredOp)

    Future.unit
  }

  "InMemoryPendingOperationStore" should {
    behave like pendingOperationsStore(() =>
      new InMemoryPendingOperationStore(TestPendingOperationMessage)
    )
  }
}
