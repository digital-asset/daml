// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.model

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.memory.InMemoryAvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.model.Command.{
  AddBatch,
  FetchBatches,
  GC,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  OrderingRequest,
  OrderingRequestBatch,
}
import com.digitalasset.canton.tracing.Traced
import com.google.protobuf.ByteString

import scala.util.Random

class Generator(random: Random, inMemoryStore: InMemoryAvailabilityStore) {

  type Gen[A] = Unit => A

  def genBatchId: Gen[BatchId] = _ => {
    if (inMemoryStore.isEmpty || random.nextBoolean()) {
      BatchId.createForTesting(random.nextLong().toString)
    } else {
      val ix = random.nextInt(inMemoryStore.size)
      inMemoryStore.keys.toSeq(ix)
    }
  }

  def genSeq[A](gen: Gen[A]): Gen[Seq[A]] = _ => {
    (0 until random.nextInt(10)).map(_ => gen.apply(()))
  }

  def genTraced[A](gen: Gen[A]): Gen[Traced[A]] = _ => {
    Traced.empty(gen.apply(()))
  }

  def genString: Gen[String] = _ => s"hello-${random.nextInt()}"
  def genBool: Gen[Boolean] = _ => random.nextBoolean()

  def genOrderingRequest: Gen[OrderingRequest] = _ => {
    OrderingRequest(
      genString.apply(()),
      ByteString.copyFromUtf8(genString.apply(())),
    )
  }

  def genBatch: Gen[OrderingRequestBatch] = _ => {
    OrderingRequestBatch(genSeq(genTraced(genOrderingRequest)).apply(()))
  }

  def generateCommand: Gen[Command] = _ => {
    random.nextInt(3) match {
      case 0 =>
        GC(genSeq(genBatchId).apply(()))
      case 1 =>
        FetchBatches(genSeq(genBatchId).apply(()))
      case _ =>
        AddBatch(genBatchId.apply(()), genBatch.apply(()))
    }
  }
}
