// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.model

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.db.DbAvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.memory.InMemoryAvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random

trait ModelBasedTest extends AnyWordSpec with BftSequencerBaseTest { this: DbTest =>

  def createStore(): AvailabilityStore[PekkoEnv] =
    new DbAvailabilityStore(storage, timeouts, loggerFactory)(implicitly[ExecutionContext])

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update_(
      sqlu"truncate table ord_availability_batch",
      functionFullName,
    )
  }

  "DBAvailabilityStore" should {
    "be the same as in-memory store on all random commands" in {
      val seed = Random.nextLong()
      val random = new Random(seed)
      val model = new InMemoryAvailabilityStore()
      val generator = new Generator(random, model)
      val store = createStore()

      val timeout = 10.seconds

      clue(s"Run model based test with seed=${seed}L") {
        for (_ <- 0 until 1000) {
          val command = generator.generateCommand(())
          command match {
            case Command.AddBatch(batchId, batch) =>
              Await.result(store.addBatch(batchId, batch), timeout).discard
              Await.result(model.addBatch(batchId, batch), timeout).discard
            case Command.FetchBatches(batches) =>
              val realValue = Await.result(store.fetchBatches(batches), timeout)
              val modelValue = Await.result(model.fetchBatches(batches), timeout)
              realValue shouldBe modelValue
            case Command.GC(staleBatchIds) =>
              store.gc(staleBatchIds)
              model.gc(staleBatchIds)
          }
        }
      }
    }
  }
}

class ModelBasedTestH2 extends ModelBasedTest with H2Test

// class ModelBasedTestPostgres extends ModelBasedTest with PostgresTest // TODO(#18868) flaky test
