// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.availability.data.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.availability.data.{
  AvailabilityStore,
  AvailabilityStoreTest,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait DbAvailabilityStoreTest extends AvailabilityStoreTest { this: DbTest =>

  override def createStore(): AvailabilityStore[PekkoEnv] =
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
}

class DbAvailabilityStoreTestH2 extends DbAvailabilityStoreTest with H2Test

class DbAvailabilityStoreTestPostgres extends DbAvailabilityStoreTest with PostgresTest
