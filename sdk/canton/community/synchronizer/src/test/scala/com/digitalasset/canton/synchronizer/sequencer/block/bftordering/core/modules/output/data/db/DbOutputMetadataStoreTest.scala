// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStoreTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.db.DbOutputMetadataStore
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait DbOutputMetadataStoreTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with OutputMetadataStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table ord_metadata_output_blocks",
        sqlu"truncate table ord_metadata_output_epochs",
      ),
      functionFullName,
    )
  }

  "DbEpochStore" should {
    behave like outputBlockMetadataStore(() =>
      new DbOutputMetadataStore(storage, timeouts, loggerFactory)
    )
  }
}

class DbOutputMetadataStoreTestH2 extends DbOutputMetadataStoreTest with H2Test

class DbOutputMetadataStoreTestPostgres extends DbOutputMetadataStoreTest with PostgresTest
