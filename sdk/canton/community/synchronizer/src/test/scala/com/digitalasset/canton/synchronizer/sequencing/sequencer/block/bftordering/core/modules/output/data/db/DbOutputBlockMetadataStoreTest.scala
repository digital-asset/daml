// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.output.data.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStoreTest
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbOutputBlockMetadataStoreTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with OutputBlockMetadataStoreTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage)(implicit traceContext: TraceContext): Future[Int] = {
    import storage.api.*
    storage.update(
      sqlu"truncate table ord_metadata_output_blocks",
      functionFullName,
    )
  }

  "DbEpochStore" should {
    behave like outputBlockMetadataStore(() =>
      new DbOutputBlockMetadataStore(storage, timeouts, loggerFactory)
    )
  }
}

class DbOutputBlockMetadataStoreTestH2 extends DbOutputBlockMetadataStoreTest with H2Test

class DbOutputBlockMetadataStoreTestPostgres
    extends DbOutputBlockMetadataStoreTest
    with PostgresTest
