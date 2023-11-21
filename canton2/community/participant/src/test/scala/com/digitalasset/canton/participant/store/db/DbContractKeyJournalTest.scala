// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.participant.store.ContractKeyJournalTest
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.DomainId
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbContractKeyJournalTest extends AsyncWordSpec with BaseTest with ContractKeyJournalTest {
  this: DbTest =>

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    storage.update(
      DBIO
        .seq(sqlu"truncate table contract_key_journal", sqlu"truncate table contract_key_pruning"),
      functionFullName,
    )
  }

  "DbActiveContractStore" should {
    behave like contractKeyJournal(ec =>
      new DbContractKeyJournal(
        storage,
        IndexedDomain.tryCreate(DomainId.tryFromString("contract-key-journal::default"), 1),
        PositiveNumeric.tryCreate(10),
        timeouts,
        loggerFactory,
      )(ec)
    )
  }

}

class ContractKeyJournalTestH2 extends DbContractKeyJournalTest with H2Test

class ContractKeyJournalTestPostgres extends DbContractKeyJournalTest with PostgresTest
