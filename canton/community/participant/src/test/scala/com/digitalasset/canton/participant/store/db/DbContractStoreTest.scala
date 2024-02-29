// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{
  BatchAggregatorConfig,
  CachingConfigs,
  DefaultProcessingTimeouts,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.ContractStoreTest
import com.digitalasset.canton.participant.store.db.DbContractStoreTest.createDbContractStoreForTesting
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.db.{DbStorageIdempotency, DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

trait DbContractStoreTest extends AsyncWordSpec with BaseTest with ContractStoreTest {
  this: DbTest =>

  // Ensure this test can't interfere with the ActiveContractStoreTest
  lazy val domainIndex: Int = DbActiveContractStoreTest.maxDomainIndex + 1

  override def cleanDb(storage: DbStorage): Future[Int] = {
    import storage.api.*
    storage.update(sqlu"delete from par_contracts where domain_id = $domainIndex", functionFullName)
  }

  "DbContractStore" should {
    behave like contractStore(() =>
      createDbContractStoreForTesting(
        storage,
        DomainId.tryFromString("domain-contract-store::default"),
        testedProtocolVersion,
        domainIndex,
        loggerFactory,
      )
    )
  }
}
object DbContractStoreTest {

  def createDbContractStoreForTesting(
      storage: DbStorageIdempotency,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      domainIndex: Int,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): DbContractStore = {
    new DbContractStore(
      storage = storage,
      domainIdIndexed = IndexedDomain.tryCreate(
        domainId,
        domainIndex,
      ),
      protocolVersion = protocolVersion,
      maxContractIdSqlInListSize = PositiveNumeric.tryCreate(2),
      cacheConfig = CachingConfigs.testing.contractStore,
      dbQueryBatcherConfig = BatchAggregatorConfig.defaultsForTesting,
      insertBatchAggregatorConfig = BatchAggregatorConfig.defaultsForTesting,
      timeouts = DefaultProcessingTimeouts.testing,
      loggerFactory = loggerFactory,
    )
  }
}

class ContractStoreTestH2 extends DbContractStoreTest with H2Test

class ContractStoreTestPostgres extends DbContractStoreTest with PostgresTest
