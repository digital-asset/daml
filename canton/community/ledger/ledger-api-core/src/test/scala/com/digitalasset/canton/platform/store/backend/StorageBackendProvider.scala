// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.h2.H2StorageBackendFactory
import com.digitalasset.canton.platform.store.backend.localstore.{
  IdentityProviderStorageBackend,
  PartyRecordStorageBackend,
  UserManagementStorageBackend,
}
import com.digitalasset.canton.platform.store.backend.oracle.OracleStorageBackendFactory
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresStorageBackendFactory
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.interning.MockStringInterning
import com.digitalasset.canton.platform.store.testing.oracle.OracleAroundAll
import com.digitalasset.canton.platform.store.testing.postgresql.PostgresAroundAll
import org.scalatest.Suite

import java.sql.Connection

/** Creates a database and a [[TestBackend]].
  * Used by [[StorageBackendSpec]] to run all StorageBackend tests on different databases.
  */
trait StorageBackendProvider {
  protected def jdbcUrl: String
  protected def lockIdSeed: Int
  protected def backend: TestBackend

  protected final def ingest(dbDtos: Vector[DbDto], connection: Connection): Unit = {
    def typeBoundIngest[T](ingestionStorageBackend: IngestionStorageBackend[T]): Unit =
      ingestionStorageBackend.insertBatch(
        connection,
        ingestionStorageBackend.batch(dbDtos, backend.stringInterningSupport),
      )
    typeBoundIngest(backend.ingestion)
  }

  protected final def updateLedgerEnd(
      ledgerEndOffset: Offset,
      ledgerEndSequentialId: Long,
  )(connection: Connection): Unit = {
    backend.parameter.updateLedgerEnd(LedgerEnd(ledgerEndOffset, ledgerEndSequentialId, 0))(
      connection
    ) // we do not care about the stringInterningId here
    updateLedgerEndCache(connection)
  }

  protected final def updateLedgerEnd(ledgerEnd: LedgerEnd)(connection: Connection): Unit = {
    backend.parameter.updateLedgerEnd(ledgerEnd)(connection)
    updateLedgerEndCache(connection)
  }

  protected final def updateLedgerEndCache(connection: Connection): Unit = {
    val ledgerEnd = backend.parameter.ledgerEnd(connection)
    backend.ledgerEndCache.set(ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId)
  }
}

trait StorageBackendProviderPostgres
    extends StorageBackendProvider
    with PostgresAroundAll
    with BaseTest {
  this: Suite =>
  override protected def jdbcUrl: String = postgresDatabase.url
  override protected val backend: TestBackend = TestBackend(
    PostgresStorageBackendFactory(loggerFactory),
    loggerFactory,
  )
}

trait StorageBackendProviderH2 extends StorageBackendProvider with BaseTest { this: Suite =>
  override protected def jdbcUrl: String = "jdbc:h2:mem:storage_backend_provider;db_close_delay=-1"
  override protected def lockIdSeed: Int =
    throw new UnsupportedOperationException //  DB Locking is not supported for H2
  override protected val backend: TestBackend = TestBackend(H2StorageBackendFactory, loggerFactory)
}

trait StorageBackendProviderOracle
    extends StorageBackendProvider
    with OracleAroundAll
    with BaseTest {
  this: Suite =>
  override protected val backend: TestBackend =
    TestBackend(OracleStorageBackendFactory, loggerFactory)
}

final case class TestBackend(
    ingestion: IngestionStorageBackend[_],
    parameter: ParameterStorageBackend,
    meteringParameter: MeteringParameterStorageBackend,
    configuration: ConfigurationStorageBackend,
    party: PartyStorageBackend,
    packageBackend: PackageStorageBackend,
    completion: CompletionStorageBackend,
    contract: ContractStorageBackend,
    event: EventStorageBackend,
    dataSource: DataSourceStorageBackend,
    dbLock: DBLockStorageBackend,
    integrity: IntegrityStorageBackend,
    reset: ResetStorageBackend,
    stringInterning: StringInterningStorageBackend,
    ledgerEndCache: MutableLedgerEndCache,
    stringInterningSupport: MockStringInterning,
    userManagement: UserManagementStorageBackend,
    participantPartyStorageBackend: PartyRecordStorageBackend,
    metering: TestMeteringBackend,
    identityProviderStorageBackend: IdentityProviderStorageBackend,
    pruningDtoQueries: PruningDtoQueries = new PruningDtoQueries,
)

final case class TestMeteringBackend(
    read: MeteringStorageReadBackend,
    write: MeteringStorageWriteBackend,
)

object TestBackend {
  def apply(
      storageBackendFactory: StorageBackendFactory,
      loggerFactory: NamedLoggerFactory,
  ): TestBackend = {
    val ledgerEndCache = MutableLedgerEndCache()
    val stringInterning = new MockStringInterning

    def createTestMeteringBackend: TestMeteringBackend = {
      TestMeteringBackend(
        read = storageBackendFactory.createMeteringStorageReadBackend(ledgerEndCache),
        write = storageBackendFactory.createMeteringStorageWriteBackend,
      )
    }

    TestBackend(
      ingestion = storageBackendFactory.createIngestionStorageBackend,
      parameter = storageBackendFactory.createParameterStorageBackend,
      meteringParameter = storageBackendFactory.createMeteringParameterStorageBackend,
      configuration = storageBackendFactory.createConfigurationStorageBackend(ledgerEndCache),
      party = storageBackendFactory.createPartyStorageBackend(ledgerEndCache),
      packageBackend = storageBackendFactory.createPackageStorageBackend(ledgerEndCache),
      completion =
        storageBackendFactory.createCompletionStorageBackend(stringInterning, loggerFactory),
      contract =
        storageBackendFactory.createContractStorageBackend(ledgerEndCache, stringInterning),
      event = storageBackendFactory
        .createEventStorageBackend(ledgerEndCache, stringInterning, loggerFactory),
      dataSource = storageBackendFactory.createDataSourceStorageBackend,
      dbLock = storageBackendFactory.createDBLockStorageBackend,
      integrity = storageBackendFactory.createIntegrityStorageBackend,
      reset = storageBackendFactory.createResetStorageBackend,
      stringInterning = storageBackendFactory.createStringInterningStorageBackend,
      ledgerEndCache = ledgerEndCache,
      stringInterningSupport = stringInterning,
      userManagement = storageBackendFactory.createUserManagementStorageBackend,
      participantPartyStorageBackend = storageBackendFactory.createPartyRecordStorageBackend,
      metering = createTestMeteringBackend,
      identityProviderStorageBackend =
        storageBackendFactory.createIdentityProviderConfigStorageBackend,
    )
  }

}
