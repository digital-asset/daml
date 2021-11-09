// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.util.UUID

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.transaction.BlindingInfo
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.{DbType, LfValueTranslationCache}
import com.daml.platform.store.appendonlydao.events.CompressionStrategy
import com.daml.platform.store.appendonlydao.{
  DbDispatcher,
  JdbcLedgerDao,
  LedgerDao,
  SequentialWriteDao,
}
import com.daml.platform.store.backend.StorageBackendFactory
import com.daml.platform.store.interning.StringInterningView
import org.scalatest.LoneElement
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

private[dao] trait JdbcLedgerDaoPostCommitValidationSpec extends LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  override protected def daoOwner(
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      errorFactories: ErrorFactories,
  )(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[LedgerDao] = {
    val metrics = new Metrics(new MetricRegistry)
    val dbType = DbType.jdbcType(jdbcUrl)
    val storageBackendFactory = StorageBackendFactory.of(dbType)
    val participantId = Ref.ParticipantId.assertFromString("JdbcLedgerDaoPostCommitValidationSpec")
    DbDispatcher
      .owner(
        dataSource = storageBackendFactory.createDataSourceStorageBackend.createDataSource(jdbcUrl),
        serverRole = ServerRole.Testing(getClass),
        connectionPoolSize = dbType.maxSupportedWriteConnections(16),
        connectionTimeout = 250.millis,
        metrics = metrics,
      )
      .map { dbDispatcher =>
        val stringInterningStorageBackend =
          storageBackendFactory.createStringInterningStorageBackend
        val stringInterningView = new StringInterningView(
          loadPrefixedEntries = (fromExclusive, toInclusive) =>
            implicit loggingContext =>
              dbDispatcher.executeSql(metrics.daml.index.db.loadStringInterningEntries) {
                stringInterningStorageBackend.loadStringInterningEntries(fromExclusive, toInclusive)
              }
        )
        JdbcLedgerDao.validatingWrite(
          dbDispatcher = dbDispatcher,
          sequentialWriteDao = SequentialWriteDao(
            participantId = participantId,
            lfValueTranslationCache = LfValueTranslationCache.Cache.none,
            metrics = metrics,
            compressionStrategy = CompressionStrategy.none(metrics),
            ledgerEndCache = ledgerEndCache,
            stringInterningView = stringInterningView,
            ingestionStorageBackend = storageBackendFactory.createIngestionStorageBackend,
            parameterStorageBackend = storageBackendFactory.createParameterStorageBackend,
          ),
          eventsPageSize = eventsPageSize,
          eventsProcessingParallelism = eventsProcessingParallelism,
          servicesExecutionContext = executionContext,
          metrics = metrics,
          lfValueTranslationCache = LfValueTranslationCache.Cache.none,
          enricher = None,
          participantId = participantId,
          storageBackendFactory = storageBackendFactory,
          errorFactories = errorFactories,
          ledgerEndCache = ledgerEndCache,
          stringInterning = stringInterningView,
        )
      }
  }

  private val ok = io.grpc.Status.Code.OK.value()
  private val aborted = io.grpc.Status.Code.ABORTED.value()

  behavior of "JdbcLedgerDao (post-commit validation)"

  it should "refuse to serialize duplicate contract keys" in {
    val keyValue = s"duplicate-key"

    // Scenario: Two concurrent commands create the same contract key.
    // At command interpretation time, the keys do not exist yet.
    // At serialization time, the ledger should refuse to serialize one of them.
    for {
      from <- ledgerDao.lookupLedgerEnd()
      original @ (_, originalAttempt) = txCreateContractWithKey(alice, keyValue)
      duplicate @ (_, duplicateAttempt) = txCreateContractWithKey(alice, keyValue)
      _ <- store(original)
      _ <- store(duplicate)
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from.lastOffset, to.lastOffset, defaultAppId, Set(alice))
    } yield {
      completions should contain.allOf(
        originalAttempt.commandId.get -> ok,
        duplicateAttempt.commandId.get -> aborted,
      )
    }
  }

  it should "refuse to serialize invalid negative lookupByKey" in {
    val keyValue = s"no-invalid-negative-lookup"

    // Scenario: Two concurrent commands: one create and one lookupByKey.
    // At command interpretation time, the lookupByKey does not find any contract.
    // At serialization time, it should be rejected because now the key is there.
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, create) <- store(txCreateContractWithKey(alice, keyValue))
      (_, lookup) <- store(txLookupByKey(alice, keyValue, None))
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from.lastOffset, to.lastOffset, defaultAppId, Set(alice))
    } yield {
      completions should contain.allOf(
        create.commandId.get -> ok,
        lookup.commandId.get -> aborted,
      )
    }
  }

  it should "refuse to serialize invalid positive lookupByKey" in {
    val keyValue = s"no-invalid-positive-lookup"

    // Scenario: Two concurrent commands: one exercise and one lookupByKey.
    // At command interpretation time, the lookupByKey finds a contract.
    // At serialization time, it should be rejected because now the contract was archived.
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, create) <- store(txCreateContractWithKey(alice, keyValue))
      createdContractId = nonTransient(create).loneElement
      (_, archive) <- store(txArchiveContract(alice, createdContractId -> Some(keyValue)))
      (_, lookup) <- store(txLookupByKey(alice, keyValue, Some(createdContractId)))
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from.lastOffset, to.lastOffset, defaultAppId, Set(alice))
    } yield {
      completions should contain.allOf(
        create.commandId.get -> ok,
        archive.commandId.get -> ok,
        lookup.commandId.get -> aborted,
      )
    }
  }

  it should "refuse to serialize invalid fetch" in {
    val keyValue = s"no-invalid-fetch"

    // Scenario: Two concurrent commands: one exercise and one fetch.
    // At command interpretation time, the fetch finds a contract.
    // At serialization time, it should be rejected because now the contract was archived.
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, create) <- store(txCreateContractWithKey(alice, keyValue))
      createdContractId = nonTransient(create).loneElement
      (_, archive) <- store(txArchiveContract(alice, createdContractId -> Some(keyValue)))
      (_, fetch) <- store(txFetch(alice, createdContractId))
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from.lastOffset, to.lastOffset, defaultAppId, Set(alice))
    } yield {
      completions should contain.allOf(
        create.commandId.get -> ok,
        archive.commandId.get -> ok,
        fetch.commandId.get -> aborted,
      )
    }
  }

  it should "be able to use divulged contract in later transaction" in {

    val divulgedContractId =
      ContractId.assertFromString(s"#${UUID.randomUUID}")
    val divulgedContracts =
      Map((divulgedContractId, someVersionedContractInstance) -> Set(alice))

    val blindingInfo = BlindingInfo(
      disclosure = Map.empty,
      divulgence = Map(divulgedContractId -> Set(alice)),
    )

    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, fetch1) <- store(txFetch(alice, divulgedContractId))
      (_, divulgence) <- store(
        divulgedContracts,
        blindingInfo = Some(blindingInfo),
        emptyTransaction(alice),
      )
      (_, fetch2) <- store(txFetch(alice, divulgedContractId))
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from.lastOffset, to.lastOffset, defaultAppId, Set(alice))
    } yield {
      completions should contain.allOf(
        fetch1.commandId.get -> aborted,
        divulgence.commandId.get -> ok,
        fetch2.commandId.get -> ok,
      )
    }
  }

  it should "do not refuse to insert entries with conflicting transaction ids" in {
    val original = txCreateContractWithKey(alice, "some-key", Some("1337"))
    val duplicateTxId = txCreateContractWithKey(alice, "another-key", Some("1337"))

    // Post-commit validation does not prevent duplicate transaction ids
    for {
      _ <- store(original)
      _ <- store(duplicateTxId)
    } yield succeed
  }
}
