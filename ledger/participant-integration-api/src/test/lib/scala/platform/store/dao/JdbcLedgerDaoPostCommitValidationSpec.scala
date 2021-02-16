// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.util.UUID

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.dao.events.LfValueTranslation
import org.scalatest.LoneElement
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

private[dao] trait JdbcLedgerDaoPostCommitValidationSpec extends LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  override protected def daoOwner(eventsPageSize: Int)(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[LedgerDao] =
    JdbcLedgerDao
      .validatingWriteOwner(
        serverRole = ServerRole.Testing(getClass),
        jdbcUrl = jdbcUrl,
        connectionPoolSize = 16,
        eventsPageSize = eventsPageSize,
        servicesExecutionContext = executionContext,
        metrics = new Metrics(new MetricRegistry),
        lfValueTranslationCache = LfValueTranslation.Cache.none,
        enricher = None,
      )

  private val ok = io.grpc.Status.Code.OK.value()
  private val invalid = io.grpc.Status.Code.INVALID_ARGUMENT.value()

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
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
    } yield {
      completions should contain.allOf(
        originalAttempt.commandId.get -> ok,
        duplicateAttempt.commandId.get -> invalid,
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
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
    } yield {
      completions should contain.allOf(
        create.commandId.get -> ok,
        lookup.commandId.get -> invalid,
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
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
    } yield {
      completions should contain.allOf(
        create.commandId.get -> ok,
        archive.commandId.get -> ok,
        lookup.commandId.get -> invalid,
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
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
    } yield {
      completions should contain.allOf(
        create.commandId.get -> ok,
        archive.commandId.get -> ok,
        fetch.commandId.get -> invalid,
      )
    }
  }

  // TODO it seems as this test would be based on flawed assumptions (a pure divulgance without create contract having a ledger_effective_time)
  ignore should "be able to use divulged contract in later transaction" in {

    val divulgedContractId =
      ContractId.assertFromString(s"#${UUID.randomUUID}")
    val divulgedContracts =
      Map((divulgedContractId, someVersionedContractInstance) -> Set(alice))

    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, fetch1) <- store(txFetch(alice, divulgedContractId))
      (_, divulgence) <- store(divulgedContracts, blindingInfo = None, emptyTransaction(alice))
      (_, fetch2) <- store(txFetch(alice, divulgedContractId))
      to <- ledgerDao.lookupLedgerEnd()
      completions <- getCompletions(from, to, defaultAppId, Set(alice))
    } yield {
      completions should contain.allOf(
        fetch1.commandId.get -> invalid,
        divulgence.commandId.get -> ok,
        fetch2.commandId.get -> ok,
      )
    }
  }

  // TODO figure out what is leading to exception here (maybe the unique constraint on the event_id?), then figure out whether we can re-add it, or if we need this feature at all?
  ignore should "refuse to insert entries with conflicting transaction ids" in {
    val original = txCreateContractWithKey(alice, "some-key", Some("1337"))
    val duplicateTxId = txCreateContractWithKey(alice, "another-key", Some("1337"))
    recoverToSucceededIf[Exception] {
      for {
        _ <- store(original)
        _ <- store(duplicateTxId)
      } yield ()
    }
  }
}
