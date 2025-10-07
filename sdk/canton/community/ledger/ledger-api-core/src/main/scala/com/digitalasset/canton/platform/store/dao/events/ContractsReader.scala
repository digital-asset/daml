// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.metrics.Timed
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.ExistingContractStatus
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.*

import scala.concurrent.{ExecutionContext, Future}

private[dao] sealed class ContractsReader(
    contractLoader: ContractLoader,
    storageBackend: ContractStorageBackend,
    dispatcher: DbDispatcher,
    metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LedgerDaoContractsReader
    with NamedLogging {

  /** Batch lookup of contract keys
    *
    * Used to unit test the SQL queries for key lookups. Does not use batching.
    */
  override def lookupKeyStatesFromDb(keys: Seq[Key], notEarlierThanEventSeqId: Long)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Map[Key, KeyState]] =
    Timed.future(
      metrics.index.db.lookupKey,
      dispatcher.executeSql(metrics.index.db.lookupContractByKeyDbMetrics)(
        storageBackend.keyStates(keys, notEarlierThanEventSeqId)
      ),
    )

  /** Lookup a contract key state at a specific ledger offset.
    *
    * @param key
    *   the contract key
    * @param notEarlierThanEventSeqId
    *   the lower bound offset of the ledger for which to query for the key state
    * @return
    *   the key state.
    */
  override def lookupKeyState(key: Key, notEarlierThanEventSeqId: Long)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[KeyState] =
    Timed.future(
      metrics.index.db.lookupKey,
      contractLoader.keys.load(key -> notEarlierThanEventSeqId).map {
        case Some(value) => value
        case None =>
          logger
            .error(
              s"Key $key resulted in an invalid empty load at offset $notEarlierThanEventSeqId"
            )(
              loggingContext.traceContext
            )
          KeyUnassigned
      },
    )

  override def lookupContractState(contractId: ContractId, notEarlierThanEventSeqId: Long)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ExistingContractStatus]] =
    Timed.future(
      metrics.index.db.lookupActiveContract,
      contractLoader.contracts.load(contractId -> notEarlierThanEventSeqId),
    )
}

private[dao] object ContractsReader {

  private[dao] def apply(
      contractLoader: ContractLoader,
      dispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      storageBackend: ContractStorageBackend,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): ContractsReader =
    new ContractsReader(
      contractLoader = contractLoader,
      storageBackend = storageBackend,
      dispatcher = dispatcher,
      metrics = metrics,
      loggerFactory = loggerFactory,
    )

}
