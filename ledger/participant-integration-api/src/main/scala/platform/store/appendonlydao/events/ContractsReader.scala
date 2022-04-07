// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import com.daml.error.definitions.IndexErrors
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.appendonlydao.events.ContractsReader._
import com.daml.platform.store.backend.ContractStorageBackend
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.interfaces.LedgerDaoContractsReader._

import com.daml.ledger.participant.state.v2.ContractPayloadStore

import scala.concurrent.{ExecutionContext, Future}

private[appendonlydao] sealed class ContractsReader(
    storageBackend: ContractStorageBackend,
    contractPayloadStore: ContractPayloadStore,
    dispatcher: DbDispatcher,
    metrics: Metrics,
)(implicit ec: ExecutionContext)
    extends LedgerDaoContractsReader {
  private val logger = ContextualizedLogger.get(getClass)

  /** Lookup a contract key state at a specific ledger offset.
    *
    * @param key the contract key
    * @param validAt the event_sequential_id of the ledger at which to query for the key state
    * @return the key state.
    */
  override def lookupKeyState(key: Key, validAt: Long)(implicit
      loggingContext: LoggingContext
  ): Future[KeyState] =
    Timed.future(
      metrics.daml.index.db.lookupKey,
      dispatcher.executeSql(metrics.daml.index.db.lookupContractByKeyDbMetrics)(
        storageBackend.keyState(key, validAt)
      ),
    )

  override def lookupContractState(contractId: ContractId, before: Long)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractState]] = {
    implicit val errorLogger: ContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, loggingContext, None)
    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics)(
          storageBackend.contractState(contractId, before)
        )
        .flatMap {
          case Some(raw) if raw.eventKind == 10 =>
            contractPayloadStore
              .loadContractPayloads(Set(contractId))
              .map(contracts =>
                Some(
                  ActiveContract(
                    contracts(contractId),
                    raw.flatEventWitnesses,
                    assertPresent(raw.ledgerEffectiveTime)(
                      "ledger_effective_time must be present for a create event"
                    ),
                  )
                )
              )
          case Some(raw) if raw.eventKind == 20 =>
            Future.successful(Some(ArchivedContract(raw.flatEventWitnesses)))
          case Some(raw) =>
            throw throw IndexErrors.DatabaseErrors.ResultSetError
              .Reject(s"Unexpected event kind ${raw.eventKind}")
              .asGrpcError
          case None => Future.successful(None)
        },
    )
  }

  /** Lookup of a contract in the case the contract value is not already known */
  override def lookupActiveContractAndLoadArgument(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] = {

    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics)(
          storageBackend.activeContractWithoutArgument(readers, contractId)
        )
        .flatMap {
          case Some(_) =>
            contractPayloadStore
              .loadContractPayloads(Set(contractId))
              .map { contracts =>
                Some(contracts(contractId))
              }

          case None =>
            Future.successful(None)
        },
    )
  }

  /** Lookup of a contract in the case the contract value is already known (loaded from a cache) */
  override def lookupActiveContractWithCachedArgument(
      readers: Set[Party],
      contractId: ContractId,
      createArgument: Value,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] = {

    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics)(
          storageBackend.activeContractWithoutArgument(readers, contractId)
        )
        .map(
          _.map(templateId =>
            toContract(
              templateId = templateId,
              createArgument = createArgument,
            )
          )
        ),
    )
  }
}

private[appendonlydao] object ContractsReader {

  private[appendonlydao] def apply(
      dispatcher: DbDispatcher,
      metrics: Metrics,
      storageBackend: ContractStorageBackend,
      contractPayloadStore: ContractPayloadStore,
  )(implicit ec: ExecutionContext): ContractsReader = {
    new ContractsReader(
      storageBackend = storageBackend,
      dispatcher = dispatcher,
      metrics = metrics,
      contractPayloadStore = contractPayloadStore,
    )
  }

  private def toContract(
      templateId: String,
      createArgument: Value,
  ): Contract =
    Contract(
      template = Identifier.assertFromString(templateId),
      arg = createArgument,
      agreementText = "",
    )

  private def assertPresent[T](in: Option[T])(err: String)(implicit
      errorLogger: ContextualizedErrorLogger
  ): T =
    in.getOrElse(throw IndexErrors.DatabaseErrors.ResultSetError.Reject(err).asGrpcError)
}
