// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.io.ByteArrayInputStream
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import com.codahale.metrics.Timer
import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.interfaces.LedgerDaoContractsReader._
import com.daml.platform.store.appendonlydao.events.ContractsReader._
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.backend.ContractStorageBackend
import com.daml.platform.store.cache.StringInterning
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.serialization.{Compression, ValueSerializer}

import scala.concurrent.{ExecutionContext, Future}

private[appendonlydao] sealed class ContractsReader(
    storageBackend: ContractStorageBackend,
    dispatcher: DbDispatcher,
    metrics: Metrics,
    ledgerEnd: AtomicReference[(Offset, Long)], // TODO make it just an accessor function
    stringInterning: StringInterning,
)(implicit ec: ExecutionContext)
    extends LedgerDaoContractsReader {

  override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Instant]] =
    Timed.future(
      metrics.daml.index.db.lookupMaximumLedgerTime,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupMaximumLedgerTimeDbMetrics)(
          storageBackend.maximumLedgerTime(ids, ledgerEnd.get()._2)
        )
        .map(_.get),
    )

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
        storageBackend.keyState(key, validAt, stringInterning)
      ),
    )

  override def lookupContractState(contractId: ContractId, before: Long)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractState]] =
    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics)(
          storageBackend.contractState(contractId, before, stringInterning)
        )
        .map(_.map {
          case raw if raw.eventKind == 10 =>
            val contract = toContract(
              contractId = contractId,
              templateId =
                assertPresent(raw.templateId)("template_id must be present for a create event"),
              createArgument = assertPresent(raw.createArgument)(
                "create_argument must be present for a create event"
              ),
              createArgumentCompression =
                Compression.Algorithm.assertLookup(raw.createArgumentCompression),
              decompressionTimer =
                metrics.daml.index.db.lookupActiveContractDbMetrics.compressionTimer,
              deserializationTimer =
                metrics.daml.index.db.lookupActiveContractDbMetrics.translationTimer,
            )
            ActiveContract(
              contract,
              raw.flatEventWitnesses,
              assertPresent(raw.ledgerEffectiveTime)(
                "ledger_effective_time must be present for a create event"
              ),
            )
          case raw if raw.eventKind == 20 => ArchivedContract(raw.flatEventWitnesses)
          case raw => throw ContractsReaderError(s"Unexpected event kind ${raw.eventKind}")
        }),
    )

  /** Lookup of a contract in the case the contract value is not already known */
  override def lookupActiveContractAndLoadArgument(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] = {

    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics)(
          storageBackend.activeContractWithArgument(
            readers,
            contractId,
            ledgerEnd.get()._2,
            stringInterning,
          )
        )
        .map(_.map { raw =>
          toContract(
            contractId = contractId,
            templateId = raw.templateId,
            createArgument = raw.createArgument,
            createArgumentCompression =
              Compression.Algorithm.assertLookup(raw.createArgumentCompression),
            decompressionTimer =
              metrics.daml.index.db.lookupActiveContractDbMetrics.compressionTimer,
            deserializationTimer =
              metrics.daml.index.db.lookupActiveContractDbMetrics.translationTimer,
          )
        }),
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
          storageBackend.activeContractWithoutArgument(
            readers,
            contractId,
            ledgerEnd.get()._2,
            stringInterning,
          )
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

  override def lookupContractKey(
      key: Key,
      readers: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[ContractId]] =
    Timed.future(
      metrics.daml.index.db.lookupKey,
      dispatcher.executeSql(metrics.daml.index.db.lookupContractByKeyDbMetrics)(
        storageBackend.contractKey(readers, key, ledgerEnd.get()._2, stringInterning)
      ),
    )
}

private[appendonlydao] object ContractsReader {

  private[appendonlydao] def apply(
      dispatcher: DbDispatcher,
      metrics: Metrics,
      storageBackend: ContractStorageBackend,
      ledgerEnd: AtomicReference[(Offset, Long)],
      stringInterning: StringInterning,
  )(implicit ec: ExecutionContext): ContractsReader = {
    new ContractsReader(
      storageBackend = storageBackend,
      dispatcher = dispatcher,
      metrics = metrics,
      ledgerEnd = ledgerEnd,
      stringInterning = stringInterning,
    )
  }

  // The contracts table _does not_ store agreement texts as they are
  // unnecessary for interpretation and validation. The contracts returned
  // from this table will _always_ have an empty agreement text.
  private def toContract(
      contractId: ContractId,
      templateId: String,
      createArgument: Array[Byte],
      createArgumentCompression: Compression.Algorithm,
      decompressionTimer: Timer,
      deserializationTimer: Timer,
  ): Contract = {
    val decompressed =
      Timed.value(
        timer = decompressionTimer,
        value = createArgumentCompression.decompress(new ByteArrayInputStream(createArgument)),
      )
    val deserialized =
      Timed.value(
        timer = deserializationTimer,
        value = ValueSerializer.deserializeValue(
          stream = decompressed,
          errorContext = s"Failed to deserialize create argument for contract ${contractId.coid}",
        ),
      )
    Contract(
      template = Identifier.assertFromString(templateId),
      arg = deserialized,
      agreementText = "",
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

  private def assertPresent[T](in: Option[T])(err: String): T =
    in.getOrElse(throw ContractsReaderError(err))

  case class ContractsReaderError(msg: String) extends RuntimeException(msg)
}
