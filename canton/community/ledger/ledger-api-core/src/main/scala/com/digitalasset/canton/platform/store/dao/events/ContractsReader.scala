// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.lf.data.Ref
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.VersionedValue
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle.Timer
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend.{
  RawArchivedContract,
  RawCreatedContract,
}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.dao.events.ContractsReader.*
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.*
import com.digitalasset.canton.platform.store.serialization.{Compression, ValueSerializer}
import com.digitalasset.canton.platform.{Contract, ContractId, Value, *}

import java.io.{ByteArrayInputStream, InputStream}
import scala.concurrent.{ExecutionContext, Future}

private[dao] sealed class ContractsReader(
    contractLoader: ContractLoader,
    storageBackend: ContractStorageBackend,
    dispatcher: DbDispatcher,
    metrics: Metrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends LedgerDaoContractsReader
    with NamedLogging {

  /** Lookup a contract key state at a specific ledger offset.
    *
    * @param key the contract key
    * @param validAt the event_sequential_id of the ledger at which to query for the key state
    * @return the key state.
    */
  override def lookupKeyState(key: Key, validAt: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[KeyState] =
    Timed.future(
      metrics.index.db.lookupKey,
      dispatcher.executeSql(metrics.index.db.lookupContractByKeyDbMetrics)(
        storageBackend.keyState(key, validAt)
      ),
    )

  override def lookupContractState(contractId: ContractId, before: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ContractState]] =
    Timed.future(
      metrics.index.db.lookupActiveContract,
      contractLoader
        .load(contractId -> before)
        .map(_.map {
          case raw: RawCreatedContract =>
            val decompressionTimer =
              metrics.index.db.lookupCreatedContractsDbMetrics.compressionTimer
            val deserializationTimer =
              metrics.index.db.lookupCreatedContractsDbMetrics.translationTimer

            val contract = toContract(
              contractId = contractId,
              templateId = raw.templateId,
              createArgument = raw.createArgument,
              createArgumentCompression =
                Compression.Algorithm.assertLookup(raw.createArgumentCompression),
              decompressionTimer = decompressionTimer,
              deserializationTimer = deserializationTimer,
            )

            val globalKey = raw.createKey.map { key =>
              val keyCompression = Compression.Algorithm.assertLookup(raw.createKeyCompression)
              val decompressed = decompress(key, keyCompression, decompressionTimer)
              val value = deserializeValue(
                decompressed,
                deserializationTimer,
                s"Failed to deserialize create key for contract ${contractId.coid}",
              )
              GlobalKey.assertBuild(
                contract.unversioned.template,
                value.unversioned,
              )
            }

            ActiveContract(
              contract = contract,
              stakeholders = raw.flatEventWitnesses,
              ledgerEffectiveTime = raw.ledgerEffectiveTime,
              agreementText = raw.agreementText,
              signatories = raw.signatories,
              globalKey = globalKey,
              keyMaintainers = raw.keyMaintainers,
              driverMetadata = raw.driverMetadata,
            )
          case raw: RawArchivedContract => ArchivedContract(raw.flatEventWitnesses)
        }),
    )
}

private[dao] object ContractsReader {

  private[dao] def apply(
      contractLoader: ContractLoader,
      dispatcher: DbDispatcher,
      metrics: Metrics,
      storageBackend: ContractStorageBackend,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): ContractsReader = {
    new ContractsReader(
      contractLoader = contractLoader,
      storageBackend = storageBackend,
      dispatcher = dispatcher,
      metrics = metrics,
      loggerFactory = loggerFactory,
    )
  }

  private def decompress(
      data: Array[Byte],
      algorithm: Compression.Algorithm,
      timer: Timer,
  ): InputStream = {
    Timed.value(
      timer,
      value = algorithm.decompress(new ByteArrayInputStream(data)),
    )
  }

  private def deserializeValue(
      decompressed: InputStream,
      timer: Timer,
      errorContext: String,
  ): VersionedValue = {
    Timed.value(
      timer,
      value = ValueSerializer.deserializeValue(decompressed, errorContext),
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
    val decompressed = decompress(createArgument, createArgumentCompression, decompressionTimer)
    val deserialized = deserializeValue(
      decompressed,
      deserializationTimer,
      s"Failed to deserialize create argument for contract ${contractId.coid}",
    )
    Contract(
      template = Identifier.assertFromString(templateId),
      arg = deserialized,
      // TODO(Tudor)
      packageName = Ref.PackageName.assertFromString("dummyReplace")
    )
  }

  private def toContract(
      templateId: String,
      createArgument: Value,
  ): Contract =
    Contract(
      template = Identifier.assertFromString(templateId),
      arg = createArgument,
      // TODO(Tudor)
      packageName = Ref.PackageName.assertFromString("dummyReplace")
    )
}
