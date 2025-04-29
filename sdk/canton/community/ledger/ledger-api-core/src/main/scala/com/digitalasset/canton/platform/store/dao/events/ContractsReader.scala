// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle.Timer
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.*
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
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.data.Ref.PackageName
import com.digitalasset.daml.lf.transaction.{GlobalKeyWithMaintainers, Node}
import com.digitalasset.daml.lf.value.Value.VersionedValue

import java.io.{ByteArrayInputStream, InputStream}
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
  override def lookupKeyStatesFromDb(keys: Seq[Key], notEarlierThanOffset: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Map[Key, KeyState]] =
    Timed.future(
      metrics.index.db.lookupKey,
      dispatcher.executeSql(metrics.index.db.lookupContractByKeyDbMetrics)(
        storageBackend.keyStates(keys, notEarlierThanOffset)
      ),
    )

  /** Lookup a contract key state at a specific ledger offset.
    *
    * @param key
    *   the contract key
    * @param notEarlierThanOffset
    *   the lower bound offset of the ledger for which to query for the key state
    * @return
    *   the key state.
    */
  override def lookupKeyState(key: Key, notEarlierThanOffset: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[KeyState] =
    Timed.future(
      metrics.index.db.lookupKey,
      contractLoader.keys.load(key -> notEarlierThanOffset).map {
        case Some(value) => value
        case None =>
          logger
            .error(s"Key $key resulted in an invalid empty load at offset $notEarlierThanOffset")(
              loggingContext.traceContext
            )
          KeyUnassigned
      },
    )

  override def lookupContractState(contractId: ContractId, notEarlierThanOffset: Offset)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ContractState]] =
    Timed.future(
      metrics.index.db.lookupActiveContract,
      contractLoader.contracts
        .load(contractId -> notEarlierThanOffset)
        .map(_.map {
          case raw: RawCreatedContract =>
            val decompressionTimer =
              metrics.index.db.lookupCreatedContractsDbMetrics.compressionTimer
            val deserializationTimer =
              metrics.index.db.lookupCreatedContractsDbMetrics.translationTimer

            val packageName = PackageName.assertFromString(raw.packageName)
            val templateId = Identifier.assertFromString(raw.templateId)
            val createArg = {
              val argCompression = Compression.Algorithm.assertLookup(raw.createArgumentCompression)
              val decompressed = decompress(raw.createArgument, argCompression, decompressionTimer)
              deserializeValue(
                decompressed,
                deserializationTimer,
                s"Failed to deserialize create argument for contract ${contractId.coid}",
              )
            }

            val keyOpt: Option[KeyWithMaintainers] = (raw.createKey, raw.keyMaintainers) match {
              case (None, None) =>
                None
              case (Some(key), Some(maintainers)) =>
                val keyCompression = Compression.Algorithm.assertLookup(raw.createKeyCompression)
                val decompressed = decompress(key, keyCompression, decompressionTimer)
                val value = deserializeValue(
                  decompressed,
                  deserializationTimer,
                  s"Failed to deserialize create key for contract ${contractId.coid}",
                )
                Some(
                  GlobalKeyWithMaintainers.assertBuild(
                    templateId = templateId,
                    value = value.unversioned,
                    maintainers = maintainers,
                    packageName = packageName,
                  )
                )
              case (keyOpt, _) =>
                val msg =
                  s"contract ${contractId.coid} has " +
                    (if (keyOpt.isDefined) "a key but no maintainers" else "maintainers but no key")
                logger.error(msg)(loggingContext.traceContext)
                sys.error(msg)
            }
            ActiveContract(
              FatContract.fromCreateNode(
                Node.Create(
                  coid = contractId,
                  packageName = packageName,
                  templateId = templateId,
                  arg = createArg.unversioned,
                  signatories = raw.signatories,
                  stakeholders = raw.flatEventWitnesses,
                  keyOpt = keyOpt,
                  version = createArg.version,
                ),
                createTime = raw.ledgerEffectiveTime,
                cantonData = Bytes.fromByteArray(raw.driverMetadata),
              )
            )
          case raw: RawArchivedContract => ArchivedContract(raw.flatEventWitnesses)
        }),
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

  private def decompress(
      data: Array[Byte],
      algorithm: Compression.Algorithm,
      timer: Timer,
  ): InputStream =
    Timed.value(
      timer,
      value = algorithm.decompress(new ByteArrayInputStream(data)),
    )

  private def deserializeValue(
      decompressed: InputStream,
      timer: Timer,
      errorContext: String,
  ): VersionedValue =
    Timed.value(
      timer,
      value = ValueSerializer.deserializeValue(decompressed, errorContext),
    )

}
