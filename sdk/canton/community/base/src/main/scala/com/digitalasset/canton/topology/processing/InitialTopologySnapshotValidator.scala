// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SynchronizerCryptoPureApi
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.{TopologyChangeOp, TopologyMapping}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.MonadUtil.syntax.*

import scala.concurrent.ExecutionContext

/** Validates an initial topology snapshot by:
  *
  * <ul>
  *   <li>running transaction authorization validation</li>
  *   <li>deduplicating topology transactions</li>
  *   <li>removing superfluous signatures</li>
  *   <li>checking that serials are strictly monotonic</li>
  *   <li>checking that rejected transactions in the initial snapshot also are rejected in stored snapshot</li>
  *   <li>checking that the effective times valid_from and valid_until of the transactions are the same in the initial snapshot
  *   and the stored snapshot</li>
  * </ul>
  *
  * Also compares the computed effective time with the effective provided in the snapshot.
  *
  * Any inconsistency between the topology snapshot and the outcome of the validation is reported.
  */
class InitialTopologySnapshotValidator(
    synchronizerId: SynchronizerId,
    pureCrypto: SynchronizerCryptoPureApi,
    store: TopologyStore[TopologyStoreId.SynchronizerStore],
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionHandling(pureCrypto, store, timeouts, loggerFactory) {

  /** Runs the topology snapshot through the normal processing/validation pipeline of the
    * TopologyStateProcessor.
    * The outcome of the validation is compared with the expected outcome of the snapshot.
    * Any inconsistencies are raised as errors.
    */
  final def validateAndApplyInitialTopologySnapshot(
      initialSnapshot: GenericStoredTopologyTransactions
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.debug(
      s"Validating ${initialSnapshot.result.size} transactions to initialize the topology store for synchronizer $synchronizerId"
    )
    val groupedBySequencedTime: Seq[
      (
          (SequencedTime, EffectiveTime),
          Seq[StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]],
      )
    ] = initialSnapshot.result
      // process transactions from the same timestamp at once
      .groupBy(stored => (stored.sequenced, stored.validFrom))
      .toSeq
      // ensure that the groups of transactions come in the correct order
      .sortBy { case (timestamps, _) => timestamps }

    for {
      _ <- EitherT.right(store.deleteAllData())
      _ <-
        groupedBySequencedTime.sequentialTraverse_ { case ((sequenced, validFrom), storedTxs) =>
          processTransactionsAtSequencedTime(sequenced, validFrom, storedTxs)
        }
      snapshotFromStore <- EitherT.right(
        store.findEssentialStateAtSequencedTime(SequencedTime.MaxValue, includeRejected = true)
      )
      _ <- initialSnapshot.result.zip(snapshotFromStore.result).sequentialTraverse_ {
        case (fromInitial, fromStore) =>
          EitherTUtil.condUnitET[FutureUnlessShutdown](
            fromInitial == fromStore,
            s"""Mismatch between transactions from the initial snapshot and the topology store:
              |Initial: $fromInitial
              |Store:   $fromStore""".stripMargin,
          )
      }
    } yield ()
  }

  private def processTransactionsAtSequencedTime(
      sequenced: SequencedTime,
      effectiveTimeFromSnapshot: EffectiveTime,
      storedTxs: Seq[StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      effectiveTime <- EitherT
        .right(
          timeAdjuster
            .trackAndComputeEffectiveTime(sequenced, strictMonotonicity = true)
        )

      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        effectiveTime == effectiveTimeFromSnapshot,
        s"The computed effective time ($effectiveTime) for sequenced time ($sequenced) is different than the effective time from the topology snapshot ($effectiveTimeFromSnapshot).",
      )

      validatedTxs <- EitherT
        .right(
          stateProcessor
            .validateAndApplyAuthorization(
              sequenced,
              effectiveTime,
              storedTxs.map(_.transaction),
              expectFullAuthorization = false,
            )
        )
      _ = inspectAndAdvanceTopologyTransactionDelay(
        effectiveTime,
        validatedTxs,
      )
    } yield ()
}
