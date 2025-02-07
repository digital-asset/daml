// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyReturningOps.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SynchronizerCryptoPureApi
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.{TopologyChangeOp, TopologyMapping}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.MonadUtil.syntax.*
import com.digitalasset.canton.version.ProtocolVersion

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
    protocolVersion: ProtocolVersion,
    pureCrypto: SynchronizerCryptoPureApi,
    store: TopologyStore[TopologyStoreId.SynchronizerStore],
    insecureIgnoreMissingExtraKeySignaturesInInitialSnapshot: Boolean,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionHandling(
      insecureIgnoreMissingExtraKeySignatures =
        insecureIgnoreMissingExtraKeySignaturesInInitialSnapshot,
      pureCrypto,
      store,
      timeouts,
      loggerFactory,
    ) {

  /** Runs the topology snapshot through the normal processing/validation pipeline of the
    * TopologyStateProcessor.
    *
    * <strong>NOTICE</strong>:
    * <ol>
    *   <li>the preparation and pre-processing of the provided topology snapshot is only done to
    *  support a wider variety of (legacy) topology snapshots.</li>
    *  <li>The outcome of the validation and import is compared with the expected outcome of the snapshot.
    *  Any inconsistencies are raised as errors. This serves as a security barrier.</li>
    * </ol
    */
  final def validateAndApplyInitialTopologySnapshot(
      initialSnapshot: GenericStoredTopologyTransactions
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {

    // the following preprocessing is necessary because the topology transactions have been assigned the same timestamp
    // upon export and it's possible that the following situation happened:
    // ---------------
    // original store:
    // ts1: tx hashOfSignatures = h1, validFrom = ts1, validUntil = ts2
    // ts2: tx hashOfSignatures = h1, validFrom = ts2
    // ---------------
    // since the topology transaction was stored at two different timestamps, they were inserted into the table just as expected.
    // but upon export the transactions have the same timestamp:
    // ---------------
    // initial snapshot:
    // ts1: tx hashOfSignatures = h1, validFrom = ts1, validUntil = ts1
    // ts1: tx hashOfSignatures = h1, validFrom = ts1
    // ---------------
    // Therefore the second insert would be ignored because of the deduplication via the unique index and "on conflict do nothing".
    // To work around this, we combine the two transaction entries (they are literally the same) by doing the following:
    // * take the validFrom value from the first occurrence
    // * take the validUntil value from the last occurrence
    // * only retain the first occurrence of the transaction with the updated validFrom/validUntil. We need to do this
    //   because there could be another transaction between the duplicates, that depends on the first duplicate to have been valid.
    val finalSnapshot = StoredTopologyTransactions(
      initialSnapshot.result
        // first retain the global order of the topology transactions within the snapshot
        .zipWithIndex
        // Find the transaction entries with the same hash of signatures at the same sequenced timestamp.
        // The problematic scenario above is only relevant for the genesis snapshot, in which all topology
        // transactions have the same sequenced/effective time.
        // However, for onboarding snapshots (no matter which node), we MUST not merge transactions from
        // different timestamps, because each transaction may affect the epsilon tracker and therefore
        // must be preserved.
        .groupBy1 { case (tx, _idx) =>
          (tx.sequenced, tx.hash, tx.transaction.hashOfSignatures(protocolVersion))
        }
        .toSeq
        .flatMap { case ((sequenced, _, _), transactions) =>
          // onboarding snapshots should only have a single transaction per bucket, because topology
          // transactions are compacted (see `TopologyStateProcessor`) and the effective times are preserved.

          // genesis snapshots produced by canton (and not assembled manually by a user) do not contain
          // rejected transactions.

          // NOTICE: given the above assumptions, there should not be a need for the partitioning of the
          // topology transactions, but we keep it in to potentially handle a snapshot correctly that we wouldn't otherwise.

          // for all non-rejected transactions with the same hash of signatures,
          // only retain a single entry
          // * at the lowest index (ie earliest occurrence),
          // * with validFrom of the lowest index (i.e. earliest occurrence),
          // * with validUntil of the highest index (i.e. latest occurrence)
          //
          // All rejected transactions can stay in the snapshot as they are.
          //
          // Proposals do not need special treatment, because they should have
          // different sets of signatures and not enough signatures to be fully authorized.
          // Therefore proposals should end up in separate groups of transactions or
          // they can be merged regardless.
          val (nonRejected, rejected) = transactions.partition { case (tx, _idx) =>
            tx.rejectionReason.isEmpty
          }
          // only merge non-rejected transactions
          val mergedNonRejected = NonEmpty
            .from(nonRejected)
            .map { nonRejectedNE =>
              val (txWithMinIndex, minIndex) = nonRejectedNE.minBy1 { case (tx_, idx) => idx }
              val (txWithMaxIndex, _) = nonRejectedNE.maxBy1 { case (tx_, idx) => idx }
              val retainedTransaction = txWithMinIndex.copy(validUntil = txWithMaxIndex.validUntil)
              if (nonRejectedNE.sizeIs > 1) {
                logger.info(s"""Combining duplicate valid transactions at $sequenced
                     |originals: $nonRejected
                     |result  : $retainedTransaction""".stripMargin)
              }
              (
                (
                  retainedTransaction,
                  minIndex,
                ),
              )

            }
            .toList
          // return the merged and the rejected transactions.
          // sorting by index to retain the original order will happen afterwards
          (mergedNonRejected ++ rejected)
        }
        // re-establish the original order by index
        .sortBy { case (_tx, idx) => idx }
        // throw away the index
        .map { case (tx, _idx) => tx }
    )

    logger.debug(
      s"Validating ${finalSnapshot.result.size}/${initialSnapshot.result.size} transactions to initialize the topology store for synchronizer $synchronizerId"
    )
    val groupedBySequencedTime: Seq[
      (
          (SequencedTime, EffectiveTime),
          Seq[StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]],
      )
    ] = finalSnapshot.result
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
      // this comparison of the input and output topology snapshots serves as a security guard/barrier
      _ <- finalSnapshot.result.zip(snapshotFromStore.result).zipWithIndex.sequentialTraverse_ {
        case ((fromInitial, fromStore), idx) =>
          EitherTUtil.condUnitET[FutureUnlessShutdown](
            // we don't do a complete == comparison, because the snapshot might contain transactions with superfluous
            // signatures that are now filtered out. As long as the hash, validFrom, validUntil, isProposal and rejection reason
            // agree between initial and stored topology transaciton, we accept the result.
            StoredTopologyTransaction.equalIgnoringSignatures(fromInitial, fromStore),
            s"""Mismatch between transactions at index $idx from the initial snapshot and the topology store:
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

      validationResult <- EitherT
        .right(
          stateProcessor
            .validateAndApplyAuthorization(
              sequenced,
              effectiveTime,
              storedTxs.map(_.transaction),
              expectFullAuthorization = false,
              // The snapshot might contain transactions that only add additional
              // signatures. Since the genesis snapshot usually has all transactions at the same timestamp
              // and we compare the provided snapshot with what is stored one-by-one,
              // we don't want to compact during the validation of the initial snapshot, as this
              // would combine multiple transactions into one.
              compactTransactions = false,
            )
        )
      (validatedTxs, _) = validationResult
      _ = inspectAndAdvanceTopologyTransactionDelay(
        effectiveTime,
        validatedTxs,
      )
    } yield ()
}
