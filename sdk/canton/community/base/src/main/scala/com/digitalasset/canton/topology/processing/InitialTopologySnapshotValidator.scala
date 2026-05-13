// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.data.EitherT
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.topology.TopologyStateProcessor
import com.digitalasset.canton.topology.processing.InitialTopologySnapshotValidator.MergeTx
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.GenericStoredTopologyTransactions
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.transaction.checks.{
  MaybeEmptyTopologyStore,
  RequiredTopologyMappingChecks,
}
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil.syntax.*
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.mutable
import scala.concurrent.ExecutionContext

/** Validates an initial topology snapshot by:
  *
  *   - running transaction authorization validation
  *   - deduplicating topology transactions
  *   - removing superfluous signatures
  *   - checking that serials are strictly monotonic
  *   - checking that rejected transactions in the initial snapshot also are rejected in stored
  *     snapshot
  *   - checking that the effective times valid_from and valid_until of the transactions are the
  *     same in the initial snapshot and the stored snapshot
  *
  * Also compares the computed effective time with the effective provided in the snapshot.
  *
  * Any inconsistency between the topology snapshot and the outcome of the validation is reported.
  *
  * @param validateInitialSnapshot
  *   if false, the validation is skipped and the snapshot is directly imported. this is risky as it
  *   might create a fork if the validation was changed. therefore, we only use this with great
  *   care. the proper solution is to make validation so fast that it doesn't impact performance.
  * @param cleanupTopologySnapshot
  *   if true, then we will clean up the topology snapshot (used for hard migration to clean up the
  *   genesis state)
  */
class InitialTopologySnapshotValidator(
    pureCrypto: CryptoPureApi,
    store: TopologyStore[TopologyStoreId],
    staticSynchronizerParameters: Option[StaticSynchronizerParameters],
    validateInitialSnapshot: Boolean,
    override val loggerFactory: NamedLoggerFactory,
    cleanupTopologySnapshot: Boolean = false,
)(implicit ec: ExecutionContext, materializer: Materializer)
    extends NamedLogging {

  private val storeIsEmpty = new AtomicBoolean(false)
  private val maybeEmptyStore = new MaybeEmptyTopologyStore {
    override def store: TopologyStore[TopologyStoreId] = InitialTopologySnapshotValidator.this.store
    override def skipLoadingFromStore: Boolean = storeIsEmpty.get()
  }
  protected val stateProcessor: TopologyStateProcessor =
    TopologyStateProcessor.forInitialSnapshotValidation(
      store,
      new RequiredTopologyMappingChecks(
        maybeEmptyStore,
        staticSynchronizerParameters,
        loggerFactory,
      ),
      pureCrypto,
      loggerFactory,
    )

  /** Runs the topology snapshot through the normal processing/validation pipeline of the
    * TopologyStateProcessor.
    *
    * '''NOTICE''':
    *   - the preparation and pre-processing of the provided topology snapshot is only done to
    *     support a wider variety of (legacy) topology snapshots.
    *   - The outcome of the validation and import is compared with the expected outcome of the
    *     snapshot. Any inconsistencies are raised as errors. This serves as a security barrier.
    */
  final def validateAndApplyInitialTopologySnapshot(
      initialSnapshot: GenericStoredTopologyTransactions
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val finalSnapshot =
      if (cleanupTopologySnapshot) preprocessInitialSnapshot(initialSnapshot) else initialSnapshot
    if (!validateInitialSnapshot) {
      logger.info("Skipping initial topology snapshot validation")
      EitherT.right(store.bulkInsert(finalSnapshot))
    } else {
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

      logger.info(
        s"Validating ${finalSnapshot.result.size}/${initialSnapshot.result.size} transactions to initialize the topology store ${store.storeId} with ${groupedBySequencedTime.size} sequencer times"
      )
      for {
        _ <- EitherT.right(store.deleteAllData())
        // set to true to bypass loading from store during genesis validation
        _ = storeIsEmpty.set(true)
        _ <-
          groupedBySequencedTime.sequentialTraverse_ { case ((sequenced, validFrom), storedTxs) =>
            processTransactionsAtSequencedTime(sequenced, validFrom, storedTxs, storeIsEmpty.get())
              .map { res =>
                // after the first batch, the store is no longer empty
                storeIsEmpty.set(false)
                res
              }
          }
        _ = logger.info(
          s"Import validated and completed, verifying now that the result matches the imported snapshot."
        )
        // this comparison of the input and output topology snapshots serves as a security guard/barrier
        mismatch <-
          EitherT
            .right(
              Source(finalSnapshot.result)
                .zip(
                  store.findEssentialStateAtSequencedTime(
                    SequencedTime.MaxValue,
                    includeRejected = true,
                  )
                )
                .zipWithIndex
                .dropWhile { case ((fromInitial, fromStore), _) =>
                  // we don't do a complete == comparison, because the snapshot might contain transactions with superfluous
                  // signatures that are now filtered out. As long as the hash, validFrom, validUntil, isProposal and rejection reason
                  // agree between initial and stored topology transaction, we accept the result.
                  StoredTopologyTransaction.equalIgnoringSignatures(fromInitial, fromStore)
                }
                runWith (Sink.headOption)
            )
            .mapK(FutureUnlessShutdown.outcomeK)
        _ <- EitherT.fromEither[FutureUnlessShutdown](
          mismatch
            .map { case ((fromInitial, fromStore), idx) =>
              s"""Mismatch between transactions at index $idx from the initial snapshot and the topology store:
           |Initial: $fromInitial (hash=${fromInitial.hash})
           |Store:   $fromStore (hash=${fromStore.hash})""".stripMargin
            }
            .toLeft(())
        )
      } yield {
        logger.info(
          s"Successfully validated and imported ${finalSnapshot.result.size} topology transactions into the topology store ${store.storeId}."
        )
      }
    }
  }

  /** Fix the initial snapshot from mainnet
    *
    * In the older times, we had to deduplicate and merge transactions here as they were merged in
    * the state processor. This is no longer the case. We just process and store the transactions as
    * we receive them. However, the topology state on mainnet as of 3.3 became dirty due to a bug
    * which threw away the signatures added after the transaction became active. The state was
    * correct, but just the signatures were dropped.
    * https://github.com/DACH-NY/canton-network-internal/issues/2116
    *
    * These transactions are fixed here now by just aggregating all signatures for the same
    * transaction hash for the non-proposals.
    */
  private def preprocessInitialSnapshot(
      initialSnapshot: GenericStoredTopologyTransactions
  )(implicit traceContext: TraceContext): GenericStoredTopologyTransactions = {
    val cleaned = mutable.ArrayBuffer.newBuilder[MergeTx]
    val previous = mutable.Map.empty[TxHash, MergeTx]
    initialSnapshot.result.zipWithIndex
      .filter { case (tx, idx) =>
        if (tx.transaction.isProposal && tx.validUntil.nonEmpty) {
          logger.info(s"Dropping completed proposal at idx=$idx $tx")
          false
        } else true
      }
      .foreach { case (stored, idx) =>
        // do not modify rejected transactions or proposals
        if (stored.rejectionReason.nonEmpty || stored.transaction.isProposal) {
          if (stored.transaction.isProposal) {
            require(
              !previous.contains(stored.hash),
              s"At $idx, found proposal after another transaction with the same hash: " + stored + s" vs pending=${previous
                  .get(stored.hash)}",
            )
          }
          cleaned.addOne(MergeTx(idx, stored))
        } else {
          // merge signatures from previous occurrences of the same transaction
          previous.get(stored.hash) match {
            case Some(existing) if existing.tx != stored =>
              require(
                existing.newValidUntil.get().contains(stored.validFrom),
                s"At $idx, replace tx does not overlap: $existing vs $stored",
              )
              val txWithMergedSignatures = stored.copy(transaction =
                stored.transaction.addSignatures(existing.tx.transaction.signatures)
              )
              if (
                txWithMergedSignatures.transaction.signatures != existing.tx.transaction.signatures
              ) {
                logger.debug(
                  s"At $idx, merging at ${stored.validFrom} from ${existing.tx.validFrom}(idx=${existing.idx}) existing signatures by ${(existing.tx.transaction.signatures -- stored.transaction.signatures)
                      .map(_.authorizingLongTermKey)} into\n  $txWithMergedSignatures"
                )
                val mergeTxWithNewSignatures = MergeTx(idx, txWithMergedSignatures)
                previous.put(txWithMergedSignatures.hash, mergeTxWithNewSignatures).discard
                cleaned.addOne(mergeTxWithNewSignatures)
              } else {
                logger.debug(
                  s"At $idx, no new signatures to merge at ${stored.validFrom} from ${existing.tx.validFrom}, therefore dropping new and overriding valid until of existing: $stored"
                )
                existing.newValidUntil.set(stored.validUntil).discard
              }
            case Some(duplicate) =>
              logger.info(s"At $idx, ignoring observed duplicate transaction: $duplicate")
            case None =>
              val mergeTx = MergeTx(idx, stored)
              previous.put(stored.hash, mergeTx).discard
              cleaned.addOne(mergeTx)
          }
        }
      }
    StoredTopologyTransactions(cleaned.result().map(_.adjusted).toSeq)

  }

  private def processTransactionsAtSequencedTime(
      sequenced: SequencedTime,
      effectiveTime: EffectiveTime,
      storedTxs: Seq[StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]],
      storeIsEmpty: Boolean,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    EitherT
      .right(
        stateProcessor
          .validateAndApplyAuthorization(
            sequenced,
            effectiveTime,
            storedTxs.map(_.transaction),
            expectFullAuthorization = false,
            // when validating the initial snapshot, missing signing key signatures are only
            // acceptable, if the transaction was part of the genesis topology snapshot
            relaxChecksForBackwardsCompatibility =
              sequenced.value == SignedTopologyTransaction.InitialTopologySequencingTime,
            storeIsEmpty = storeIsEmpty,
          )
      )
      .map(_ => ())

}

object InitialTopologySnapshotValidator {
  private final case class MergeTx(idx: Int, tx: GenericStoredTopologyTransaction) {
    val newValidUntil: AtomicReference[Option[EffectiveTime]] = new AtomicReference(tx.validUntil)
    def adjusted: GenericStoredTopologyTransaction = if (tx.validUntil == newValidUntil.get()) tx
    else
      tx.copy(
        validUntil = newValidUntil.get()
      )
  }
}
