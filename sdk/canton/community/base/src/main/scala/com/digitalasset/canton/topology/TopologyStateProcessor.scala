// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.instances.seq.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.AsyncResult
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionAuthorizationValidator,
}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransactions,
  TopologyMappingChecks,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** @param outboxQueue
  *   If a [[SynchronizerOutboxQueue]] is provided, the processed transactions are not directly
  *   stored, but rather sent to the synchronizer via an ephemeral queue (i.e. no persistence).
  */
class TopologyStateProcessor private (
    val store: TopologyStore[TopologyStoreId],
    outboxQueue: Option[SynchronizerOutboxQueue],
    topologyMappingChecks: TopologyMappingChecks,
    pureCrypto: CryptoPureApi,
    loggerFactoryParent: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  override protected val loggerFactory: NamedLoggerFactory =
    // only add the `store` key for the authorized store. In case this TopologyStateProcessor is for a synchronizer,
    // it will already have the `synchronizer` key, so having `store` with the same synchronizerId is just a waste
    if (store.storeId == AuthorizedStore) {
      loggerFactoryParent.append("store", store.storeId.toString)
    } else loggerFactoryParent

  // small container to store potentially pending data
  private case class MaybePending(originalTx: GenericSignedTopologyTransaction)
      extends PrettyPrinting {
    val adjusted = new AtomicReference[Option[GenericSignedTopologyTransaction]](None)
    val rejection = new AtomicReference[Option[TopologyTransactionRejection]](None)
    val expireImmediately = new AtomicBoolean(false)

    def currentTx: GenericSignedTopologyTransaction = adjusted.get().getOrElse(originalTx)

    def validatedTx: GenericValidatedTopologyTransaction =
      ValidatedTopologyTransaction(currentTx, rejection.get(), expireImmediately.get())

    override protected def pretty: Pretty[MaybePending] =
      prettyOfClass(
        param("original", _.originalTx),
        paramIfDefined("adjusted", _.adjusted.get()),
        paramIfDefined("rejection", _.rejection.get()),
        paramIfTrue("expireImmediately", _.expireImmediately.get()),
      )
  }

  private val txForMapping = TrieMap[MappingHash, MaybePending]()
  private val proposalsByMapping = TrieMap[MappingHash, Seq[TxHash]]()
  private val proposalsForTx = TrieMap[TxHash, MaybePending]()

  private val authValidator =
    new TopologyTransactionAuthorizationValidator(
      pureCrypto,
      store,
      // if transactions are put directly into a store (ie there is no outbox queue)
      // then the authorization validation is final.
      validationIsFinal = outboxQueue.isEmpty,
      loggerFactory.append("role", if (outboxQueue.isEmpty) "incoming" else "outgoing"),
    )

  // compared to the old topology stores, the x stores don't distinguish between
  // state & transaction store. cascading deletes are irrevocable and delete everything
  // that depended on a certificate.

  /** validate the authorization and the signatures of the given transactions
    *
    * The function is NOT THREAD SAFE AND MUST RUN SEQUENTIALLY
    */
  def validateAndApplyAuthorization(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      transactionsToValidate: Seq[GenericSignedTopologyTransaction],
      expectFullAuthorization: Boolean,
      transactionMayHaveMissingSigningKeySignatures: Boolean,
      compactTransactions: Boolean = true,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(Seq[GenericValidatedTopologyTransaction], AsyncResult[Unit])] = {
    // if transactions aren't persisted in the store but rather enqueued in the synchronizer outbox queue,
    // the processing should abort on errors, because we don't want to enqueue rejected transactions.
    val abortOnError = outboxQueue.nonEmpty

    type Lft = Seq[GenericValidatedTopologyTransaction]

    val transactions =
      if (compactTransactions) SignedTopologyTransactions.compact(transactionsToValidate)
      else transactionsToValidate

    // first, pre-load the currently existing mappings and proposals for the given transactions
    val preloadTxsForMappingF = preloadTxsForMapping(effective, transactions)
    val preloadProposalsForTxF = preloadProposalsForTx(effective, transactions)
    val ret = for {
      _ <- EitherT.right[Lft](preloadProposalsForTxF)
      _ <- EitherT.right[Lft](preloadTxsForMappingF)
      // compute / collapse updates
      (removesF, pendingWrites) = {
        val pendingWrites = transactions.map(MaybePending.apply)
        val removes = pendingWrites
          .foldLeftM((Map.empty[MappingHash, PositiveInt], Set.empty[TxHash])) {
            case ((removeMappings, removeTxs), tx) =>
              validateAndMerge(
                effective,
                tx.originalTx,
                expectFullAuthorization = expectFullAuthorization || !tx.originalTx.isProposal,
                transactionMayHaveMissingSigningKeySignatures =
                  transactionMayHaveMissingSigningKeySignatures,
              ).map { finalTx =>
                tx.adjusted.set(Some(finalTx.transaction))
                tx.rejection.set(finalTx.rejectionReason)
                determineRemovesAndUpdatePending(tx, removeMappings, removeTxs)
              }
          }
        (removes, pendingWrites)
      }
      removes <- EitherT.right[Lft](removesF)
      (mappingRemoves, txRemoves) = removes
      validatedTx = pendingWrites.map(pw => pw.validatedTx)
      _ <- EitherT.cond[FutureUnlessShutdown](
        !abortOnError || validatedTx.forall(_.rejectionReason.isEmpty),
        (), {
          logger.info("Topology transactions failed:\n  " + validatedTx.mkString("\n  "))
          // reset caches as they are broken now if we abort
          clearCaches()
          validatedTx
        }: Lft,
      ): EitherT[FutureUnlessShutdown, Lft, Unit]
      // string approx for output
      epsilon =
        s"${effective.value.toEpochMilli - sequenced.value.toEpochMilli}"
      ln = validatedTx.size
      _ = validatedTx.zipWithIndex.foreach {
        case (ValidatedTopologyTransaction(tx, None, _), idx) =>
          val enqueuingOrStoring = if (outboxQueue.nonEmpty) "Enqueuing" else "Storing"
          logger.info(
            s"$enqueuingOrStoring topology transaction ${idx + 1}/$ln ${tx.operation} ${tx.mapping} with ts=$effective (epsilon=$epsilon ms)"
          )
        case (ValidatedTopologyTransaction(tx, Some(r), _), idx) =>
          // TODO(i19737): we need to emit a security alert, if the rejection is due to a malicious broadcast
          logger.info(
            s"Rejected transaction ${idx + 1}/$ln ${tx.operation} ${tx.mapping} at ts=$effective (epsilon=$epsilon ms) due to $r"
          )
      }
      asyncResult <- outboxQueue match {
        case Some(queue) =>
          // if we use the synchronizer outbox queue, we must also reset the caches, because the local validation
          // doesn't automatically imply successful validation once the transactions have been sequenced.
          clearCaches()
          EitherT
            .rightT[FutureUnlessShutdown, Lft](queue.enqueue(validatedTx.map(_.transaction)))
            .map { result =>
              logger.info("Enqueued topology transactions:\n" + validatedTx.mkString(",\n"))
              result
            }

        case None =>
          EitherT
            .right[Lft](
              store.update(
                sequenced,
                effective,
                mappingRemoves,
                txRemoves,
                validatedTx,
              )
            )
            .map { _ =>
              logger.info(
                s"Persisted topology transactions ($sequenced, $effective):\n" + validatedTx
                  .mkString(
                    ",\n"
                  )
              )
              AsyncResult.immediate
            }
      }
    } yield (validatedTx, asyncResult)
    // EitherT only served to shortcircuit in case abortOnError==true.
    // Therefore we merge the left (failed validations) with the right (successful or failed validations, in case !abortOnError.
    // The caller of this method must anyway deal with rejections.
    ret
      .leftMap(_ -> AsyncResult.immediate)
      .merge
  }

  private def clearCaches(): Unit = {
    txForMapping.clear()
    proposalsForTx.clear()
    proposalsByMapping.clear()
    authValidator.reset()
  }

  private def preloadTxsForMapping(
      effective: EffectiveTime,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val hashes = NonEmpty.from(
      transactions
        .map(x => x.mapping.uniqueKey)
        .filterNot(txForMapping.contains)
        .toSet
    )
    hashes.fold(FutureUnlessShutdown.unit) {
      store
        .findTransactionsForMapping(effective, _)
        .map(_.foreach { item =>
          txForMapping.put(item.mapping.uniqueKey, MaybePending(item)).discard
        })
    }
  }

  private def trackProposal(txHash: TxHash, mappingHash: MappingHash): Unit =
    proposalsByMapping
      .updateWith(mappingHash) {
        case None => Some(Seq(txHash))
        case Some(seq) => Some(seq :+ txHash)
      }
      .discard

  private def preloadProposalsForTx(
      effective: EffectiveTime,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val hashes =
      NonEmpty.from(
        transactions
          .map(x => x.hash)
          .filterNot(proposalsForTx.contains)
          .toSet
      )

    hashes.fold(FutureUnlessShutdown.unit) {
      store
        .findProposalsByTxHash(effective, _)
        .map(_.foreach { item =>
          val txHash = item.hash
          // store the proposal
          proposalsForTx.put(txHash, MaybePending(item)).discard
          // maintain a map from mapping to txs
          trackProposal(txHash, item.mapping.uniqueKey)
        })
    }
  }

  private def serialIsMonotonicallyIncreasing(
      inStore: Option[GenericSignedTopologyTransaction],
      toValidate: GenericSignedTopologyTransaction,
  ): Either[TopologyTransactionRejection, Unit] = inStore match {
    case Some(value) =>
      val expected = value.serial.increment
      Either.cond(
        expected == toValidate.serial,
        (),
        TopologyTransactionRejection.SerialMismatch(expected, toValidate.serial),
      )
    case None => Either.unit
  }

  private def transactionIsAuthorized(
      effective: EffectiveTime,
      inStore: Option[GenericSignedTopologyTransaction],
      toValidate: GenericSignedTopologyTransaction,
      expectFullAuthorization: Boolean,
      transactionMayHaveMissingSigningKeySignatures: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, GenericSignedTopologyTransaction] =
    EitherT
      .right(
        authValidator
          .validateAndUpdateHeadAuthState(
            effective.value,
            toValidate,
            inStore,
            expectFullAuthorization = expectFullAuthorization,
            transactionMayHaveMissingSigningKeySignatures =
              transactionMayHaveMissingSigningKeySignatures,
          )
      )
      .subflatMap { tx =>
        tx.rejectionReason.toLeft(tx.transaction)
      }

  private def mergeSignatures(
      inStore: Option[GenericSignedTopologyTransaction],
      toValidate: GenericSignedTopologyTransaction,
  ): (Boolean, GenericSignedTopologyTransaction) =
    inStore match {
      case Some(value) if value.hash == toValidate.hash =>
        (true, value.addSignaturesFromTransaction(toValidate))

      case _ => (false, toValidate)
    }

  private def mergeWithPendingProposal(
      toValidate: GenericSignedTopologyTransaction
  ): GenericSignedTopologyTransaction =
    proposalsForTx.get(toValidate.hash) match {
      case None => toValidate
      case Some(existingProposal) =>
        toValidate.addSignaturesFromTransaction(existingProposal.validatedTx.transaction)
    }

  private def validateAndMerge(
      effective: EffectiveTime,
      txA: GenericSignedTopologyTransaction,
      expectFullAuthorization: Boolean,
      transactionMayHaveMissingSigningKeySignatures: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericValidatedTopologyTransaction] = {
    // get current valid transaction for the given mapping
    val tx_inStore = txForMapping.get(txA.mapping.uniqueKey).map(_.currentTx)
    // first, merge a pending proposal with this transaction. we do this as it might
    // subsequently activate the given transaction
    val tx_mergedProposalSignatures = mergeWithPendingProposal(txA)
    val (isMerge, tx_deduplicatedAndMerged) =
      mergeSignatures(tx_inStore, tx_mergedProposalSignatures)
    val ret = for {
      // Run mapping specific semantic checks
      _ <- topologyMappingChecks.checkTransaction(
        effective,
        tx_deduplicatedAndMerged,
        tx_inStore,
        txForMapping.view.mapValues { pending =>
          require(
            !pending.expireImmediately.get() && pending.rejection.get.isEmpty,
            s"unexpectedly used rejected or immediately expired tx: $pending",
          )
          pending.currentTx
        }.toMap,
      )
      _ <-
        // we potentially merge the transaction with the currently active if this is just a signature update
        // now, check if the serial is monotonically increasing
        if (isMerge) {
          EitherTUtil.unitUS[TopologyTransactionRejection]
        } else {
          EitherT.fromEither[FutureUnlessShutdown](
            serialIsMonotonicallyIncreasing(tx_inStore, tx_deduplicatedAndMerged).void
          )
        }
      // we check if the transaction is properly authorized given the current topology state
      // if it is a proposal, then we demand that all signatures are appropriate (but
      // not necessarily sufficient)
      // !THIS CHECK NEEDS TO BE THE LAST CHECK! because the transaction authorization validator
      // will update its internal cache. If a transaction then gets rejected afterwards, the cache
      // is corrupted.
      fullyValidated <- transactionIsAuthorized(
        effective,
        tx_inStore,
        tx_deduplicatedAndMerged,
        expectFullAuthorization = expectFullAuthorization,
        transactionMayHaveMissingSigningKeySignatures =
          transactionMayHaveMissingSigningKeySignatures,
      )
    } yield fullyValidated
    ret.fold(
      rejection => ValidatedTopologyTransaction(txA, Some(rejection)),
      tx => ValidatedTopologyTransaction(tx, None),
    )
  }

  private def determineRemovesAndUpdatePending(
      tx: MaybePending,
      removeMappings: Map[MappingHash, PositiveInt],
      removeTxs: Set[TxHash],
  )(implicit traceContext: TraceContext): (Map[MappingHash, PositiveInt], Set[TxHash]) = {
    val finalTx = tx.currentTx
    // UPDATE tx SET valid_until = effective WHERE storeId = XYZ
    //    AND valid_until is NULL and valid_from < effective

    if (tx.rejection.get().nonEmpty) {
      // if the transaction has been rejected, we don't actually expire any proposals or currently valid transactions
      (removeMappings, removeTxs)
    } else if (finalTx.isProposal) {
      // if this is a proposal, we only delete the "previously existing proposal"
      // AND ((tx_hash = ..))
      val txHash = finalTx.hash
      proposalsForTx.put(txHash, tx).foreach { existingProposal =>
        // update currently pending (this is relevant in case we have proposals for the
        // same txs within a batch)
        existingProposal.expireImmediately.set(true)
        ErrorUtil.requireState(
          existingProposal.rejection.get().isEmpty,
          s"Error state should be empty for $existingProposal",
        )
      }
      trackProposal(txHash, finalTx.mapping.uniqueKey)
      (removeMappings, removeTxs + txHash)
    } else {
      // if this is a sufficiently signed and valid transaction, we delete all existing proposals and the previous tx
      //  we can just use a mapping key: there can not be a future proposal, as it would violate the
      //  monotonically increasing check
      // AND ((mapping_key = ...) )
      val mappingHash = finalTx.mapping.uniqueKey
      txForMapping.put(mappingHash, tx).foreach { existingMapping =>
        // replace previous tx in case we have concurrent updates within the same timestamp
        existingMapping.expireImmediately.set(true)
        ErrorUtil.requireState(
          existingMapping.rejection.get().isEmpty,
          s"Error state should be empty for $existingMapping",
        )
      }
      // remove all pending proposals for this mapping
      proposalsByMapping
        .remove(mappingHash)
        .foreach(
          _.foreach(proposal =>
            proposalsForTx.remove(proposal).foreach { existing =>
              ErrorUtil.requireState(
                existing.expireImmediately.compareAndSet(false, true),
                s"ExpireImmediately should be false for $existing",
              )
            }
          )
        )
      (
        removeMappings.updatedWith(mappingHash)(
          Ordering[Option[PositiveInt]].max(_, Some(finalTx.serial))
        ),
        removeTxs,
      )
    }
  }
}

object TopologyStateProcessor {

  /** Creates a TopologyStateProcessor for topology managers.
    */
  def forTopologyManager[PureCrypto <: CryptoPureApi](
      store: TopologyStore[TopologyStoreId],
      outboxQueue: Option[SynchronizerOutboxQueue],
      topologyMappingChecks: TopologyMappingChecks,
      pureCrypto: PureCrypto,
      loggerFactoryParent: NamedLoggerFactory,
  )(implicit ec: ExecutionContext) =
    new TopologyStateProcessor(
      store,
      outboxQueue,
      topologyMappingChecks,
      pureCrypto,
      loggerFactoryParent,
    )

  /** Creates a TopologyStateProcessor for the purpose of initial snapshot validation.
    */
  def forInitialSnapshotValidation[PureCrypto <: CryptoPureApi](
      store: TopologyStore[TopologyStoreId],
      topologyMappingChecks: TopologyMappingChecks,
      pureCrypto: PureCrypto,
      loggerFactoryParent: NamedLoggerFactory,
  )(implicit ec: ExecutionContext) =
    new TopologyStateProcessor(
      store,
      outboxQueue = None,
      topologyMappingChecks,
      pureCrypto,
      loggerFactoryParent,
    )

  /** Creates a TopologyStateProcessor for the purpose of business-as-usual topology transaction
    * processing.
    */
  def forTransactionProcessing[PureCrypto <: CryptoPureApi](
      store: TopologyStore[TopologyStoreId],
      topologyMappingChecks: TopologyMappingChecks,
      pureCrypto: PureCrypto,
      loggerFactoryParent: NamedLoggerFactory,
  )(implicit ec: ExecutionContext) =
    new TopologyStateProcessor(
      store,
      outboxQueue = None,
      topologyMappingChecks,
      pureCrypto,
      loggerFactoryParent,
    )
}
