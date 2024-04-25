// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.instances.seq.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  IncomingTopologyTransactionAuthorizationValidator,
  SequencedTime,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.store.{
  SignedTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
  TopologyTransactionRejection,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
  TopologyMappingChecks,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** @param outboxQueue If a [[DomainOutboxQueue]] is provided, the processed transactions are not directly stored,
  *                    but rather sent to the domain via an ephemeral queue (i.e. no persistence).
  * @param enableTopologyTransactionValidation If disabled, all of the authorization validation logic in
  *                                            IncomingTopologyTransactionAuthorizationValidator is skipped.
  */
class TopologyStateProcessor(
    val store: TopologyStore[TopologyStoreId],
    outboxQueue: Option[DomainOutboxQueue],
    enableTopologyTransactionValidation: Boolean,
    topologyMappingChecks: TopologyMappingChecks,
    pureCrypto: CryptoPureApi,
    loggerFactoryParent: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  override protected val loggerFactory: NamedLoggerFactory =
    loggerFactoryParent.append("store", store.storeId.toString)

  // small container to store potentially pending data
  private case class MaybePending(originalTx: GenericSignedTopologyTransaction) {
    val adjusted = new AtomicReference[Option[GenericSignedTopologyTransaction]](None)
    val rejection = new AtomicReference[Option[TopologyTransactionRejection]](None)
    val expireImmediately = new AtomicBoolean(false)

    def currentTx: GenericSignedTopologyTransaction = adjusted.get().getOrElse(originalTx)

    def validatedTx: GenericValidatedTopologyTransaction =
      ValidatedTopologyTransaction(currentTx, rejection.get(), expireImmediately.get())
  }

  // TODO(#14063) use cache instead and remember empty
  private val txForMapping = TrieMap[MappingHash, MaybePending]()
  private val proposalsByMapping = TrieMap[MappingHash, Seq[TxHash]]()
  private val proposalsForTx = TrieMap[TxHash, MaybePending]()

  private val authValidator =
    new IncomingTopologyTransactionAuthorizationValidator(
      pureCrypto,
      store,
      None,
      // if transactions are put directly into a store (ie there is no outbox queue)
      // then the authorization validation is final.
      validationIsFinal = outboxQueue.isEmpty,
      loggerFactory.append("role", "incoming"),
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
      // TODO(#12390) propagate and abort unless we use force
      abortIfCascading: Boolean,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, Seq[GenericValidatedTopologyTransaction], Seq[
    GenericValidatedTopologyTransaction
  ]] = {
    // if transactions aren't persisted in the store but rather enqueued in the domain outbox queue,
    // the processing should abort on errors, because we don't want to enqueue rejected transactions.
    val abortOnError = outboxQueue.nonEmpty

    type Lft = Seq[GenericValidatedTopologyTransaction]

    val transactions = SignedTopologyTransactions.compact(transactionsToValidate)

    // first, pre-load the currently existing mappings and proposals for the given transactions
    val preloadTxsForMappingF = preloadTxsForMapping(effective, transactions)
    val preloadProposalsForTxF = preloadProposalsForTx(effective, transactions)
    val duplicatesF = findDuplicates(effective, transactions)
    // TODO(#14064) preload authorization data
    val ret = for {
      _ <- EitherT.right[Lft](preloadProposalsForTxF)
      _ <- EitherT.right[Lft](preloadTxsForMappingF)
      duplicates <- EitherT.right[Lft](duplicatesF)
      // compute / collapse updates
      (removesF, pendingWrites) = {
        val pendingWrites = transactions.map(MaybePending)
        val removes = pendingWrites
          .zip(duplicates)
          .foldLeftM((Map.empty[MappingHash, PositiveInt], Set.empty[TxHash])) {
            case ((removeMappings, removeTxs), (tx, _noDuplicateFound @ None)) =>
              validateAndMerge(
                effective,
                tx.originalTx,
                expectFullAuthorization || !tx.originalTx.isProposal,
              ).map { finalTx =>
                tx.adjusted.set(Some(finalTx.transaction))
                tx.rejection.set(finalTx.rejectionReason)
                determineRemovesAndUpdatePending(tx, removeMappings, removeTxs)
              }
            case ((removeMappings, removeTxs), (tx, Some(duplicateTimestamp))) =>
              tx.rejection.set(
                Some(TopologyTransactionRejection.Duplicate(duplicateTimestamp.value))
              )
              Future.successful(determineRemovesAndUpdatePending(tx, removeMappings, removeTxs))

          }
        (removes, pendingWrites)
      }
      removes <- EitherT.right[Lft](removesF)
      (mappingRemoves, txRemoves) = removes
      validatedTx = pendingWrites.map(pw => pw.validatedTx)
      _ <- EitherT.cond[Future](
        // TODO(#12390) differentiate error reason and only abort actual errors, not in-batch merges
        !abortOnError || validatedTx.forall(_.nonDuplicateRejectionReason.isEmpty),
        (), {
          // reset caches as they are broken now if we abort
          clearCaches()
          validatedTx
        }: Lft,
      ): EitherT[Future, Lft, Unit]
      // string approx for output
      epsilon =
        s"${effective.value.toEpochMilli - sequenced.value.toEpochMilli}"
      ln = validatedTx.size
      _ = validatedTx.zipWithIndex.foreach {
        case (ValidatedTopologyTransaction(tx, None, _), idx) =>
          logger.info(
            s"Storing topology transaction ${idx + 1}/$ln ${tx.operation} ${tx.mapping} with ts=$effective (epsilon=${epsilon} ms)"
          )
        case (ValidatedTopologyTransaction(tx, Some(r), _), idx) =>
          logger.info(
            s"Rejected transaction ${idx + 1}/$ln ${tx.operation} ${tx.mapping} at ts=$effective (epsilon=${epsilon} ms) due to $r"
          )
      }
      _ <- outboxQueue match {
        case Some(queue) =>
          // if we use the domain outbox queue, we must also reset the caches, because the local validation
          // doesn't automatically imply successful validation once the transactions have been sequenced.
          clearCaches()
          EitherT.rightT[Future, Lft](queue.enqueue(validatedTx.map(_.transaction)))

        case None =>
          EitherT.right[Lft](
            store.update(
              sequenced,
              effective,
              mappingRemoves,
              txRemoves,
              validatedTx,
            )
          )
      }
    } yield validatedTx
    ret.bimap(
      failed => {
        logger.info("Topology transactions failed:\n  " + failed.mkString("\n  "))
        failed
      },
      success => {
        if (outboxQueue.isEmpty) {
          logger.info(
            s"Persisted topology transactions ($sequenced, $effective):\n" + success
              .mkString(
                ",\n"
              )
          )
        } else logger.info("Enqueued topology transactions:\n" + success.mkString((",\n")))
        success
      },
    )
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
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val hashes = NonEmpty.from(
      transactions
        .map(x => x.mapping.uniqueKey)
        .filterNot(txForMapping.contains)
        .toSet
    )
    hashes.fold(Future.unit) {
      store
        .findTransactionsForMapping(effective, _)
        .map(_.foreach { item =>
          txForMapping.put(item.mapping.uniqueKey, MaybePending(item)).discard
        })
    }
  }

  private def trackProposal(txHash: TxHash, mappingHash: MappingHash): Unit = {
    proposalsByMapping
      .updateWith(mappingHash) {
        case None => Some(Seq(txHash))
        case Some(seq) => Some(seq :+ txHash)
      }
      .discard
  }

  private def preloadProposalsForTx(
      effective: EffectiveTime,
      transactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val hashes =
      NonEmpty.from(
        transactions
          .map(x => x.hash)
          .filterNot(proposalsForTx.contains)
          .toSet
      )

    hashes.fold(Future.unit) {
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
    case None => Right(())
  }

  private def transactionIsAuthorized(
      effective: EffectiveTime,
      inStore: Option[GenericSignedTopologyTransaction],
      toValidate: GenericSignedTopologyTransaction,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyTransactionRejection, GenericSignedTopologyTransaction] = {
    if (enableTopologyTransactionValidation) {
      EitherT
        .right(
          authValidator
            .validateAndUpdateHeadAuthState(
              effective.value,
              Seq(toValidate),
              inStore.map(tx => tx.mapping.uniqueKey -> tx).toList.toMap,
              expectFullAuthorization,
            )
        )
        .subflatMap { case (_, txs) =>
          // TODO(#12390) proper error
          txs.headOption
            .toRight[TopologyTransactionRejection](
              TopologyTransactionRejection.Other("expected validation result doesn't exist")
            )
            .flatMap(tx => tx.rejectionReason.toLeft(tx.transaction))
        }
    } else {
      EitherT.rightT(toValidate.copy(isProposal = false))
    }
  }

  private def mergeSignatures(
      inStore: Option[GenericSignedTopologyTransaction],
      toValidate: GenericSignedTopologyTransaction,
  ): (Boolean, GenericSignedTopologyTransaction) = {
    inStore match {
      case Some(value) if value.hash == toValidate.hash =>
        (true, value.addSignatures(toValidate.signatures.toSeq))

      case _ => (false, toValidate)
    }
  }

  /** determine whether one of the txs got already added earlier */
  private def findDuplicates(
      timestamp: EffectiveTime,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  )(implicit traceContext: TraceContext): Future[Seq[Option[EffectiveTime]]] = {
    Future.sequence(
      transactions.map { tx =>
        // skip duplication check for non-adds
        if (tx.operation == TopologyChangeOp.Remove)
          Future.successful(None)
        else {
          // check that the transaction has not been added before (but allow it if it has a different version ...)
          store
            .findStored(timestamp.value, tx)
            .map(
              _.filter(x =>
                // if the transaction to validate has the same proto version
                x.transaction.protoVersion == tx.protoVersion &&
                  // and the transaction to validate doesn't add new signatures
                  tx.signatures.diff(x.transaction.signatures).isEmpty
              ).map(_.validFrom)
            )
        }
      }
    )
  }

  private def mergeWithPendingProposal(
      toValidate: GenericSignedTopologyTransaction
  ): GenericSignedTopologyTransaction = {
    proposalsForTx.get(toValidate.hash) match {
      case None => toValidate
      case Some(existingProposal) =>
        toValidate.addSignatures(existingProposal.validatedTx.transaction.signatures.toSeq)
    }
  }

  private def validateAndMerge(
      effective: EffectiveTime,
      txA: GenericSignedTopologyTransaction,
      expectFullAuthorization: Boolean,
  )(implicit traceContext: TraceContext): Future[GenericValidatedTopologyTransaction] = {
    // get current valid transaction for the given mapping
    val tx_inStore = txForMapping.get(txA.mapping.uniqueKey).map(_.currentTx)
    // first, merge a pending proposal with this transaction. we do this as it might
    // subsequently activate the given transaction
    val tx_mergedProposalSignatures = mergeWithPendingProposal(txA)
    val (isMerge, tx_deduplicatedAndMerged) =
      mergeSignatures(tx_inStore, tx_mergedProposalSignatures)
    val ret = for {
      // Run mapping specific semantic checks
      _ <- topologyMappingChecks.checkTransaction(effective, tx_deduplicatedAndMerged, tx_inStore)
      _ <-
        // we potentially merge the transaction with the currently active if this is just a signature update
        // now, check if the serial is monotonically increasing
        if (isMerge) {
          EitherTUtil.unit[TopologyTransactionRejection]
        } else {
          EitherT.fromEither[Future](
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
        expectFullAuthorization,
      )
    } yield fullyValidated
    ret.fold(
      // TODO(#12390) emit appropriate log message and use correct rejection reason
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
          s"Error state should be empty for ${existingProposal}",
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
          s"Error state should be empty for ${existingMapping}",
        )
      }
      // remove all pending proposals for this mapping
      proposalsByMapping
        .remove(mappingHash)
        .foreach(
          _.foreach(proposal =>
            proposalsForTx.remove(proposal).foreach { existing =>
              val cur = existing.rejection.getAndSet(
                Some(TopologyTransactionRejection.Other("Outdated proposal within batch"))
              )
              ErrorUtil.requireState(cur.isEmpty, s"Error state should be empty for ${existing}")
            }
          )
        )
      // TODO(#12390) if this is a removal of a certificate, compute cascading deletes
      //   if boolean flag is set, then abort, otherwise notify
      //   rules: if a namespace delegation is a root delegation, it won't be affected by the
      //          cascading deletion of its authorizer. this will allow us to roll namespace certs
      //          also, root cert authorization is only valid if serial == 1
      (
        removeMappings.updatedWith(mappingHash)(
          Ordering[Option[PositiveInt]].max(_, Some(finalTx.serial))
        ),
        removeTxs,
      )
    }
  }
}
