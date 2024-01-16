// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.instances.seq.*
import cats.syntax.foldable.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  IncomingTopologyTransactionAuthorizationValidatorX,
  SequencedTime,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.{
  TopologyStoreId,
  TopologyStoreX,
  TopologyTransactionRejection,
  ValidatedTopologyTransactionX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyMappingXChecks
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** @param outboxQueue If a [[DomainOutboxQueue]] is provided, the processed transactions are not directly stored,
  *                    but rather sent to the domain via an ephemeral queue (i.e. no persistence).
  * @param enableTopologyTransactionValidation If disabled, all of the authorization validation logic in
  *                                            IncomingTopologyTransactionAuthorizationValidatorX is skipped.
  */
class TopologyStateProcessorX(
    val store: TopologyStoreX[TopologyStoreId],
    outboxQueue: Option[DomainOutboxQueue],
    enableTopologyTransactionValidation: Boolean,
    topologyMappingXChecks: TopologyMappingXChecks,
    crypto: Crypto,
    loggerFactoryParent: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  override protected val loggerFactory: NamedLoggerFactory =
    loggerFactoryParent.append("store", store.storeId.toString)

  // small container to store potentially pending data
  private case class MaybePending(originalTx: GenericSignedTopologyTransactionX) {
    val adjusted = new AtomicReference[Option[GenericSignedTopologyTransactionX]](None)
    val rejection = new AtomicReference[Option[TopologyTransactionRejection]](None)
    val expireImmediately = new AtomicBoolean(false)

    def currentTx: GenericSignedTopologyTransactionX = adjusted.get().getOrElse(originalTx)

    def validatedTx: GenericValidatedTopologyTransactionX =
      ValidatedTopologyTransactionX(currentTx, rejection.get(), expireImmediately.get())
  }

  // TODO(#14063) use cache instead and remember empty
  private val txForMapping = TrieMap[MappingHash, MaybePending]()
  private val proposalsByMapping = TrieMap[MappingHash, Seq[TxHash]]()
  private val proposalsForTx = TrieMap[TxHash, MaybePending]()

  private val authValidator =
    new IncomingTopologyTransactionAuthorizationValidatorX(
      crypto.pureCrypto,
      store,
      None,
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
      transactions: Seq[GenericSignedTopologyTransactionX],
      // TODO(#12390) propagate and abort unless we use force
      abortIfCascading: Boolean,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, Seq[GenericValidatedTopologyTransactionX], Seq[
    GenericValidatedTopologyTransactionX
  ]] = {
    // if transactions aren't persisted in the store but rather enqueued in the domain outbox queue,
    // the processing should abort on errors, because we don't want to enqueue rejected transactions.
    val abortOnError = outboxQueue.nonEmpty

    type Lft = Seq[GenericValidatedTopologyTransactionX]

    // first, pre-load the currently existing mappings and proposals for the given transactions
    val preloadTxsForMappingF = preloadTxsForMapping(EffectiveTime.MaxValue, transactions)
    val preloadProposalsForTxF = preloadProposalsForTx(EffectiveTime.MaxValue, transactions)
    // TODO(#14064) preload authorization data
    val ret = for {
      _ <- EitherT.right[Lft](preloadProposalsForTxF)
      _ <- EitherT.right[Lft](preloadTxsForMappingF)
      // compute / collapse updates
      (removesF, pendingWrites) = {
        val pendingWrites = transactions.map(MaybePending)
        val removes = pendingWrites
          .foldLeftM((Set.empty[MappingHash], Set.empty[TxHash])) {
            case ((removeMappings, removeTxs), tx) =>
              validateAndMerge(
                effective,
                tx.originalTx,
                expectFullAuthorization || !tx.originalTx.isProposal,
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
      _ <- EitherT.cond[Future](
        // TODO(#12390) differentiate error reason and only abort actual errors, not in-batch merges
        !abortOnError || validatedTx.forall(_.nonDuplicateRejectionReason.isEmpty),
        (), {
          // reset caches as they are broken now if we abort
          clearCaches()
          validatedTx
        }: Lft,
      ): EitherT[Future, Lft, Unit]

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
      transactions: Seq[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val hashes = NonEmpty.from(
      transactions
        .map(x => x.transaction.mapping.uniqueKey)
        .filterNot(txForMapping.contains)
        .toSet
    )
    hashes.fold(Future.unit) {
      store
        .findTransactionsForMapping(effective, _)
        .map(_.foreach { item =>
          txForMapping.put(item.transaction.mapping.uniqueKey, MaybePending(item)).discard
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
      transactions: Seq[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val hashes =
      NonEmpty.from(
        transactions
          .map(x => x.transaction.hash)
          .filterNot(proposalsForTx.contains)
          .toSet
      )

    hashes.fold(Future.unit) {
      store
        .findProposalsByTxHash(effective, _)
        .map(_.foreach { item =>
          val txHash = item.transaction.hash
          // store the proposal
          proposalsForTx.put(txHash, MaybePending(item)).discard
          // maintain a map from mapping to txs
          trackProposal(txHash, item.transaction.mapping.uniqueKey)
        })
    }
  }

  private def serialIsMonotonicallyIncreasing(
      inStore: Option[GenericSignedTopologyTransactionX],
      toValidate: GenericSignedTopologyTransactionX,
  ): Either[TopologyTransactionRejection, Unit] = inStore match {
    case Some(value) =>
      val expected = value.transaction.serial.increment
      Either.cond(
        expected == toValidate.transaction.serial,
        (),
        TopologyTransactionRejection.SerialMismatch(expected, toValidate.transaction.serial),
      )
    case None => Right(())
  }

  private def transactionIsAuthorized(
      effective: EffectiveTime,
      inStore: Option[GenericSignedTopologyTransactionX],
      toValidate: GenericSignedTopologyTransactionX,
      expectFullAuthorization: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyTransactionRejection, GenericSignedTopologyTransactionX] = {
    if (enableTopologyTransactionValidation) {
      EitherT
        .right(
          authValidator
            .validateAndUpdateHeadAuthState(
              effective.value,
              Seq(toValidate),
              inStore.map(tx => tx.transaction.mapping.uniqueKey -> tx).toList.toMap,
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

  private def deduplicateAndMergeSignatures(
      inStore: Option[GenericSignedTopologyTransactionX],
      toValidate: GenericSignedTopologyTransactionX,
  ): Either[TopologyTransactionRejection, (Boolean, GenericSignedTopologyTransactionX)] =
    inStore match {
      case Some(value) if value.transaction.hash == toValidate.transaction.hash =>
        if (toValidate.signatures.diff(value.signatures).isEmpty) {
          // the new transaction doesn't provide any new signatures => Duplicate
          // TODO(#12390) use proper timestamp or remove the timestamp from the error?
          Left(TopologyTransactionRejection.Duplicate(CantonTimestamp.MinValue))
        } else Right((true, value.addSignatures(toValidate.signatures.toSeq)))

      case _ => Right((false, toValidate))
    }

  private def mergeWithPendingProposal(
      toValidate: GenericSignedTopologyTransactionX
  ): GenericSignedTopologyTransactionX = {
    proposalsForTx.get(toValidate.transaction.hash) match {
      case None => toValidate
      case Some(existingProposal) =>
        toValidate.addSignatures(existingProposal.validatedTx.transaction.signatures.toSeq)
    }
  }

  private def validateAndMerge(
      effective: EffectiveTime,
      txA: GenericSignedTopologyTransactionX,
      expectFullAuthorization: Boolean,
  )(implicit traceContext: TraceContext): Future[GenericValidatedTopologyTransactionX] = {
    // get current valid transaction for the given mapping
    val tx_inStore = txForMapping.get(txA.transaction.mapping.uniqueKey).map(_.currentTx)
    // first, merge a pending proposal with this transaction. we do this as it might
    // subsequently activate the given transaction
    val tx_mergedProposalSignatures = mergeWithPendingProposal(txA)
    val ret = for {
      mergeResult <- EitherT.fromEither[Future](
        deduplicateAndMergeSignatures(tx_inStore, tx_mergedProposalSignatures)
      )
      (isMerge, tx_deduplicatedAndMerged) = mergeResult
      // we check if the transaction is properly authorized given the current topology state
      // if it is a proposal, then we demand that all signatures are appropriate (but
      // not necessarily sufficient)
      tx_authorized <- transactionIsAuthorized(
        effective,
        tx_inStore,
        tx_deduplicatedAndMerged,
        expectFullAuthorization,
      )

      // Run mapping specific semantic checks
      _ <- topologyMappingXChecks.checkTransaction(effective, tx_authorized, tx_inStore)

      // we potentially merge the transaction with the currently active if this is just a signature update
      // now, check if the serial is monotonically increasing
      fullyValidated <-
        if (isMerge)
          EitherT.rightT[Future, TopologyTransactionRejection](tx_authorized)
        else {
          EitherT.fromEither[Future](
            serialIsMonotonicallyIncreasing(tx_inStore, tx_authorized).map(_ => tx_authorized)
          )
        }
    } yield fullyValidated
    ret.fold(
      // TODO(#12390) emit appropriate log message and use correct rejection reason
      rejection => ValidatedTopologyTransactionX(txA, Some(rejection)),
      tx => ValidatedTopologyTransactionX(tx, None),
    )
  }

  private def determineRemovesAndUpdatePending(
      tx: MaybePending,
      removeMappings: Set[MappingHash],
      removeTxs: Set[TxHash],
  )(implicit traceContext: TraceContext): (Set[MappingHash], Set[TxHash]) = {
    val finalTx = tx.currentTx
    // UPDATE tx SET valid_until = effective WHERE storeId = XYZ
    //    AND valid_until is NULL and valid_from < effective

    if (tx.rejection.get().nonEmpty) {
      // if the transaction has been rejected, we don't actually expire any proposals or currently valid transactions
      (removeMappings, removeTxs)
    } else if (finalTx.isProposal) {
      // if this is a proposal, we only delete the "previously existing proposal"
      // AND ((tx_hash = ..))
      val txHash = finalTx.transaction.hash
      proposalsForTx.put(txHash, tx).foreach { existingProposal =>
        // update currently pending (this is relevant in case we have proposals for the
        // same txs within a batch)
        existingProposal.expireImmediately.set(true)
        ErrorUtil.requireState(
          existingProposal.rejection.get().isEmpty,
          s"Error state should be empty for ${existingProposal}",
        )
      }
      trackProposal(txHash, finalTx.transaction.mapping.uniqueKey)
      (removeMappings, removeTxs + txHash)
    } else {
      // if this is a sufficiently signed and valid transaction, we delete all existing proposals and the previous tx
      //  we can just use a mapping key: there can not be a future proposal, as it would violate the
      //  monotonically increasing check
      // AND ((mapping_key = ...) )
      val mappingHash = finalTx.transaction.mapping.uniqueKey
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
      (removeMappings + mappingHash, removeTxs)
    }
  }
}
