// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout, TopologyConfig}
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.AsyncResult
import com.digitalasset.canton.topology.cache.{TopologyStateLookup, TopologyStateWriteThroughCache}
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionAuthorizationValidator,
}
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.ValidatedTopologyTransaction.GenericValidatedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.checks.TopologyMappingChecks
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

/** A non-thread safe class which validates and stores topology transactions
  *
  * @param outboxQueue
  *   If a [[SynchronizerOutboxQueue]] is provided, the processed transactions are not directly
  *   stored, but rather sent to the synchronizer via an ephemeral queue (i.e. no persistence).
  */
class TopologyStateProcessor private (
    val store: TopologyStore[TopologyStoreId],
    cache: TopologyStateWriteThroughCache,
    outboxQueue: Option[SynchronizerOutboxQueue],
    topologyMappingChecksFactory: TopologyStateLookup => TopologyMappingChecks,
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

  private val topologyMappingChecks = topologyMappingChecksFactory(cache)

  private val authValidator =
    new TopologyTransactionAuthorizationValidator(
      pureCrypto,
      cache,
      // if transactions are put directly into a store (ie there is no outbox queue)
      // then the authorization validation is final.
      validationIsFinal = outboxQueue.isEmpty,
      loggerFactory.append("role", if (outboxQueue.isEmpty) "incoming" else "outgoing"),
    )

  /** validate the authorization and the signatures of the given transactions
    *
    * The function is NOT THREAD SAFE AND MUST RUN SEQUENTIALLY
    * @param relaxChecksForBackwardsCompatibility
    *   in order to import mainnet topology state after hard migration, we need to relax certain
    *   checks which we added subsequently but which are not honored with older transactions.
    *   exceptions are:
    *   - no proof-of-ownership for signing keys on older OTKs
    *   - adding members before adding OTKs
    * @param storeIsEmpty
    *   if set to true, the store is considered empty. no check will load from the store. this is
    *   useful for importing large genesis timestamps. for all other scenarios it will blow up.
    */
  def validateAndApplyAuthorization(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      transactions: Seq[GenericSignedTopologyTransaction],
      expectFullAuthorization: Boolean,
      relaxChecksForBackwardsCompatibility: Boolean,
      storeIsEmpty: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[(Seq[GenericValidatedTopologyTransaction], AsyncResult[Unit])] = {
    // if transactions aren't persisted in the store but rather enqueued in the synchronizer outbox queue,
    // the processing should abort on errors, because we don't want to enqueue rejected transactions.
    val abortOnError = outboxQueue.nonEmpty

    type Lft = Seq[GenericValidatedTopologyTransaction]

    val ret = for {
      // first, preload the currently existing state for the given transactions
      _ <- EitherT.right[Lft](preloadCaches(effective, transactions, storeIsEmpty))
      // iterate over all transactions, validate and merge them
      validatedTx <- EitherT.right(MonadUtil.sequentialTraverse(transactions) { tx =>
        for {
          finalTx <-
            validateAndMerge(
              effective,
              tx,
              expectFullAuthorization = expectFullAuthorization || !tx.isProposal,
              relaxChecksForBackwardsCompatibility = relaxChecksForBackwardsCompatibility,
              storeIsEmpty,
            )
          _ <- cache.append(sequenced, effective, finalTx)
        } yield finalTx
      })
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
      ln = validatedTx.size
      _ = validatedTx.zipWithIndex.foreach {
        case (ValidatedTopologyTransaction(tx, None, _), idx) =>
          val enqueuingOrStoring = if (outboxQueue.nonEmpty) "Enqueuing" else "Storing"
          logger.info(
            s"$enqueuingOrStoring topology transaction ${idx + 1}/$ln ${tx.hash} with ts=$effective, signedBy=${tx.signatures
                .map(_.authorizingLongTermKey)}"
          )
        case (ValidatedTopologyTransaction(tx, Some(r), _), idx) =>
          // TODO(i19737): we need to emit a security alert, if the rejection is due to a malicious broadcast
          logger.info(
            s"Rejected transaction ${idx + 1}/$ln ${tx.hash} at ts=$effective, signedBy=${tx.signatures
                .map(_.authorizingLongTermKey)} due to $r"
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
            // TODO(#29400) we could move the flush into the async result immediate to make processing even faster
            .right[Lft](cache.flush(sequenced, effective))
            .map { _ =>
              logger.info(
                s"Persisted topology transactions ($sequenced, $effective):\n" + validatedTx
                  .take(100)
                  .mkString(",\n")
              )
              // Reduces caches if they become too large
              cache.evict().discard
              authValidator.evict()
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
    cache.dropEverything()
    authValidator.reset()
  }

  private def preloadCaches(
      effective: EffectiveTime,
      transactions: Seq[GenericSignedTopologyTransaction],
      storeIsEmpty: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val (uids, nss) = transactions.foldLeft((Set.empty[UniqueIdentifier], Set.empty[Namespace])) {
      case ((uids, nss), item) =>
        val auth = item.mapping.requiredAuth(None).referenced
        // collect all uids / namespaces we need to fetch to have them active in the cache
        // we include the mapping namespace for decentralized namespaces (their namespace is not included in the
        // required auth ... )
        (uids ++ item.mapping.referencedUids, nss ++ auth.namespaces + item.mapping.namespace)
    }
    cache.fetch(effective, uids, nss, storeIsEmpty)
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
        TopologyTransactionRejection.Processor
          .SerialMismatch(actual = toValidate.serial, expected = expected),
      )
    case None =>
      val sloppyEnforcement =
        store.storeId.forSynchronizer.exists(_.protocolVersion < ProtocolVersion.v35)
      if (sloppyEnforcement)
        Either.unit
      else {
        // starting with pv=35, we will require that newly added proposals start with serial 1
        // topology transactions might start at a later point
        Either.cond(
          !toValidate.isProposal || toValidate.serial == PositiveInt.one,
          (),
          TopologyTransactionRejection.Processor
            .SerialMismatch(actual = toValidate.serial, expected = PositiveInt.one),
        )
      }
  }

  private def transactionIsAuthorized(
      effective: EffectiveTime,
      inStore: Option[GenericSignedTopologyTransaction],
      toValidate: GenericSignedTopologyTransaction,
      expectFullAuthorization: Boolean,
      relaxChecksForBackwardsCompatibility: Boolean,
      storeIsEmpty: Boolean,
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
            relaxChecksForBackwardsCompatibility = relaxChecksForBackwardsCompatibility,
            storeIsEmpty = storeIsEmpty,
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
        (true, value.addSignatures(toValidate.signatures))
      case _ => (false, toValidate)
    }

  private def mergeWithPendingProposal(
      toValidate: GenericSignedTopologyTransaction
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[GenericSignedTopologyTransaction] =
    cache
      .lookupPendingProposal(toValidate.transaction)
      .map {
        case None => toValidate
        case Some(existingProposal) =>
          toValidate.addSignatures(existingProposal.transaction.signatures)
      }

  private def validateAndMerge(
      effective: EffectiveTime,
      txA: GenericSignedTopologyTransaction,
      expectFullAuthorization: Boolean,
      relaxChecksForBackwardsCompatibility: Boolean,
      storeIsEmpty: Boolean,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GenericValidatedTopologyTransaction] = {

    val ret = for {
      tx_inStore <- EitherT.right(
        cache.lookupActiveForMapping(txA.mapping).map(_.map(_.transaction))
      )
      // first, merge a pending proposal with this transaction. we do this as it might
      // subsequently activate the given transaction
      tx_mergedProposalSignatures <- EitherT.right(mergeWithPendingProposal(txA))
      (isMerge, tx_deduplicatedAndMerged) = mergeSignatures(tx_inStore, tx_mergedProposalSignatures)
      // Run mapping specific semantic checks
      _ <- topologyMappingChecks.checkTransaction(
        effective,
        tx_deduplicatedAndMerged,
        tx_inStore,
        relaxChecksForBackwardsCompatibility,
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
        relaxChecksForBackwardsCompatibility = relaxChecksForBackwardsCompatibility,
        storeIsEmpty = storeIsEmpty,
      )
    } yield fullyValidated
    ret.fold(
      rejection => ValidatedTopologyTransaction(txA, Some(rejection)),
      tx => ValidatedTopologyTransaction(tx, None),
    )
  }

}

object TopologyStateProcessor {

  /** Creates a TopologyStateProcessor for topology managers.
    */
  def forTopologyManager[PureCrypto <: CryptoPureApi](
      store: TopologyStore[TopologyStoreId],
      topologyCacheAggregatorConfig: BatchAggregatorConfig,
      topologyConfig: TopologyConfig,
      outboxQueue: Option[SynchronizerOutboxQueue],
      topologyMappingChecksFactory: TopologyStateLookup => TopologyMappingChecks,
      pureCrypto: PureCrypto,
      timeouts: ProcessingTimeout,
      loggerFactoryParent: NamedLoggerFactory,
  )(implicit ec: ExecutionContext) =
    new TopologyStateProcessor(
      store,
      new TopologyStateWriteThroughCache(
        store,
        topologyCacheAggregatorConfig,
        maxCacheSize = topologyConfig.maxTopologyStateCacheItems,
        enableConsistencyChecks = topologyConfig.enableTopologyStateCacheConsistencyChecks,
        timeouts,
        loggerFactoryParent.append("purpose", store.storeId.toString),
      ),
      outboxQueue,
      topologyMappingChecksFactory,
      pureCrypto,
      loggerFactoryParent,
    )

  /** Creates a TopologyStateProcessor for the purpose of initial snapshot validation.
    */
  def forInitialSnapshotValidation[PureCrypto <: CryptoPureApi](
      store: TopologyStore[TopologyStoreId],
      topologyCacheAggregatorConfig: BatchAggregatorConfig,
      topologyConfig: TopologyConfig,
      topologyMappingChecksFactory: TopologyStateLookup => TopologyMappingChecks,
      pureCrypto: PureCrypto,
      timeouts: ProcessingTimeout,
      loggerFactoryParent: NamedLoggerFactory,
  )(implicit ec: ExecutionContext) =
    new TopologyStateProcessor(
      store,
      new TopologyStateWriteThroughCache(
        store,
        topologyCacheAggregatorConfig,
        maxCacheSize = topologyConfig.maxTopologyStateCacheItems,
        enableConsistencyChecks = topologyConfig.enableTopologyStateCacheConsistencyChecks,
        timeouts,
        loggerFactoryParent.append("purpose", "initial-validation"),
      ),
      outboxQueue = None,
      topologyMappingChecksFactory,
      pureCrypto,
      loggerFactoryParent,
    )

  /** Creates a TopologyStateProcessor for the purpose of business-as-usual topology transaction
    * processing.
    */
  def forTransactionProcessing[PureCrypto <: CryptoPureApi](
      store: TopologyStore[TopologyStoreId],
      cache: TopologyStateWriteThroughCache,
      topologyMappingChecksFactory: TopologyStateLookup => TopologyMappingChecks,
      pureCrypto: PureCrypto,
      loggerFactoryParent: NamedLoggerFactory,
  )(implicit ec: ExecutionContext) =
    new TopologyStateProcessor(
      store,
      cache,
      outboxQueue = None,
      topologyMappingChecksFactory,
      pureCrypto,
      loggerFactoryParent,
    )
}
