// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.Monad
import cats.data.NonEmptyChain
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.conflictdetection.LockableStates.LockableStatesCheckHandle
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker.{
  ContractKeyJournalError,
  InvalidCommitSet,
  RequestTrackerStoreError,
  TransferStoreError,
}
import com.digitalasset.canton.participant.store.ActiveContractStore.*
import com.digitalasset.canton.participant.store.ContractKeyJournal.ContractKeyState
import com.digitalasset.canton.participant.store.TransferStore.{
  TransferCompleted,
  UnknownTransferId,
}
import com.digitalasset.canton.participant.store.memory.TransferCache
import com.digitalasset.canton.participant.store.{ActiveContractStore, ContractKeyJournal}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{LfContractId, LfGlobalKey, TransferId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{CheckedT, ErrorUtil, MonadUtil, SimpleExecutionQueue}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{RequestCounter, TransferCounter}
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** The [[ConflictDetector]] stores the state of contracts activated or deactivated by in-flight requests in memory,
  * and similarly for contract keys.
  * Such states take precedence over the states stored in the ACS or the contract key journal, which are only updated when a request is finalized.
  * A request is in flight from the call to [[ConflictDetector.registerActivenessSet]] until the corresponding call to
  * [[ConflictDetector.finalizeRequest]] has written the updates to the ACS.
  *
  * The [[ConflictDetector]] also checks that transfer-ins refer to active transfers and atomically completes them
  * during finalization.
  *
  * @param checkedInvariant Defines whether all methods should check the class invariant when they are called.
  *                         Invariant checking is slow.
  *                         It should only be enabled for testing and debugging.
  */
// We do not make the execution context implicit
// so that we are always aware of when a context switch may happen.
private[participant] class ConflictDetector(
    private val acs: ActiveContractStore,
    private val contractKeyJournal: ContractKeyJournal,
    private val transferCache: TransferCache,
    protected override val loggerFactory: NamedLoggerFactory,
    private val checkedInvariant: Boolean,
    private val executionContext: ExecutionContext,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    private val protocolVersion: ProtocolVersion,
) extends NamedLogging
    with FlagCloseable {
  import ConflictDetector.*
  import LockableStates.withRC

  /** Execution queue to ensure that there are no concurrent accesses to the states */
  private[this] val executionQueue: SimpleExecutionQueue =
    new SimpleExecutionQueue(
      "conflict-detector-queue",
      futureSupervisor,
      timeouts,
      loggerFactory,
    )

  /** Lock management and cache for contracts. */
  private[this] val contractStates: LockableStates[LfContractId, ActiveContractStore.Status] =
    LockableStates.empty[LfContractId, ActiveContractStore.Status](
      acs,
      loggerFactory,
      timeouts,
      executionContext = executionContext,
    )

  /** Lock management and cache for keys. */
  private[this] val keyStates: LockableStates[LfGlobalKey, ContractKeyJournal.Status] =
    LockableStates.empty[LfGlobalKey, ContractKeyJournal.Status](
      contractKeyJournal,
      loggerFactory,
      timeouts,
      executionContext = executionContext,
    )

  /** Contains the [[com.digitalasset.canton.RequestCounter]]s of all requests
    * that have registered their activeness check using [[registerActivenessSet]]
    * and not yet completed it using [[checkActivenessAndLock]].
    * The [[ConflictDetector.PendingActivenessCheck]] stores what needs to be done
    * during the activeness check.
    */
  private[this] val pendingActivenessChecks: mutable.Map[RequestCounter, PendingActivenessCheck] =
    new mutable.HashMap()

  /** Contains the [[com.digitalasset.canton.RequestCounter]]s of all requests
    * that have completed their activeness check using [[checkActivenessAndLock]]
    * and have not yet been finalized using [[finalizeRequest]].
    * The [[LockedStates]] contains the locked contracts, keys, and the checked transfers.
    */
  private[this] val transfersAndLockedStates: mutable.Map[RequestCounter, LockedStates] =
    new mutable.HashMap()

  /** Contains the [[PendingEvictions]] for each request that has been finalized in-memory
    * and whose changes are being persisted to the stores.
    * The values are only needed for invariant checking and debugging
    * whereas the key set is needed for precondition checking.
    *
    * The eviction strategy is defined via [[LockableStatus.shouldEvict]].
    */
  private[this] val pendingEvictions: TrieMap[RequestCounter, PendingEvictions] = new TrieMap()

  private[this] val directExecutionContext: DirectExecutionContext =
    DirectExecutionContext(noTracingLogger)

  private[this] val initialTransferCounter = TransferCounter.forCreatedContract(protocolVersion)

  /** Registers a pending activeness set.
    * This marks all contracts and keys in the `activenessSet` with a pending activeness check.
    * If necessary, it creates in-memory states for them and fetches their latest state from the stores.
    *
    * May be called concurrently with any other method.
    * Must be called before [[checkActivenessAndLock]] is called for the request.
    *
    * @return The future completes once all states for contracts and keys in `activenessSet` have been fetched from the stores
    *         and added to the in-memory conflict detection state.
    *         Errors from the stores fail the future as a [[LockableStates.ConflictDetectionStoreAccessError]].
    */
  def registerActivenessSet(rc: RequestCounter, activenessSet: ActivenessSet)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    implicit val ec: ExecutionContext = executionContext

    for {
      handle <- pendingActivenessCheckGuarded(rc, activenessSet)
      prefetched <- FutureUnlessShutdown.outcomeF(
        prefetch(rc, handle)
      )
      _ <- runSequentially(s"prefetch states for request $rc")(
        providePrefetchedStatesUnguarded(rc, prefetched)
      )
      _ <- FutureUnlessShutdown.outcomeF(handle.statesReady)
    } yield ()
  }

  private[this] def pendingActivenessCheckGuarded(
      rc: RequestCounter,
      activenessSet: ActivenessSet,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PrefetchHandle] = {
    implicit val ec = executionContext
    runSequentially(s"register activeness check for request $rc")(
      pendingActivenessCheckUnguarded(rc, activenessSet)
    ).map(_.valueOr(err => ErrorUtil.internalError(err)))
  }

  /** Ensures that every contract and key in `activenessSet` has an in-memory state
    * and marks these states with a pending activeness check.
    * The returned [[ConflictDetector.PrefetchHandle]] contains the contracts and keys
    * that must be retrieved from the stores, and a future that completes whan all other states are available.
    *
    * This method may be called only from a guarded context to ensure sequential access to the in-memory state.
    */
  private[this] def pendingActivenessCheckUnguarded(
      rc: RequestCounter,
      activenessSet: ActivenessSet,
  )(implicit
      traceContext: TraceContext
  ): Either[IllegalConflictDetectionStateException, PrefetchHandle] =
    Either.cond(
      !(pendingActivenessChecks.contains(rc) || transfersAndLockedStates.contains(rc) ||
        pendingEvictions.contains(rc)), {
        logger.trace(withRC(rc, "Registering pending activeness check"))

        val contractHandle = contractStates.pendingActivenessCheck(rc, activenessSet.contracts)
        val keyHandle = keyStates.pendingActivenessCheck(rc, activenessSet.keys)

        val statesReady = {
          implicit val ec: ExecutionContext = directExecutionContext
          contractHandle.availableF.zip(keyHandle.availableF).void
        }

        // Do not prefetch transfers. Might be worth implementing at some time.
        val pending = new PendingActivenessCheck(
          activenessSet.transferIds,
          contractHandle,
          keyHandle,
          statesReady,
        )
        pendingActivenessChecks.put(rc, pending).discard

        checkInvariant()

        new PrefetchHandle(
          contractsToFetch = contractHandle.toBeFetched,
          keysToFetch = keyHandle.toBeFetched,
          statesReady = statesReady,
        )
      },
      IllegalConflictDetectionStateException(s"Request $rc is already in flight."),
    )

  /** Fetch the states for the contracts and keys in the handle from the stores and return them.
    * Errors from the stores fail the returned future as a [[LockableStates.ConflictDetectionStoreAccessError]].
    */
  private[this] def prefetch(rc: RequestCounter, handle: PrefetchHandle)(implicit
      traceContext: TraceContext
  ): Future[PrefetchedStates] = {
    implicit val ec = executionContext
    logger.trace(withRC(rc, "Prefetching states"))
    val contractsF = contractStates.prefetchStates(handle.contractsToFetch)
    val keysF = keyStates.prefetchStates(handle.keysToFetch)
    contractsF.zipWith(keysF)(PrefetchedStates.apply)
  }

  /** Adds the `fetchedStates` to the in-memory versioned state unless the in-memory state is newer.
    * States that were to be fetched and are not contained in [[ConflictDetector.PrefetchedStates]]
    * are considered fresh.
    *
    * This method may be called only from a guarded context to ensure sequential access to the in-memory state.
    */
  private[this] def providePrefetchedStatesUnguarded(
      rc: RequestCounter,
      fetchedStates: PrefetchedStates,
  )(implicit traceContext: TraceContext): Unit = {
    logger.trace(withRC(rc, "Adding prefetched states"))
    val pending = pendingActivenessChecks.getOrElse(
      rc,
      ErrorUtil.internalError(
        new IllegalStateException(s"Request $rc has no pending activeness check.")
      ),
    )
    contractStates.providePrefetchedStates(pending.contracts, fetchedStates.contracts)
    keyStates.providePrefetchedStates(pending.keys, fetchedStates.keys)
    checkInvariant()
  }

  /** Performs the pre-registered activeness check consisting of the following:
    * <ul>
    *   <li>Perform the [[ActivenessCheck]]s for contracts and keys in the [[ActivenessSet]].</li>
    *   <li>Lock the contracts and keys according to the [[ActivenessSet]]</li>
    *   <li>Check that the transfer-ins from the [[ActivenessSet]] are active</li>
    * </ul>
    *
    * Must not be called concurrently with itself or [[finalizeRequest]].
    *
    * @return The activeness result of the activeness check
    *         The future fails with [[IllegalConflictDetectionStateException]] in the following cases:
    *         <ul>
    *           <li>The request has not registered the activeness check using [[registerActivenessSet]]
    *               or the future returned by registration has not yet completed.</li>
    *           <li>The request has already performed its activeness check.</li>
    *           <li>Invariant checks fail</li>
    *         </ul>
    */
  def checkActivenessAndLock(
      rc: RequestCounter
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ActivenessResult] = {
    runSequentially(s"activeness check for request $rc") {
      checkActivenessAndLockUnguarded(rc)
    }.map(_.valueOr(ErrorUtil.internalError))(executionContext)
      .flatMap { case (contractsResult, keysResult, pending) =>
        // Checking transfer IDs need not be done within the sequential execution context
        // because the TaskScheduler ensures that this task completes before new tasks are scheduled.

        implicit val ec: ExecutionContext = executionContext
        val inactiveTransfers = Set.newBuilder[TransferId]
        def checkTransfer(transferId: TransferId): Future[Unit] = {
          logger.trace(withRC(rc, s"Checking that transfer $transferId is active."))
          transferCache.lookup(transferId).value.map {
            case Right(_) =>
            case Left(UnknownTransferId(_)) | Left(TransferCompleted(_, _)) =>
              val _ = inactiveTransfers += transferId
          }
        }

        FutureUnlessShutdown.outcomeF {
          for {
            _ <- MonadUtil.sequentialTraverse_(pending.transferIds)(checkTransfer)
            _ <- sequentiallyCheckInvariant()
          } yield ActivenessResult(
            contracts = contractsResult,
            inactiveTransfers = inactiveTransfers.result(),
            keys = keysResult,
          )
        }
      }(executionContext)
  }

  private def checkActivenessAndLockUnguarded(
      rc: RequestCounter
  )(implicit traceContext: TraceContext): Either[
    IllegalConflictDetectionStateException,
    (
        ActivenessCheckResult[LfContractId, Status],
        ActivenessCheckResult[LfGlobalKey, ContractKeyJournal.Status],
        PendingActivenessCheck,
    ),
  ] = {
    // This part must be run sequentially because we're accessing the mutable state in `contractStates` and `keyStates`.
    // The task scheduler's synchronization is not enough because the task scheduler doesn't cover
    // prefetching and eviction, which access and modify the mutable state.
    checkInvariant()

    pendingActivenessChecks.remove(rc) match {
      case Some(pending) =>
        if (!pending.statesReady.isCompleted)
          ErrorUtil.internalError(
            IllegalConflictDetectionStateException(s"Not all states have been prefetched for $rc.")
          )

        val (lockedContracts, contractsResult) = contractStates.checkAndLock(pending.contracts)
        val (lockedKeys, keysResult) = keyStates.checkAndLock(pending.keys)
        val lockedStates = LockedStates(pending.transferIds, lockedContracts, lockedKeys)
        transfersAndLockedStates.put(rc, lockedStates).discard

        checkInvariant()
        Right((contractsResult, keysResult, pending))
      case None =>
        Left(
          IllegalConflictDetectionStateException(s"Request $rc has no pending activeness check.")
        )
    }
  }

  /** Updates the states for the in-flight request `toc.`[[com.digitalasset.canton.participant.util.TimeOfChange.rc rc]] according to the `commitSet`.
    * All states in the [[CommitSet]] must have been declared as [[ActivenessCheck.lock]] in the [[ActivenessSet]].
    *
    * <ul>
    *   <li>Contracts in `commitSet.`[[CommitSet.archivals archivals]] are archived.</li>
    *   <li>Contracts in `commitSet.`[[CommitSet.creations creations]] are created.</li>
    *   <li>Contracts in `commitSet.`[[CommitSet.transferOuts transferOuts]] are transferred away to the given target domain.</li>
    *   <li>Contracts in `commitSet.`[[CommitSet.transferIns transferIns]] become active with the given source domain</li>
    *   <li>Keys in `commitSet.`[[CommitSet.keyUpdates keyUpdates]] are set to the given [[com.digitalasset.canton.participant.store.ContractKeyJournal.Status]]</li>
    *   <li>All contracts and keys locked by the [[ActivenessSet]] are unlocked.</li>
    *   <li>Transfers in [[ActivenessSet.transferIds]] are completed if they are in `commitSet.`[[CommitSet.transferIns transferIns]].</li>
    * </ul>
    * and writes the updates from `commitSet` to the [[com.digitalasset.canton.participant.store.ActiveContractStore]]
    * and the [[com.digitalasset.canton.participant.store.ContractKeyJournal]].
    * If no exception is thrown, the request is no longer in flight.
    *
    * All changes are carried out even if the [[ActivenessResult]] was not successful.
    *
    * Must not be called concurrently with [[checkActivenessAndLock]].
    *
    * @param commitSet The contracts and keys to be modified
    *                  The commit set may only contain contracts and keys that have been locked in
    *                  the corresponding activeness check.
    *                  The transfer IDs in [[CommitSet.transferIns]] need not have been checked for activeness though.
    * @return The outer future completes when the conflict detector's internal state has been updated.
    *         The inner future completes when the updates have been persisted.
    *         [[com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker.RequestTrackerStoreError]]s
    *         are signalled as [[scala.Left$]].
    *         The futures may also fail with the following exceptions:
    *         <ul>
    *         <li>[[IllegalConflictDetectionStateException]] if an internal error is detected.
    *         In particular if invariant checking is enabled and the invariant is violated.
    *         This exception is not recoverable.</li>
    *         <li>[[RequestTracker$.InvalidCommitSet]] if the `commitSet` violates its precondition.
    *         For the contract, key, and transfer state management, this case is treated like passing [[CommitSet.empty]].</li>
    *         <li>[[java.lang.IllegalArgumentException]] if the request is not in flight.</li>
    *         </ul>
    */
  def finalizeRequest(commitSet: CommitSet, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    FutureUnlessShutdown[Either[NonEmptyChain[RequestTrackerStoreError], Unit]]
  ] =
    runSequentially(s"finalize request ${toc.rc}") {
      checkInvariant()
      val rc = toc.rc

      logger.trace(withRC(rc, "Updating contract and key states in the conflict detector"))

      val locked @ LockedStates(checkedTransfers, lockedContracts, lockedKeys) =
        transfersAndLockedStates
          .remove(rc)
          .getOrElse(throw new IllegalArgumentException(s"Request $rc is not in flight."))

      val CommitSet(archivals, creations, transferOuts, transferIns, keyUpdates) = commitSet

      val unlockedChanges = Seq(
        UnlockedChanges(creations.keySet -- lockedContracts, "Creations"),
        UnlockedChanges(transferIns.keySet -- lockedContracts, "Transfer-ins"),
        UnlockedChanges(archivals.keySet -- lockedContracts, "Archivals"),
        UnlockedChanges(transferOuts.keySet -- lockedContracts, "Transfer-outs"),
        UnlockedChanges(keyUpdates.keySet -- lockedKeys, "Keys"),
      )
      def nonEmpty(unlockedChanges: UnlockedChanges): Boolean = {
        implicit val pretty = unlockedChanges.pretty
        val nonEmpty = !unlockedChanges.isEmpty
        if (nonEmpty)
          logger.warn(
            withRC(
              rc,
              show"${unlockedChanges.name.unquoted} ${unlockedChanges.unlockedIds} in commit set are not locked.",
            )
          )
        nonEmpty
      }
      if (unlockedChanges.exists(nonEmpty)) {
        lockedContracts.foreach(contractStates.releaseLock(rc, _))
        lockedKeys.foreach(keyStates.releaseLock(rc, _))
        // There is no need to update the `pendingEvictions` here because the request cannot be in there by the invariant.
        checkInvariant()
        FutureUnlessShutdown.failed(InvalidCommitSet(rc, commitSet, locked))
      } else {
        val pendingContractWrites = new mutable.ArrayDeque[LfContractId]

        lockedContracts.foreach { coid =>
          val isActivation = creations.contains(coid) || transferIns.contains(coid)
          val optTargetDomain = transferOuts.get(coid).map(_.unwrap.targetDomainId)
          val isDeactivation = optTargetDomain.isDefined || archivals.contains(coid)
          if (isDeactivation) {
            if (isActivation) {
              logger.trace(withRC(rc, s"Activating and deactivating transient contract $coid."))
            } else {
              logger.trace(withRC(rc, s"Deactivating contract $coid."))
            }
            val transferCounter = transferOuts.get(coid).flatMap(_.unwrap.transferCounter)
            val newStatus = optTargetDomain.fold[Status](Archived) { targetDomain =>
              TransferredAway(targetDomain, transferCounter)
            }
            contractStates.setStatusPendingWrite(coid, newStatus, toc)
            pendingContractWrites += coid
          } else if (isActivation) {
            val transferCounter = transferIns.get(coid) match {
              case Some(value) => value.unwrap.transferCounter
              case None => creations.get(coid).flatMap(_.unwrap.transferCounter)
            }

            logger.trace(withRC(rc, s"Activating contract $coid."))
            contractStates.setStatusPendingWrite(
              coid,
              Active(transferCounter),
              toc,
            )
            pendingContractWrites += coid
          } else {
            contractStates.releaseLock(rc, coid)
          }
        }

        /* Complete only those transfers that have been checked for activeness in Phase 3
         * Non-transferring participants will not check for transfers being active,
         * but nevertheless record that the contract was transferred in from a certain domain.
         */
        val transfersToComplete =
          transferIns.values.filter(t => checkedTransfers.contains(t.unwrap.transferId))

        val pendingKeyWrites = new mutable.ArrayDeque[LfGlobalKey]
        lockedKeys.foreach { key =>
          keyUpdates.get(key) match {
            case None => keyStates.releaseLock(rc, key)
            case Some(newState) =>
              logger.trace(withRC(rc, show"Updating key $key to $newState"))
              keyStates.setStatusPendingWrite(key, newState, toc)
              pendingKeyWrites += key
          }
        }

        // Synchronously complete all transfers in the TransferCache.
        // (Use a List rather than a Stream to ensure synchronicity!)
        // Writes to the TransferStore are still asynchronous.
        val pendingTransferWrites =
          transfersToComplete.toList.map(t =>
            transferCache.completeTransfer(t.unwrap.transferId, toc)
          )

        pendingEvictions
          .put(
            rc,
            PendingEvictions(locked, commitSet, pendingContractWrites.toSeq, pendingKeyWrites.toSeq),
          )
          .discard

        checkInvariant()
        // All in-memory states have been updated. Persist the changes asynchronously.
        val storeFuture = {
          implicit val ec: ExecutionContext = executionContext
          /* We write archivals, creations, and transfers to the ACS even if we haven't found the contracts in the
           * contract status map such that the right data makes it to the ACS, which is a journal rather than a snapshot.
           */
          logger.trace(withRC(rc, s"About to write commit set to the conflict detection stores"))
          val archivalWrites = acs.archiveContracts(archivals.keySet.to(LazyList), toc)
          val creationWrites = acs.markContractsActive(
            creations.keySet.map(cid => cid -> initialTransferCounter).to(LazyList),
            toc,
          )
          val transferOutWrites =
            acs.transferOutContracts(
              transferOuts
                .map { case (coid, wrapped) =>
                  val CommitSet.TransferOutCommit(targetDomain, _, transferCounter) = wrapped.unwrap
                  (coid, targetDomain, transferCounter, toc)
                }
                .to(LazyList)
            )

          val transferInWrites =
            acs.transferInContracts(
              transferIns
                .map { case (coid, wrapped) =>
                  val CommitSet.TransferInCommit(transferId, _contractMetadata, transferCounter) =
                    wrapped.unwrap
                  (coid, transferId.sourceDomain, transferCounter, toc)
                }
                .to(LazyList)
            )
          val transferCompletions = pendingTransferWrites.sequence_
          val keyWrites =
            contractKeyJournal.addKeyStateUpdates(keyUpdates.view.mapValues(_ -> toc).toMap)

          // Collect the results from the above futures run in parallel.
          // A for comprehension does not work due to the explicit type parameters.
          val monad = Monad[CheckedT[Future, AcsError, AcsWarning, *]]
          val acsFuture =
            monad.flatMap(archivalWrites)(_ =>
              monad.flatMap(creationWrites)(_ =>
                monad.flatMap(transferOutWrites)(_ => transferInWrites)
              )
            )

          logger.debug(withRC(rc, "Wait for conflict detection store updates to complete"))
          List(
            acsFuture.toEitherTWithNonaborts
              .leftMap(_.map(RequestTracker.AcsError))
              .value
              .map(_.toValidated),
            transferCompletions.toEitherTWithNonaborts
              .leftMap(_.map(TransferStoreError))
              .value
              .map(_.toValidated),
            keyWrites.leftMap(ContractKeyJournalError).value.map(_.toValidatedNec),
          ).sequence
        }

        // The task scheduler ensures that everything up to here
        // does not interleave with `checkActivenessAndLock` or `finalizeRequest` up to here,
        // except for the writes to the stores spawned above.
        // Every single body of a Future after the next `flatMap` may be interleaved
        // at EVERY flatMap in on anything that runs through guardedExecution. This includes all operations up to here.
        // The execution queue merely ensures that each Future by itself runs atomically.
        FutureUnlessShutdown
          .outcomeF(storeFuture)(executionContext)
          .flatMap { results =>
            logger.debug(
              withRC(
                rc,
                "Conflict detection store updates have finished. Waiting for evicting states.",
              )
            )
            runSequentially(s"evict states for request $rc") {
              val result = results.sequence_.toEither
              logger.debug(withRC(rc, "Evicting states"))
              // Schedule evictions only if no shutdown is happening. (ecForCd is shut down before ecForAcs.)
              pendingContractWrites.foreach(contractStates.signalWriteAndTryEvict(rc, _))
              pendingKeyWrites.foreach(keyStates.signalWriteAndTryEvict(rc, _))
              pendingEvictions.remove(rc).discard
              checkInvariant()
              result
            }
          }(executionContext)
      }
    }

  /** Returns the internal state of the contract:
    * <ul>
    *   <li>`Some(cs)` if the contract is in memory with state `cs`.</li>
    *   <li>`None` signifies that the contract state is not held in memory (it may be stored in the ACS, though).</li>
    * </ul>
    *
    * @see LockableStates.getInternalState
    */
  @VisibleForTesting
  private[conflictdetection] def getInternalContractState(
      coid: LfContractId
  ): Option[ImmutableContractState] =
    contractStates.getInternalState(coid)

  /** Returns the internal state of the key:
    * <ul>
    *   <li>`Some(ks)` if the key is in memory with state `cs`.</li>
    *   <li>`None` signifies that the key state is not held in memory (it may be stored in the contract key journal, though).</li>
    * </ul>
    *
    * @see LockableStates.getInternalState
    */
  @VisibleForTesting
  private[conflictdetection] def getInternalKeyState(key: LfGlobalKey): Option[ImmutableKeyState] =
    keyStates.getInternalState(key)

  /** Returns the state of a contract, fetching it from the [[com.digitalasset.canton.participant.store.ActiveContractStore]] if it is not in memory.
    * If called concurrently, the state may only be outdated.
    */
  def getApproximateStates(coids: Seq[LfContractId])(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, ContractState]] =
    contractStates.getApproximateStates(coids)

  /** Ensures that the thunk `x` executes in the `executionQueue`,
    * i.e., is sequentialized w.r.t. all other calls to `runSequentially`.
    *
    * @return The future completes after `x` has been executed and with `x`'s result.
    */
  private[this] def runSequentially[A](
      description: String
  )(x: => A)(implicit traceContext: TraceContext): FutureUnlessShutdown[A] = {
    implicit val ec = executionContext
    executionQueue.execute(Future { x }, description)
  }

  /** Checks the class invariant if invariant checking is enabled.
    *
    * Must only be called from a Future that's guarded by the execution queue.
    *
    * @throws IllegalConflictDetectionStateException if the invariant is violated and invariant checking is enabled.
    */
  private[this] def checkInvariant()(implicit traceContext: TraceContext): Unit =
    if (checkedInvariant) invariant() else ()

  /** Checks the class invariant if invariant checking is enabled.
    *
    * @throws IllegalConflictDetectionStateException if the invariant is violated and invariant checking is enabled.
    */
  private[this] def sequentiallyCheckInvariant()(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    if (checkedInvariant)
      runSequentially(s"invariant check")(invariant())
        .onShutdown(logger.debug("Invariant check aborted due to shutdown"))(executionContext)
    else Future.unit

  /** Checks the class invariant.
    *
    * @throws IllegalConflictDetectionStateException if the invariant does not hold.
    */
  private[this] def invariant()(implicit traceContext: TraceContext): Unit = {
    def assertDisjoint[A](
        xs: (collection.Set[A], String),
        ys: (collection.Set[A], String),
    ): Unit = {
      val overlap = xs._1 intersect ys._1
      if (overlap.nonEmpty)
        throw IllegalConflictDetectionStateException(
          s"${xs._2} overlap with ${ys._2}. Overlap: $overlap"
        )
    }

    assertDisjoint(
      pendingActivenessChecks.keySet -> "pendingActivenessChecks",
      transfersAndLockedStates.keySet -> "transfersAndLockedStates",
    )
    assertDisjoint(
      transfersAndLockedStates.keySet -> "transfersAndLockedStates",
      pendingEvictions.keySet -> "pendingEvictions",
    )
    assertDisjoint(
      pendingActivenessChecks.keySet -> "pendingActivenessChecks",
      pendingEvictions.keySet -> "pendingEvictions",
    )

    contractStates.invariant(pendingActivenessChecks, transfersAndLockedStates, pendingEvictions)(
      _.contracts.affected,
      _.contracts,
      _.pendingContracts,
    )
    keyStates.invariant(pendingActivenessChecks, transfersAndLockedStates, pendingEvictions)(
      _.keys.affected,
      _.keys,
      _.pendingKeys,
    )
  }

  override protected def onClosed(): Unit = Lifecycle.close(executionQueue)(logger)
}

private[conflictdetection] object ConflictDetector {
  private[ConflictDetector] final case class PendingEvictions(
      lockedStates: LockedStates,
      commitSet: CommitSet,
      pendingContracts: Seq[LfContractId],
      pendingKeys: Seq[LfGlobalKey],
  )

  type ImmutableContractState = ImmutableLockableState[ActiveContractStore.Status]
  val ImmutableContractState: ImmutableLockableState.type = ImmutableLockableState

  type ImmutableKeyState = ImmutableLockableState[ContractKeyJournal.Status]
  val ImmutableKeyState: ImmutableLockableState.type = ImmutableLockableState

  /** Contains the contracts and keys to fetch for the activeness check
    * and a future that completes when all other contracts' and keys' states are available in memory.
    */
  private class PrefetchHandle(
      val contractsToFetch: Iterable[LfContractId],
      val keysToFetch: Iterable[LfGlobalKey],
      val statesReady: Future[Unit],
  )

  /** The prefetched states to be inserted into the in-memory states */
  private final case class PrefetchedStates(
      contracts: Map[LfContractId, ContractState],
      keys: Map[LfGlobalKey, ContractKeyState],
  )

  /** Stores the handles from the registration of the activeness check until it actually is carried out.
    *
    * @param transferIds The transfer ids from the activeness set
    * @param contracts The handle for contracts
    * @param keys The handle for keys
    * @param statesReady The future that completes when all from [[PrefetchHandle.statesReady]]
    */
  private final class PendingActivenessCheck private[ConflictDetector] (
      val transferIds: Set[TransferId],
      val contracts: LockableStatesCheckHandle[LfContractId, ActiveContractStore.Status],
      val keys: LockableStatesCheckHandle[LfGlobalKey, ContractKeyJournal.Status],
      val statesReady: Future[Unit],
  ) {
    require(contracts.requestCounter == keys.requestCounter)
  }

  /** Stores the state that must be passed from the activeness check to the finalization.
    *
    * @param transfers The transfers whose activeness was checked
    * @param contracts The contracts that have been locked
    * @param keys The keys that have been locked
    */
  final case class LockedStates(
      transfers: Set[TransferId],
      contracts: Seq[LfContractId],
      keys: Seq[LfGlobalKey],
  ) extends PrettyPrinting {

    override def pretty: Pretty[LockedStates] = prettyOfClass(
      paramIfNonEmpty("transfers", _.transfers),
      paramIfNonEmpty("contracts", _.contracts),
      paramIfNonEmpty("keys", _.keys),
    )
  }

  // Hide the type parameter as an existential so that we can put different ones into a list
  // and yet have the right `Pretty` instance bundled.
  private sealed abstract class UnlockedChanges(val name: String) {
    type T
    def unlockedIds: Set[T]
    def pretty: Pretty[T]
    def isEmpty: Boolean = unlockedIds.isEmpty
  }
  private object UnlockedChanges {
    def apply[K](ids: Set[K], name: String)(implicit prettyK: Pretty[K]): UnlockedChanges =
      new UnlockedChanges(name) {
        override type T = K
        override val unlockedIds: Set[T] = ids
        override val pretty: Pretty[T] = prettyK
      }
  }
}
