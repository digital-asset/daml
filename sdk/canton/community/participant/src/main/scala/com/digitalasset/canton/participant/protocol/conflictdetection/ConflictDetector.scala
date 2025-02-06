// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.Monad
import cats.data.{Chain, NonEmptyChain}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.concurrent.{DirectExecutionContext, FutureSupervisor}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.conflictdetection.LockableStates.LockableStatesCheckHandle
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker.{
  InvalidCommitSet,
  ReassignmentsStoreError,
  RequestTrackerStoreError,
}
import com.digitalasset.canton.participant.store.ActiveContractStore
import com.digitalasset.canton.participant.store.ActiveContractStore.*
import com.digitalasset.canton.participant.store.ReassignmentStore.{
  AssignmentStartingBeforeUnassignment,
  ReassignmentCompleted,
  UnknownReassignmentId,
}
import com.digitalasset.canton.participant.store.memory.ReassignmentCache
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{CheckedT, ErrorUtil, MonadUtil, SimpleExecutionQueue}
import com.digitalasset.canton.{ReassignmentCounter, RequestCounter}
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** The [[ConflictDetector]] stores the state of contracts activated or deactivated by in-flight requests in memory.
  * Such states take precedence over the states stored in the ACS, which are only updated when a request is finalized.
  * A request is in-flight from the call to [[ConflictDetector.registerActivenessSet]] until the corresponding call to
  * [[ConflictDetector.finalizeRequest]] has written the updates to the ACS.
  *
  * The [[ConflictDetector]] also checks that assignments refer to active reassignments and atomically completes them
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
    private val reassignmentCache: ReassignmentCache,
    protected override val loggerFactory: NamedLoggerFactory,
    private val checkedInvariant: Boolean,
    private val executionContext: ExecutionContext,
    exitOnFatalFailures: Boolean,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
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
      crashOnFailure = exitOnFatalFailures,
    )

  /** Lock management and cache for contracts. */
  private[this] val contractStates: LockableStates[LfContractId, ActiveContractStore.Status] =
    LockableStates.empty[LfContractId, ActiveContractStore.Status](
      acs,
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
    * The [[LockedStates]] contains the locked contracts, keys, and the checked reassignments.
    */
  private[this] val reassignmentsAndLockedStates: mutable.Map[RequestCounter, LockedStates] =
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

  private[this] val initialReassignmentCounter = ReassignmentCounter.Genesis

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
      prefetched <- prefetch(rc, handle)
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
    implicit val ec: ExecutionContext = executionContext
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
      !(pendingActivenessChecks.contains(rc) || reassignmentsAndLockedStates.contains(rc) ||
        pendingEvictions.contains(rc)), {
        logger.trace(withRC(rc, "Registering pending activeness check"))

        val contractHandle = contractStates.pendingActivenessCheck(rc, activenessSet.contracts)

        val statesReady = {
          implicit val ec: ExecutionContext = directExecutionContext
          contractHandle.availableF.void
        }

        // Do not prefetch reassignments. Might be worth implementing at some time.
        val pending = new PendingActivenessCheck(
          activenessSet.reassignmentIds,
          contractHandle,
          statesReady,
        )
        pendingActivenessChecks.put(rc, pending).discard

        checkInvariant()

        new PrefetchHandle(
          contractsToFetch = contractHandle.toBeFetched,
          statesReady = statesReady,
        )
      },
      IllegalConflictDetectionStateException(s"Request $rc is already in-flight."),
    )

  /** Fetch the states for the contracts and keys in the handle from the stores and return them.
    * Errors from the stores fail the returned future as a [[LockableStates.ConflictDetectionStoreAccessError]].
    */
  private[this] def prefetch(rc: RequestCounter, handle: PrefetchHandle)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[PrefetchedStates] = {
    implicit val ec: ExecutionContext = executionContext
    logger.trace(withRC(rc, "Prefetching states"))
    val contractsF = contractStates.prefetchStates(handle.contractsToFetch)
    contractsF.map(PrefetchedStates.apply)
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
    checkInvariant()
  }

  /** Performs the pre-registered activeness check consisting of the following:
    * <ul>
    *   <li>Perform the [[ActivenessCheck]]s for contracts and keys in the [[ActivenessSet]].</li>
    *   <li>Lock the contracts and keys according to the [[ActivenessSet]]</li>
    *   <li>Check that the assignments from the [[ActivenessSet]] are active</li>
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
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ActivenessResult] =
    runSequentially(s"activeness check for request $rc") {
      checkActivenessAndLockUnguarded(rc)
    }.map(_.valueOr(ErrorUtil.internalError))(executionContext)
      .flatMap { case (contractsResult, pending) =>
        // Checking reassignment IDs need not be done within the sequential execution context
        // because the TaskScheduler ensures that this task completes before new tasks are scheduled.

        implicit val ec: ExecutionContext = executionContext
        val inactiveReassignments = Set.newBuilder[ReassignmentId]
        def checkReassignment(reassignmentId: ReassignmentId): FutureUnlessShutdown[Unit] = {
          logger.trace(withRC(rc, s"Checking that reassignment $reassignmentId is active."))
          reassignmentCache.lookup(reassignmentId).value.map {
            case Right(_) =>
            case Left(UnknownReassignmentId(_)) |
                Left(ReassignmentCompleted(_, _)) | Left(AssignmentStartingBeforeUnassignment(_)) =>
              val _ = inactiveReassignments += reassignmentId
          }
        }

        for {
          _ <- MonadUtil.sequentialTraverse_[FutureUnlessShutdown, ReassignmentId](
            pending.reassignmentIds
          )(checkReassignment)
          _ <- sequentiallyCheckInvariant()
        } yield ActivenessResult(
          contracts = contractsResult,
          inactiveReassignments = inactiveReassignments.result(),
        )
      }(executionContext)

  private def checkActivenessAndLockUnguarded(
      rc: RequestCounter
  )(implicit traceContext: TraceContext): Either[
    IllegalConflictDetectionStateException,
    (
        ActivenessCheckResult[LfContractId, Status],
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
        val lockedStates = LockedStates(pending.reassignmentIds, lockedContracts)
        reassignmentsAndLockedStates.put(rc, lockedStates).discard

        checkInvariant()
        Right((contractsResult, pending))
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
    *   <li>Contracts in `commitSet.`[[CommitSet.unassignments unassignments]] are reassigned away to the given target synchronizer.</li>
    *   <li>Contracts in `commitSet.`[[CommitSet.assignments assignments]] become active with the given source synchronizer</li>
    *   <li>All contracts and keys locked by the [[ActivenessSet]] are unlocked.</li>
    *   <li>Reassignments in [[ActivenessSet.reassignmentIds]] are completed if they are in `commitSet.`[[CommitSet.assignments assignments]].</li>
    * </ul>
    * and writes the updates from `commitSet` to the [[com.digitalasset.canton.participant.store.ActiveContractStore]].
    * If no exception is thrown, the request is no longer in-flight.
    *
    * All changes are carried out even if the [[ActivenessResult]] was not successful.
    *
    * Must not be called concurrently with [[checkActivenessAndLock]].
    *
    * @param commitSet The contracts and keys to be modified
    *                  The commit set may only contain contracts and keys that have been locked in
    *                  the corresponding activeness check.
    *                  The reassignment IDs in [[CommitSet.assignments]] need not have been checked for activeness though.
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
    *         For the contract, key, and reassignment state management, this case is treated like passing [[CommitSet.empty]].</li>
    *         <li>[[java.lang.IllegalArgumentException]] if the request is not in-flight.</li>
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

      val locked @ LockedStates(checkedReassignments, lockedContracts) =
        reassignmentsAndLockedStates
          .remove(rc)
          .getOrElse(throw new IllegalArgumentException(s"Request $rc is not in-flight."))

      val CommitSet(archivals, creations, unassignments, assignments) = commitSet

      val unlockedChanges = Seq(
        UnlockedChanges(creations.keySet -- lockedContracts, "Creations"),
        UnlockedChanges(assignments.keySet -- lockedContracts, "Assignments"),
        UnlockedChanges(archivals.keySet -- lockedContracts, "Archivals"),
        UnlockedChanges(unassignments.keySet -- lockedContracts, "Unassignments"),
      )
      def nonEmpty(unlockedChanges: UnlockedChanges): Boolean = {
        implicit val pretty: Pretty[unlockedChanges.T] = unlockedChanges.pretty
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
        // There is no need to update the `pendingEvictions` here because the request cannot be in there by the invariant.
        checkInvariant()
        FutureUnlessShutdown.failed(InvalidCommitSet(rc, commitSet, locked))
      } else {
        val pendingContractWrites = new mutable.ArrayDeque[LfContractId]

        lockedContracts.foreach { coid =>
          val isActivation = creations.contains(coid) || assignments.contains(coid)
          val unassignmentO = unassignments.get(coid)
          val isDeactivation = unassignmentO.isDefined || archivals.contains(coid)
          if (isDeactivation) {
            if (isActivation) {
              logger.trace(withRC(rc, s"Activating and deactivating transient contract $coid."))
            } else {
              logger.trace(withRC(rc, s"Deactivating contract $coid."))
            }
            val newStatus = unassignmentO.fold[Status](Archived) { unassignment =>
              ReassignedAway(
                unassignment.targetSynchronizerId,
                unassignment.reassignmentCounter,
              )
            }
            contractStates.setStatusPendingWrite(coid, newStatus, toc)
            pendingContractWrites += coid
          } else if (isActivation) {
            val reassignmentCounter = assignments.get(coid) match {
              case Some(value) => value.reassignmentCounter
              case None =>
                creations
                  .get(coid)
                  .map(_.reassignmentCounter)
                  .getOrElse(
                    ErrorUtil.internalError(
                      new IllegalStateException(
                        s"We did not find active contract $coid in $creations"
                      )
                    )
                  )
            }

            logger.trace(withRC(rc, s"Activating contract $coid."))
            contractStates.setStatusPendingWrite(
              coid,
              Active(reassignmentCounter),
              toc,
            )
            pendingContractWrites += coid
          } else {
            contractStates.releaseLock(rc, coid)
          }
        }

        /* Complete only those reassignments that have been checked for activeness in Phase 3
         * Non-reassigning participants will not check for reassignments being active,
         * but nevertheless record that the contract was assigned from a certain synchronizer.
         */
        val reassignmentsToComplete =
          assignments.values.filter(t => checkedReassignments.contains(t.reassignmentId))

        // Synchronously complete all reassignments in the ReassignmentCache.
        // (Use a List rather than a Stream to ensure synchronicity!)
        // Writes to the ReassignmentStore are still asynchronous.
        val pendingReassignmentWrites =
          reassignmentsToComplete.toList.map(t =>
            reassignmentCache.completeReassignment(t.reassignmentId, toc.timestamp)
          )

        pendingEvictions
          .put(
            rc,
            PendingEvictions(locked, commitSet, pendingContractWrites.toSeq),
          )
          .discard

        checkInvariant()
        // All in-memory states have been updated. Persist the changes asynchronously.
        val storeFuture = {
          implicit val ec: ExecutionContext = executionContext
          /* We write archivals, creations, and reassignments to the ACS even if we haven't found the contracts in the
           * contract status map such that the right data makes it to the ACS, which is a journal rather than a snapshot.
           */
          logger.trace(withRC(rc, s"About to write commit set to the conflict detection stores"))
          val archivalWrites = acs.archiveContracts(archivals.keySet.to(LazyList), toc)
          val creationWrites = acs.markContractsCreated(
            creations.keySet.map(cid => cid -> initialReassignmentCounter).to(LazyList),
            toc,
          )
          val unassignmentWrites =
            acs.unassignContracts(
              unassignments
                .map { case (coid, unassignmentCommit) =>
                  val CommitSet.UnassignmentCommit(targetSynchronizer, _, reassignmentCounter) =
                    unassignmentCommit
                  (coid, targetSynchronizer, reassignmentCounter, toc)
                }
                .to(LazyList)
            )

          val assignmentWrites =
            acs.assignContracts(
              assignments
                .map { case (coid, assignmentCommit) =>
                  val CommitSet
                    .AssignmentCommit(reassignmentId, _contractMetadata, reassignmentCounter) =
                    assignmentCommit
                  (coid, reassignmentId.sourceSynchronizer, reassignmentCounter, toc)
                }
                .to(LazyList)
            )
          val reassignmentCompletions = pendingReassignmentWrites.sequence_

          // Collect the results from the above futures run in parallel.
          // A for comprehension does not work due to the explicit type parameters.
          val monad = Monad[CheckedT[FutureUnlessShutdown, AcsError, AcsWarning, *]]
          val acsFuture =
            monad.flatMap(archivalWrites)(_ =>
              monad.flatMap(creationWrites)(_ =>
                monad.flatMap(unassignmentWrites)(_ => assignmentWrites)
              )
            )

          logger.debug(withRC(rc, "Wait for conflict detection store updates to complete"))
          List(
            acsFuture.toEitherTWithNonaborts
              .leftMap(_.map(RequestTracker.AcsError.apply))
              .value
              .map(_.toValidated),
            reassignmentCompletions.toEitherTWithNonaborts
              .leftMap(_.map(ReassignmentsStoreError.apply))
              .value
              .map(_.toValidated),
          ).sequence
        }

        // The task scheduler ensures that everything up to here
        // does not interleave with `checkActivenessAndLock` or `finalizeRequest` up to here,
        // except for the writes to the stores spawned above.
        // Every single body of a Future after the next `flatMap` may be interleaved
        // at EVERY flatMap in on anything that runs through guardedExecution. This includes all operations up to here.
        // The execution queue merely ensures that each Future by itself runs atomically.
        storeFuture
          .flatMap { results =>
            logger.debug(
              withRC(
                rc,
                "Conflict detection store updates have finished. Waiting for evicting states.",
              )
            )
            runSequentially(s"evict states for request $rc") {
              val result = results.sequence_.toEither.leftMap { chainNE =>
                NonEmptyChain.fromChainUnsafe(Chain.fromSeq(chainNE.toList.distinct))
              }
              logger.debug(withRC(rc, "Evicting states"))
              // Schedule evictions only if no shutdown is happening. (ecForCd is shut down before ecForAcs.)
              pendingContractWrites.foreach(contractStates.signalWriteAndTryEvict(rc, _))
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

  /** Returns the state of a contract, fetching it from the [[com.digitalasset.canton.participant.store.ActiveContractStore]] if it is not in memory.
    * If called concurrently, the state may only be outdated.
    */
  def getApproximateStates(coids: Seq[LfContractId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[LfContractId, ContractState]] =
    contractStates.getApproximateStates(coids)

  /** Ensures that the thunk `x` executes in the `executionQueue`,
    * i.e., is sequentialized w.r.t. all other calls to `runSequentially`.
    *
    * @return The future completes after `x` has been executed and with `x`'s result.
    */
  private[this] def runSequentially[A](
      description: String
  )(x: => A)(implicit traceContext: TraceContext): FutureUnlessShutdown[A] = {
    implicit val ec: ExecutionContext = executionContext
    executionQueue.execute(Future(x), description)
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
  ): FutureUnlessShutdown[Unit] =
    if (checkedInvariant)
      runSequentially(s"invariant check")(invariant())
    else FutureUnlessShutdown.unit

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
      reassignmentsAndLockedStates.keySet -> "reassignmentsAndLockedStates",
    )
    assertDisjoint(
      reassignmentsAndLockedStates.keySet -> "reassignmentsAndLockedStates",
      pendingEvictions.keySet -> "pendingEvictions",
    )
    assertDisjoint(
      pendingActivenessChecks.keySet -> "pendingActivenessChecks",
      pendingEvictions.keySet -> "pendingEvictions",
    )

    contractStates.invariant(
      pendingActivenessChecks,
      reassignmentsAndLockedStates,
      pendingEvictions,
    )(
      _.contracts.affected,
      _.contracts,
      _.pendingContracts,
    )
  }

  override protected def onClosed(): Unit = LifeCycle.close(executionQueue)(logger)
}

private[conflictdetection] object ConflictDetector {
  private[ConflictDetector] final case class PendingEvictions(
      lockedStates: LockedStates,
      commitSet: CommitSet,
      pendingContracts: Seq[LfContractId],
  )

  type ImmutableContractState = ImmutableLockableState[ActiveContractStore.Status]
  val ImmutableContractState: ImmutableLockableState.type = ImmutableLockableState

  /** Contains the contracts and keys to fetch for the activeness check
    * and a future that completes when all other contracts' states are available in memory.
    */
  private class PrefetchHandle(
      val contractsToFetch: Iterable[LfContractId],
      val statesReady: Future[Unit],
  )

  /** The prefetched states to be inserted into the in-memory states */
  private final case class PrefetchedStates(
      contracts: Map[LfContractId, ContractState]
  )

  /** Stores the handles from the registration of the activeness check until it actually is carried out.
    *
    * @param reassignmentIds The reassignment ids from the activeness set
    * @param contracts The handle for contracts
    * @param statesReady The future that completes when all from [[PrefetchHandle.statesReady]]
    */
  private final class PendingActivenessCheck private[ConflictDetector] (
      val reassignmentIds: Set[ReassignmentId],
      val contracts: LockableStatesCheckHandle[LfContractId, ActiveContractStore.Status],
      val statesReady: Future[Unit],
  )

  /** Stores the state that must be passed from the activeness check to the finalization.
    *
    * @param reassignments The reassignments whose activeness was checked
    * @param contracts The contracts that have been locked
    */
  final case class LockedStates(
      reassignments: Set[ReassignmentId],
      contracts: Seq[LfContractId],
  ) extends PrettyPrinting {

    override protected def pretty: Pretty[LockedStates] = prettyOfClass(
      paramIfNonEmpty("reassignments", _.reassignments),
      paramIfNonEmpty("contracts", _.contracts),
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
