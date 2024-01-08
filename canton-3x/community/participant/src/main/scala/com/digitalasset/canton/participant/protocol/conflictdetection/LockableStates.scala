// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.{ConflictDetectionStore, HasPrunable}
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.google.common.annotations.VisibleForTesting

import scala.collection.compat.immutable.ArraySeq
import scala.collection.concurrent.TrieMap
import scala.collection.{concurrent, mutable}
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/** Manages in-memory states of items (contracts, keys, ...) that are used or modified by in-flight requests.
  * Such in-memory states take precedence over the states in the underlying store,
  * which is only updated when a request is finalized.
  *
  * @param lockableStatus Type-class dictionary for operations on the state that are specific to conflict detection.
  * @tparam Key Identifier for states
  * @tparam Status The status type for states.
  */
private[conflictdetection] class LockableStates[
    Key,
    Status <: PrettyPrinting with HasPrunable,
] private (
    private val store: ConflictDetectionStore[Key, Status],
    protected override val loggerFactory: NamedLoggerFactory,
    timeouts: ProcessingTimeout,
    private val executionContext: ExecutionContext,
)(
    implicit val lockableStatus: LockableStatus[Status],
    implicit val prettyKey: Pretty[Key],
    implicit val classTagKey: ClassTag[Key],
) extends NamedLogging {

  import LockableStates.*
  import Pretty.*

  /** The in-memory map for storing the states.
    * This map is also accessed by the [[DomainRouter]]
    * and must therefore be thread-safe.
    */
  private val states: concurrent.Map[Key, MutableLockableState[Status]] =
    new TrieMap[Key, MutableLockableState[Status]]()

  private val directExecutionContext = DirectExecutionContext(noTracingLogger)

  /** Registers the activeness check as pending for the given request.
    *
    * Must not be called concurrently with other methods of this class unless stated otherwise.
    */
  def pendingActivenessCheck(rc: RequestCounter, check: ActivenessCheck[Key])(implicit
      traceContext: TraceContext
  ): LockableStatesCheckHandle[Key, Status] = {
    def mkPendingState(): MutableLockableState[Status] = {
      val state = new MutableLockableState[Status](None)
      val fetchNeeded = state.registerPendingActivenessCheck()
      assert(fetchNeeded.isEmpty)
      state
    }

    val ActivenessCheck(fresh, free, active, lock, needPriorState) = check
    val toBeFetchedM = mutable.Map.empty[Key, MutableLockableState[Status]]

    // always holds a fresh mutable state object that we insert into `states`
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var next: MutableLockableState[Status] = mkPendingState()

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var otherStatesAvailable: Future[Unit] = Future.unit
    def joinFuture(f: Future[_]): Unit = {
      // deliberately discard the contents of the future
      implicit val ec = directExecutionContext
      otherStatesAvailable = otherStatesAvailable.zip(f).void
    }

    val priorStates = ArraySeq.newBuilder[KeyStateLock[Key, Status]]
    priorStates.sizeHint(needPriorState.size)

    def markPending(
        keys: Set[Key],
        mk: (Key, MutableLockableState[Status]) => KeyStateLock[Key, Status],
    ): ArraySeq[KeyStateLock[Key, Status]] = {
      val mutableStates = ArraySeq.newBuilder[KeyStateLock[Key, Status]]
      mutableStates.sizeHint(keys.size)

      for (id <- keys.iterator) {
        val newState = next
        val state = states.putIfAbsent(id, newState) match {
          case None =>
            next = mkPendingState()
            toBeFetchedM += (id -> newState)
            newState
          case Some(oldState) =>
            // `next` isn't needed in for this `id`, so conserve it for the next id
            val readyOF = oldState.registerPendingActivenessCheck()
            readyOF.fold[Unit](
              ErrorUtil.internalError(
                IllegalConflictDetectionStateException(
                  s"${lockableStatus.kind} $id has neither an in-memory state nor a pending activeness check."
                )
              )
            )(joinFuture)
            oldState
        }
        val keyState = mk(id, state)
        mutableStates += keyState
        // No need to deduplicate prior states here because markPending is called only once for each id
        if (needPriorState.contains(id)) priorStates.addOne(keyState)
      }

      mutableStates.result()
    }

    def stateWithLocking(id: Key, state: MutableLockableState[Status]): KeyStateLock[Key, Status] =
      KeyStateLock(id, state, lock.contains(id))

    // Since fresh, free, and active are pairwise disjoint, we can check and lock them in one go.
    val freshStates = markPending(fresh, stateWithLocking)
    val freeStates = markPending(free, stateWithLocking)
    val activeStates = markPending(active, stateWithLocking)
    val lockOnly = markPending(check.lockOnly, KeyStateLock(_, _, doLock = true))

    new LockableStatesCheckHandle(
      rc,
      otherStatesAvailable,
      lock.size,
      freshStates,
      freeStates,
      activeStates,
      lockOnly,
      toBeFetchedM,
      priorStates.result(),
    )(check.prettyK)
  }

  /** Adds the `fetched` states to the in-memory states that are cached in the handle.
    * Items in `handle`'s [[LockableStates.LockableStatesCheckHandle.toBeFetched]] are considered treated as nonexistent
    * if `fetched` does not contain them.
    *
    * Must not be called concurrently with other methods of this class unless stated otherwise.
    */
  def providePrefetchedStates(
      handle: LockableStatesCheckHandle[Key, Status],
      fetched: Map[Key, StateChange[Status]],
  )(implicit traceContext: TraceContext): Unit = {
    val toBeFetchedM = handle.toBeFetchedM
    val rc = handle.requestCounter
    toBeFetchedM.foreach { case (id, state) =>
      logger.trace(withRC(rc, s"Prefetched state for ${lockableStatus.kind} $id: $state"))
      state.provideFetchedState(fetched.get(id))
    }
    toBeFetchedM.clear()
  }

  /** Performs the activeness check consisting of the following:
    * <ul>
    *   <li>Check that [[LockableStates$.LockableStatesCheckHandle]]`.fresh` are fresh.</li>
    *   <li>Check that [[LockableStates$.LockableStatesCheckHandle]]`.free` are free, i.e., if they are in known,
    *     then they are not locked and free as determined by [[LockableStatus.isFree]]</li>
    *   <li>Check that [[LockableStates$.LockableStatesCheckHandle]]`.active` are active, i.e., they are known
    *     and not locked and active as determined by [[LockableStatus.isActive]].</li>
    *   <li>Lock all in [[LockableStates$.LockableStatesCheckHandle]]`.lockOnly` and the above that are marked for locking.</li>
    * </ul>
    *
    * Must not be called concurrently with other methods of this class unless stated otherwise.
    *
    * @return The activeness result of the activeness check
    * @throws IllegalConflictDetectionStateException if the handle has outstanding pre-fetches.
    */
  def checkAndLock(
      handle: LockableStatesCheckHandle[Key, Status]
  )(implicit traceContext: TraceContext): (Seq[Key], ActivenessCheckResult[Key, Status]) = {
    val rc = handle.requestCounter

    if (!handle.noOutstandingFetches)
      ErrorUtil.internalError(
        IllegalConflictDetectionStateException(
          s"Request $rc has outstanding pre-fetches: ${handle.toBeFetched}"
        )
      )
    if (!handle.availableF.isCompleted)
      ErrorUtil.internalError(
        IllegalConflictDetectionStateException(
          s"Request $rc must wait on all pre-fetches being delivered."
        )
      )

    val alreadyLocked = Set.newBuilder[Key]
    val notFree = Map.newBuilder[Key, Status]
    val notActive = Map.newBuilder[Key, Status]
    val notFresh = Set.newBuilder[Key]
    val unknown = Set.newBuilder[Key]

    val obtainedLocks = ArraySeq.newBuilder[Key]
    obtainedLocks.sizeHint(handle.lockCount)

    val priorStates = Map.newBuilder[Key, Option[Status]]
    priorStates.sizeHint(handle.priorStates.size)

    def throwOnNotPrefetched(id: Key): Nothing =
      ErrorUtil.internalError(
        IllegalConflictDetectionStateException(
          s"State for ${lockableStatus.kind} $id was not prefetched."
        )
      )

    // Add the prior state of all unlocked items before locking.
    handle.priorStates.foreach { ksl =>
      val id = ksl.id

      if (!ksl.state.locked) {
        val priorState = ksl.state.versionedState.getOrElse(throwOnNotPrefetched(id)).map(_.status)
        logger.trace(s"Returning prior state for ${lockableStatus.kind} $id: $priorState")
        priorStates += (id -> priorState)
      }
    }

    def checkLockAnd(
        ksl: KeyStateLock[Key, Status],
        ifUnlocked: Key => Option[StateChange[Status]] => Unit,
        ifLocked: Key => Option[StateChange[Status]] => Unit = { id => _ =>
          alreadyLocked += id
        },
    ): Unit = {
      val KeyStateLock(id, state, doLock) = ksl

      state.completeActivenessCheck()
      val wasUnlocked = if (doLock) {
        logger.trace(
          withRC(rc, s"Locking ${lockableStatus.kind} $id in state ${state.versionedState}")
        )
        val wasUnlocked = state.obtainLock()
        obtainedLocks += id
        wasUnlocked
      } else !state.locked

      if (wasUnlocked) {
        ifUnlocked(id)(state.versionedState.getOrElse(throwOnNotPrefetched(id)))
        if (!doLock) tryEvict(rc, id, state)
      } else {
        logger.trace(withRC(rc, s"${lockableStatus.kind} $id was already locked"))
        ifLocked(id)(state.versionedState.getOrElse(throwOnNotPrefetched(id)))
      }
    }

    def doFreshUnlocked(id: Key): Option[StateChange[Status]] => Unit = {
      case None =>
        logger.trace(withRC(rc, s"Unknown ${lockableStatus.kind} $id is fresh"))
      case Some(versionedState) =>
        logger.trace(withRC(rc, s"${lockableStatus.kind} $id is not fresh: $versionedState."))
        notFresh += id
    }

    def doFreshLocked(id: Key): Option[StateChange[Status]] => Unit = {
      case None =>
        alreadyLocked += id
      case Some(versionedState) =>
        // Even if the state is locked, we notice that `id` cannot be fresh, so let's report this as not fresh instead of locked.
        logger.trace(withRC(rc, s"${lockableStatus.kind} $id is not fresh: $versionedState."))
        notFresh += id
    }

    def doFree(id: Key): Option[StateChange[Status]] => Unit = {
      case None =>
        logger.trace(withRC(rc, s"Unknown ${lockableStatus.kind} $id is free"))
      case Some(versionedState) =>
        val status = versionedState.status
        if (!lockableStatus.isFree(status)) {
          logger.trace(withRC(rc, s"${lockableStatus.kind} $id is not free: $versionedState"))
          notFree += (id -> status)
        }
    }

    def doActive(id: Key): Option[StateChange[Status]] => Unit = {
      case None =>
        logger.trace(withRC(rc, s"Unknown ${lockableStatus.kind} $id is not active"))
        unknown += id
      case Some(versionedState) =>
        val status = versionedState.status
        if (!lockableStatus.isActive(status)) {
          logger.trace(withRC(rc, s"${lockableStatus.kind} $id is not active: $versionedState"))
          notActive += id -> status
        }
    }

    def lockOnly(id: Key)(versionedStateO: Option[StateChange[Status]]): Unit = {
      if (versionedStateO.isEmpty) {
        logger.trace(withRC(rc, s"${lockableStatus.kind} $id to be locked is unknown."))
        unknown += id
      }
    }

    handle.fresh.foreach(checkLockAnd(_, doFreshUnlocked, doFreshLocked))
    handle.free.foreach(checkLockAnd(_, doFree))
    handle.active.foreach(checkLockAnd(_, doActive))
    handle.lockOnly.foreach(checkLockAnd(_, lockOnly))

    val result = ActivenessCheckResult(
      alreadyLocked = alreadyLocked.result(),
      notFresh = notFresh.result(),
      unknown = unknown.result(),
      notFree = notFree.result(),
      notActive = notActive.result(),
      priorStates = priorStates.result(),
    )(handle.prettyK)

    (obtainedLocks.result(), result)
  }

  /** Fetches the given states from the store and returns them.
    * Nonexistent items are excluded from the returned map.
    *
    * May be called concurrently with other methods of this class.
    */
  def prefetchStates(
      toBeFetched: Iterable[Key]
  )(implicit traceContext: TraceContext): Future[Map[Key, StateChange[Status]]] = {
    implicit val ec = executionContext
    store
      .fetchStates(toBeFetched)
      .transform(
        identity,
        ConflictDetectionStoreAccessError(
          s"Conflict detection store error while retrieving ${lockableStatus.kind}s $toBeFetched",
          _,
        ),
      )
  }

  /** Returns the internal state of `id`:
    * <ul>
    *   <li>`Some(state)` if the state is in memory with state `state`.</li>
    *   <li>`None` signifies that the state is not held in memory (it may be stored in the store, though).</li>
    * </ul>
    */
  @VisibleForTesting
  private[conflictdetection] def getInternalState(id: Key): Option[ImmutableLockableState[Status]] =
    states.get(id).map(_.snapshot)

  /** Returns the state of `id`, fetching it from the [[store]] if it is not in memory.
    * May be called concurrently with any other method. In that case, the returned state may be outdated.
    */
  def getApproximateStates(
      ids: Seq[Key]
  )(implicit traceContext: TraceContext): Future[Map[Key, StateChange[Status]]] = {
    implicit val ec = executionContext
    // first, check what we have in cache
    val cached = ids.map(id => (id, states.get(id).map(_.approximateState)))
    // We don't store the state in memory, as it may be outdated when it is stored.
    // So let's fetch it from disk
    val toFetch = cached.collect {
      case (id, res) if res.isEmpty => id
    }
    val fetchedF = store.fetchStates(toFetch)
    cached
      .parTraverse {
        case (id, None) => fetchedF.map(fetched => (id, fetched.get(id)))
        case (id, Some(storedF)) => storedF.map((id, _))
      }
      .map(_.collect { case (key, Some(value)) =>
        (key, value)
      }.toMap)
  }

  /** Evict the state if it is no longer needed in the map.
    *
    * Must not be called concurrently with other methods of this class unless stated otherwise.
    */
  def signalWriteAndTryEvict(rc: RequestCounter, id: Key)(implicit
      traceContext: TraceContext
  ): Unit = {
    val state = states.getOrElse(
      id,
      throw IllegalConflictDetectionStateException(
        s"${lockableStatus.kind} $id was evicted in spite of a pending write."
      ),
    )
    state.signalWrite()
    tryEvict(rc, id, state)
  }

  /** Must not be called concurrently with other methods of this class unless stated otherwise. */
  def releaseLock(rc: RequestCounter, id: Key)(implicit traceContext: TraceContext): Unit = {
    val cs = getLockedState(rc, id)
    logger.trace(withRC(rc, s"Releasing lock on ${lockableStatus.kind} $id: $cs"))
    cs.unlock()
    tryEvict(rc, id, cs)
  }

  /** Must not be called concurrently with other methods of this class unless stated otherwise. */
  def setStatusPendingWrite(id: Key, newState: Status, toc: TimeOfChange): Unit = {
    val cs = getLockedState(toc.rc, id)
    cs.setStatusPendingWrite(newState, toc)
  }

  /** Evicts the given state if it is not locked
    * The state `expectedState` must be what `stores` maps `id` to.
    */
  private[this] def tryEvict(
      rc: RequestCounter,
      id: Key,
      expectedState: MutableLockableState[Status],
  )(implicit traceContext: TraceContext): Unit =
    if (expectedState.safeToEvict) {
      logger.trace(withRC(rc, s"Evicting ${lockableStatus.kind} $id with state $expectedState"))
      val foundState = states.remove(id)
      if (!foundState.contains(expectedState))
        throw IllegalConflictDetectionStateException(
          s"Different state $foundState for ${lockableStatus.kind} $id found than given: $expectedState"
        )
    }

  private[this] def getLockedState(rc: RequestCounter, coid: Key): MutableLockableState[Status] =
    states.getOrElse(
      coid,
      throw IllegalConflictDetectionStateException(
        withRC(rc, s"${lockableStatus.kind} $coid should have been locked, but is not in memory.")
      ),
    )

  /** Checks the class invariant.
    *
    * Must not be called concurrently with other methods of this class unless stated otherwise.
    *
    * @throws IllegalConflictDetectionStateException if the invariant does not hold.
    */
  def invariant[A, B, C](
      pendingActivenessChecks: collection.Map[RequestCounter, A],
      locked: collection.Map[RequestCounter, B],
      pendingEvictions: collection.Map[RequestCounter, C],
  )(
      selectorActiveness: A => Set[Key],
      selectorLocked: B => Seq[Key],
      selectorPending: C => Seq[Key],
  )(implicit traceContext: TraceContext): Unit = {
    def mapUnion[D, F](as: Iterable[D])(f: D => Set[F]): Set[F] =
      as.foldLeft(Set.empty[F]) { (result, a) =>
        result.union(f(a))
      }

    val lockedSnapshot = locked.map { case (id, b) => id -> selectorLocked(b).toSet }.toMap

    def assertPendingActivenessChecksMarked(): Unit = {
      states.foreach { case (id, state) =>
        if (
          state.versionedState.isEmpty &&
          LockableState.PendingActivenessCheckCounter.isEmpty(state.pendingActivenessChecks)
        )
          throw IllegalConflictDetectionStateException(
            show"${lockableStatus.kind.unquoted} $id without a pre-fetched state has no pending activeness checks."
          )

        val pendingActivenessCheckCount = state.pendingActivenessChecks

        val requestCount = pendingActivenessChecks.foldLeft(0) { case (count, (_, data)) =>
          val pending = if (selectorActiveness(data).contains(id)) 1 else 0
          count + pending
        }
        if (pendingActivenessCheckCount != requestCount)
          throw IllegalConflictDetectionStateException(
            show"${lockableStatus.kind.unquoted} $id with $requestCount pending requests has $pendingActivenessCheckCount pending requests marked."
          )
      }

      pendingActivenessChecks.foreach { case (rc, data) =>
        selectorActiveness(data).foreach { id =>
          if (!states.contains(id))
            throw IllegalConflictDetectionStateException(
              show"${lockableStatus.kind.unquoted} $id with pending request $rc is not in memory."
            )
        }
      }
    }

    def assertLockedStatesArePrefetched(): Unit = {
      states.foreach { case (id, state) =>
        if (state.locked && state.versionedState.isEmpty)
          throw IllegalConflictDetectionStateException(
            show"${lockableStatus.kind.unquoted} $id is locked without a prefetched state."
          )
      }
    }

    def assertFreshStatesAreLockedAndHaveNoPendingWrites(): Unit = {
      states.foreach { case (id, state) =>
        if (state.versionedState.contains(None)) {
          if (!state.locked && !state.hasPendingActivenessChecks)
            throw IllegalConflictDetectionStateException(
              show"${lockableStatus.kind.unquoted} $id is in memory, but neither is there a state nor a lock nor a pending activeness check."
            )
          if (state.hasPendingWrites)
            throw IllegalConflictDetectionStateException(
              show"${lockableStatus.kind.unquoted} $id has pending writes but no state."
            )
        }
      }
    }

    def assertStatesAreLockedTheCorrectNumberOfTimes(): Unit = {
      states.foreach { case (id, state) =>
        val lockCount = state.lock
        val requests = lockedSnapshot.filter(kv => kv._2.contains(id)).keySet
        if (requests.sizeCompare(lockCount) != 0)
          throw IllegalConflictDetectionStateException(
            show"${lockableStatus.kind.unquoted} $id is locked $lockCount times, but only by ${requests.size} requests:\n$requests"
          )
      }

      val lockedIds = mapUnion(lockedSnapshot.values)(Predef.identity)
      lockedIds.foreach { id =>
        if (!states.contains(id))
          throw new IllegalStateException(
            show"Locked ${lockableStatus.kind.unquoted} $id is not in memory."
          )
      }
    }

    def assertStatesWithPendingWritesAreMarkedAsPendingTheCorrectNumberOfTimes(): Unit = {
      states.foreach { case (id, state) =>
        val pendingWritesCount = state.pendingWrites
        val pendingEvictionsCount = pendingEvictions.foldLeft(0) {
          case (count, (_, pendingEviction)) =>
            val pending = if (selectorPending(pendingEviction).contains(id)) 1 else 0
            count + pending
        }
        if (pendingEvictionsCount != pendingWritesCount)
          throw IllegalConflictDetectionStateException(
            show"${lockableStatus.kind.unquoted} $id with $pendingEvictionsCount pending writes has $pendingWritesCount pending writes marked."
          )
      }

      pendingEvictions.foreach { case (rc, pendingEviction) =>
        selectorPending(pendingEviction).foreach { id =>
          if (!states.contains(id))
            throw IllegalConflictDetectionStateException(
              show"${lockableStatus.kind.unquoted} $id with pending writes by request $rc is not in memory."
            )
        }
      }
    }

    def assertEvictableStatesAreQueuedForEviction(): Unit = {
      val pendingEvictedStates = mapUnion(pendingEvictions.values)(selectorPending(_).toSet)
      states.foreach { case (id, state) =>
        if (state.safeToEvict && !pendingEvictedStates.contains(id))
          throw IllegalConflictDetectionStateException(
            show"${lockableStatus.kind.unquoted} $id is safe to evict, but will not be evicted"
          )
      }
    }

    def assertVersionedStateIsLatestIfNoPendingWrites(): Unit = {
      val withoutPendingWrites = states.filterNot { case (id, state) => state.hasPendingWrites }
      // Await on the store Futures to make sure that there's no context switch in the conflict detection thread
      // This ensures that invariant checking runs atomically.
      val storeSnapshot =
        timeouts.io.await(
          s"running fetchStatesForInvariantChecking with ${withoutPendingWrites.keys}"
        )(store.fetchStatesForInvariantChecking(withoutPendingWrites.keys))
      val pruningStatusO = timeouts.io.await("getting the pruning status")(store.pruningStatus)
      withoutPendingWrites.foreach { case (id, state) =>
        val storedState = storeSnapshot.get(id)
        state.versionedState.foreach { versionedState =>
          if (versionedState != storedState) {
            val mayHaveBeenPruned = pruningStatusO.exists(pruningTime =>
              versionedState.exists(vs =>
                vs.timestamp <= pruningTime.timestamp && vs.status.prunable
              )
            )
            if (!mayHaveBeenPruned || storedState.nonEmpty)
              throw IllegalConflictDetectionStateException(
                show"${lockableStatus.kind.unquoted} $id without pending writes has inconsistent states. Memory: ${state.versionedState}, store: $storedState, pruning status: $pruningStatusO"
              )
          }
        }
      }
    }

    assertPendingActivenessChecksMarked()
    assertLockedStatesArePrefetched()
    assertFreshStatesAreLockedAndHaveNoPendingWrites()
    assertStatesAreLockedTheCorrectNumberOfTimes()
    assertStatesWithPendingWritesAreMarkedAsPendingTheCorrectNumberOfTimes()
    assertEvictableStatesAreQueuedForEviction()
    assertVersionedStateIsLatestIfNoPendingWrites()
  }
}

private[conflictdetection] object LockableStates {
  def empty[K: Pretty: ClassTag, A <: PrettyPrinting with HasPrunable](
      store: ConflictDetectionStore[K, A],
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
      executionContext: ExecutionContext,
  )(implicit lockableStatus: LockableStatus[A]) =
    new LockableStates(store, loggerFactory, timeouts, executionContext)

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  final case class ConflictDetectionStoreAccessError(msg: String, cause: Throwable = null)
      extends RuntimeException(msg, cause)

  def withRC(rc: RequestCounter, msg: String) = s"Request $rc: $msg"

  /** Handle with references to all the [[MutableLockableState]]s that are needed for the activeness check.
    *
    * @param requestCounter The request counter to which this handle belongs to
    * @param availableF Completes if all states other than `toBeFetched` have been provided from the stores
    * @param lockCount Number of locks to acquire during the activeness check
    * @param fresh The list of items and their [[MutableLockableState]]s in [[LockableStates.states]] that are checked for being fresh.
    * @param free The list of items and their [[MutableLockableState]]s in [[LockableStates.states]] that are checked for being free.
    * @param active The list of items and their [[MutableLockableState]]s in [[LockableStates.states]] that are checked for being active.
    * @param lockOnly The list of items and their [[MutableLockableState]]s in [[LockableStates.states]] that are to be locked.
    * @param toBeFetchedM The items that must be fetched from the store for this request and their corresponding [[MutableLockableState]] in [[LockableStates.states]].
    * @param priorStates The items whose state prior to the activeness check shall be returned in the [[ActivenessCheckResult]].
    */
  class LockableStatesCheckHandle[Key, Status <: PrettyPrinting] private[LockableStates] (
      val requestCounter: RequestCounter,
      val availableF: Future[Unit],
      private[LockableStates] val lockCount: Int,
      private[LockableStates] val fresh: Seq[KeyStateLock[Key, Status]],
      private[LockableStates] val free: Seq[KeyStateLock[Key, Status]],
      private[LockableStates] val active: Seq[KeyStateLock[Key, Status]],
      private[LockableStates] val lockOnly: Seq[KeyStateLock[Key, Status]],
      private[LockableStates] val toBeFetchedM: mutable.Map[Key, MutableLockableState[Status]],
      private[LockableStates] val priorStates: Seq[KeyStateLock[Key, Status]],
  )(implicit
      val prettyK: Pretty[Key]
  ) extends PrettyPrinting {

    def toBeFetched: collection.Set[Key] = toBeFetchedM.keySet

    def noOutstandingFetches: Boolean = toBeFetchedM.isEmpty

    lazy val affected: Set[Key] =
      fresh.map(_.id).toSet ++ free.map(_.id).toSet ++ active.map(_.id).toSet ++ lockOnly
        .map(_.id)
        .toSet

    override def pretty: Pretty[LockableStatesCheckHandle.this.type] = prettyOfClass(
      param("request counter", _.requestCounter)
    )
  }

  /** @param id The ID of the item
    * @param state The reference to the [[MutableLockableState]] in [[LockableStates.states]] for this item
    * @param doLock Whether the activeness check shall obtain a lock on this item
    */
  private final case class KeyStateLock[Key, Status <: PrettyPrinting](
      id: Key,
      state: MutableLockableState[Status],
      doLock: Boolean,
  )
}
