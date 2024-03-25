// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.conflictdetection.LockableState.{
  LockCounter,
  PendingActivenessCheckCounter,
  PendingWriteCounter,
}
import com.digitalasset.canton.participant.util.{StateChange, TimeOfChange}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.Ordered.orderingToOrdered
import scala.concurrent.{Future, Promise}

private[conflictdetection] trait LockableState[Status <: PrettyPrinting] extends PrettyPrinting {

  /** [[scala.None$]] for no pre-fetched state
    * [[scala.Some$]]`(`[[scala.None$]]`)` for the state where the underlying store has no entry.
    * [[scala.Some$]]`(`[[scala.Some$]]`(...))` for the actual state `...`.
    */
  def versionedState: Option[Option[StateChange[Status]]]
  def pendingActivenessChecks: PendingActivenessCheckCounter
  def lock: LockCounter
  def pendingWrites: PendingWriteCounter

  def hasPendingActivenessChecks: Boolean =
    !PendingActivenessCheckCounter.isEmpty(pendingActivenessChecks)

  def locked: Boolean = !LockCounter.isEmpty(lock)

  def hasPendingWrites: Boolean = !PendingWriteCounter.isEmpty(pendingWrites)

  override def pretty: Pretty[LockableState.this.type] = prettyOfClass(
    unnamedParamIfDefined(_.versionedState),
    param("pending activeness checks", _.pendingActivenessChecks.toString.unquoted),
    param("locks", _.lock.toString.unquoted),
    param("pending writes", _.pendingWrites.toString.unquoted),
  )
}

private[conflictdetection] object LockableState {

  sealed trait ConflictDetectionCounterModule[T] {
    def empty: T
    def isEmpty(counter: T): Boolean
    def isSaturated(counter: T): Boolean

    def acquire(counter: T): T
    def release(counter: T): T

    @VisibleForTesting private[conflictdetection] def assertFromInt(x: Int): T
  }

  private class ConflictDetectionCounterModuleImpl(kind: String)
      extends ConflictDetectionCounterModule[Int] {
    override def empty: Int = 0
    override def isEmpty(counter: Int): Boolean = counter == 0
    override def isSaturated(counter: Int): Boolean = counter == 0xffffffff

    override def acquire(counter: Int): Int =
      if (isSaturated(counter))
        throw IllegalConflictDetectionStateException(
          s"Cannot register another $kind due to overflow"
        )
      else
        counter + 1

    override def release(counter: Int): Int =
      if (isEmpty(counter))
        throw IllegalConflictDetectionStateException(
          s"No ${kind}s there to be completed or released."
        )
      else
        counter - 1

    override def assertFromInt(x: Int): Int = x
  }

  sealed abstract class ConflictDetectionCounters {
    type PendingActivenessCheckCounter <: Int
    val PendingActivenessCheckCounter: ConflictDetectionCounterModule[PendingActivenessCheckCounter]

    type LockCounter <: Int
    val LockCounter: ConflictDetectionCounterModule[LockCounter]

    type PendingWriteCounter <: Int
    val PendingWriteCounter: ConflictDetectionCounterModule[PendingWriteCounter]
  }

  private class ConflictDetectionCountersImpl extends ConflictDetectionCounters {
    override type PendingActivenessCheckCounter = Int
    override val PendingActivenessCheckCounter: ConflictDetectionCounterModule[Int] =
      new ConflictDetectionCounterModuleImpl("pending activeness check")

    override type LockCounter = Int
    override val LockCounter: ConflictDetectionCounterModule[Int] =
      new ConflictDetectionCounterModuleImpl("lock")

    override type PendingWriteCounter = Int
    override val PendingWriteCounter: ConflictDetectionCounterModule[Int] =
      new ConflictDetectionCounterModuleImpl("pending write")
  }

  val counters: ConflictDetectionCounters = new ConflictDetectionCountersImpl

  type PendingActivenessCheckCounter = counters.PendingActivenessCheckCounter
  val PendingActivenessCheckCounter: counters.PendingActivenessCheckCounter.type =
    counters.PendingActivenessCheckCounter

  type LockCounter = counters.LockCounter
  val LockCounter: counters.LockCounter.type = counters.LockCounter

  type PendingWriteCounter = counters.PendingWriteCounter
  val PendingWriteCounter: counters.PendingWriteCounter.type = counters.PendingWriteCounter

  private[this] def uintToString(i: Int): String = (i.toLong & 0xffffffffL).toString
}

private[conflictdetection] final case class ImmutableLockableState[Status <: PrettyPrinting](
    override val versionedState: Option[Option[StateChange[Status]]],
    override val pendingActivenessChecks: PendingActivenessCheckCounter,
    override val lock: LockCounter,
    override val pendingWrites: PendingWriteCounter,
) extends LockableState[Status]

/** A mutable lockable in-memory state with support for pre-fetching from a store
  *
  * @param initialState [[scala.None$]] for no pre-fetched state
  *                     [[scala.Some$]]`(`[[scala.None$]]`)` for the pre-fetched state where the underlying store has not entry.
  *                     [[scala.Some$]]`(`[[scala.Some$]]`(...))` for the pre-fetched state `...` from the underlying store.`
  */
private[conflictdetection] class MutableLockableState[Status <: PrettyPrinting](
    initialState: Option[Option[StateChange[Status]]]
) extends LockableState[Status]
    with PrettyPrinting {

  import LockableState.*

  /** [[scala.Right$]] if the underlying store has no more recent state than this
    * [[scala.Left$]] a promise that is completed after the state has been fetched from the store or a state was set.
    * The promise is completed with the corresponding state.
    *
    * This is an atomic reference rather than a plain `var`
    * because the [[DomainRouter]] reads this state without further synchronization.
    * Since the reference is mutated only by a single writer at a time,
    * we can use [[java.util.concurrent.atomic.AtomicReference.lazySet]] for updating the values in there.
    * The update will become visible to the [[DomainRouter]]
    * at the latest when the conflict detection thread jumps to another future
    * or a CAS inside of the [[scala.collection.concurrent.TrieMap]] happens as part of inserting or evicting a state.
    * See http://psy-lob-saw.blogspot.com/2012/12/atomiclazyset-is-performance-win-for.html
    */
  private val internalVersionedState
      : AtomicReference[Either[Promise[Option[StateChange[Status]]], Option[StateChange[Status]]]] =
    new AtomicReference(initialState.toRight(Promise[Option[StateChange[Status]]]()))

  /** The number of pending activeness checks.
    *
    * Access must ensure it is properly synchronized.
    * The [[ConflictDetector]] uses its [[com.digitalasset.canton.util.SimpleExecutionQueue]] for that.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var pendingActivenessChecksVar: PendingActivenessCheckCounter =
    PendingActivenessCheckCounter.empty

  /** The number of locks currently held.
    *
    * Access must ensure it is properly synchronized.
    * The [[ConflictDetector]] uses its [[com.digitalasset.canton.util.SimpleExecutionQueue]] for that.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var lockVar: LockCounter = LockCounter.empty

  /** The number of pending writes.
    *
    * Access must ensure it is properly synchronized.
    * The [[ConflictDetector]] uses its [[com.digitalasset.canton.util.SimpleExecutionQueue]] for that.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var pendingWritesVar: PendingWriteCounter = PendingWriteCounter.empty

  override def versionedState: Option[Option[StateChange[Status]]] =
    internalVersionedState.get.toOption
  override def pendingActivenessChecks: PendingActivenessCheckCounter = pendingActivenessChecksVar
  override def lock: LockCounter = lockVar
  override def pendingWrites: PendingWriteCounter = pendingWritesVar

  def safeToEvict(implicit stateLocking: LockableStatus[Status]): Boolean =
    PendingActivenessCheckCounter.isEmpty(pendingActivenessChecks) &&
      LockCounter.isEmpty(lock) &&
      PendingWriteCounter.isEmpty(pendingWrites) &&
      versionedState.forall(_.forall(state => stateLocking.shouldEvict(state.status)))

  /** If called concurrently with the other methods, the returned future may provide an outdated state. */
  private[conflictdetection] def approximateState: Future[Option[StateChange[Status]]] =
    internalVersionedState.get.fold(_.future, Future.successful)

  /** Registers one pending activeness checks.
    *
    * @return [[scala.None$]] if the state is unknown. The caller must fetch the latest state from the store
    *         and signal it with [[provideFetchedState]].
    *         [[scala.Some$]] a future that completes after the state has become available
    *         (via [[provideFetchedState]] or [[setStatusPendingWrite]])
    *
    * @throws IllegalConflictDetectionStateException if `2^32 - 1` pending activeness checks have already been registered.
    */
  def registerPendingActivenessCheck(): Option[Future[Option[StateChange[Status]]]] = {
    val ivs = internalVersionedState.get
    val pendingActiveness = !PendingActivenessCheckCounter.isEmpty(pendingActivenessChecksVar)
    pendingActivenessChecksVar = PendingActivenessCheckCounter.acquire(pendingActivenessChecksVar)
    ivs match {
      case Left(promise) =>
        // If there's already a pending activeness check for another request
        // then we don't need to fetch the state once more and can just wait for the other request providing it.
        if (pendingActiveness) Some(promise.future) else None
      case Right(state) => Some(Future.successful(state))
    }
  }

  /** Signals that `fetchedState` is the latest state in the store,
    * except for updates written to the store through the [[ConflictDetector]].
    * Signals to all pending activeness check callers that the state is now available
    * unless the state previously was available.
    *
    * Does not complete a pending activeness check.
    */
  def provideFetchedState(fetchedState: Option[StateChange[Status]]): Unit = {
    updateStateIfNew(fetchedState)
  }

  /** Completes one pending activeness check.
    *
    * @throws IllegalConflictDetectionStateException if all pending activeness checks have already been completed.
    */
  def completeActivenessCheck(): Unit = {
    pendingActivenessChecksVar = PendingActivenessCheckCounter.release(pendingActivenessChecksVar)
  }

  /** Obtains one lock on the status and returns whether the lock was free before.
    *
    * @throws IllegalConflictDetectionStateException if `2^32 - 1` locks have already been taken.
    */
  def obtainLock(): Boolean = {
    val isLocked = locked
    lockVar = LockCounter.acquire(lockVar)
    !isLocked
  }

  /** Releases one lock.
    *
    * @throws IllegalConflictDetectionStateException if no lock is held.
    */
  def unlock(): Unit = {
    lockVar = LockCounter.release(lockVar)
  }

  /** Set a new state for the contract with the update of the persistent store pending.
    *
    * @throws IllegalConflictDetectionStateException if no lock is held or there are already `2^32-1` pending writes.
    */
  def setStatusPendingWrite(newStatus: Status, toc: TimeOfChange): Unit = {
    unlock()
    pendingWritesVar = PendingWriteCounter.acquire(pendingWritesVar)
    updateStateIfNew(Some(StateChange(newStatus, toc)))
  }

  /** Signal that one of the contract's pending state changes has been written to the persistent store.
    *
    * @throws IllegalConflictDetectionStateException if there are no pending writes.
    */
  def signalWrite(): Unit = {
    pendingWritesVar = PendingWriteCounter.release(pendingWritesVar)
  }

  def snapshot: ImmutableLockableState[Status] =
    ImmutableLockableState[Status](
      versionedState,
      pendingActivenessChecksVar,
      lockVar,
      pendingWritesVar,
    )

  private[this] def updateStateIfNew(newState: Option[StateChange[Status]]): Unit = {
    // No need for a compare and swap because there is only a single writer.
    internalVersionedState.get match {
      case Left(promise) =>
        internalVersionedState.lazySet(Right(newState))
        promise.success(newState)
      case Right(None) => internalVersionedState.lazySet(Right(newState))
      case Right(Some(StateChange(_oldStatus, oldToc))) =>
        if (newState.exists(_.asOf >= oldToc)) internalVersionedState.lazySet(Right(newState))
    }
  }
}
