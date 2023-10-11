// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.OptionT
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.concurrent.{FutureSupervisor, SupervisedPromise}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  Counter,
  PeanoQueue,
  SynchronizedPeanoTreeQueue,
}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.repair.RepairContext
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.{Clean, Pending}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.store.CursorPrehead
import com.digitalasset.canton.store.CursorPrehead.RequestCounterCursorPrehead
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, HasFlushFuture, NoCopy}
import com.digitalasset.canton.{DiscardOps, RequestCounter, RequestCounterDiscriminator}
import com.google.common.annotations.VisibleForTesting

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}

/** The request journal records the committed [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState!]]
  * associated with particular requests.
  * The request journal is only written to by the [[com.digitalasset.canton.participant.protocol.ProtocolProcessor]]s.
  * In particular, reads of request journal state are used for maintaining consistency in reads from contract stores.
  * The request journal is also used for bookkeeping and recovery.
  * The only exception to the writing rule is the [[com.digitalasset.canton.participant.store.RequestJournalStore.prune]] method,
  * which may be user-triggered, though the call's pre-conditions must be respected.
  *
  * The request journal uses two strategies to persistently organize states:
  *
  * <ul>
  *   <li>For every request, which is identified by a participant-local request counter,
  *     the request journal records the [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState]]
  *     associated with the request counter.</li>
  *   <li>For [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestStateWithCursor]] states,
  *     a cursor tracks the head request for that state.</li>
  * </ul>
  *
  * The <strong>head request</strong> for a state value is a
  * [[com.digitalasset.canton.RequestCounter]]
  * defined as follows:
  *
  * <ul>
  *   <li>Normally, the least request (ordering by request counter)
  *     which has not yet reached or progressed past that state value.
  *     However, the actual head may lag behind arbitrarily because the head is not updated atomically with the request states.</li>
  *   <li>In the edge case where no such request exists in the journal,
  *     the head points to the first request counter that has not been added to the journal.</li>
  * </ul>
  * The <strong>prehead request</strong> is the request before the head request, or [[scala.None$]] if there is no such request.
  *
  * The request journal also stores the timestamp associated with the request.
  * The assumption is made that every request is associated with only one timestamp.
  * However, several requests may have the same timestamp.
  */
class RequestJournal(
    store: RequestJournalStore,
    metrics: SyncDomainMetrics,
    override protected val loggerFactory: NamedLoggerFactory,
    initRc: RequestCounter,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext, closeContext: CloseContext)
    extends RequestJournalReader
    with NamedLogging
    with HasFlushFuture {
  import RequestJournal.*

  /* The request journal implementation is interested only in the front of the PeanoQueues, not its head.
   * To avoid memory leaks, we therefore drain the queues whenever we insert a RequestCounter. */
  private val pendingCursor: PeanoQueue[RequestCounter, CursorInfo] =
    new SynchronizedPeanoTreeQueue[RequestCounterDiscriminator, CursorInfo](initRc)

  private val cleanCursor: PeanoQueue[RequestCounter, CursorInfo] =
    new SynchronizedPeanoTreeQueue[RequestCounterDiscriminator, CursorInfo](initRc)

  override def query(
      rc: RequestCounter
  )(implicit traceContext: TraceContext): OptionT[Future, RequestData] = {
    store.query(rc)
  }

  private val numDirtyRequests = new AtomicInteger(0)

  /** Yields the number of requests that are currently not in state clean.
    *
    * The number may be incorrect, if previous calls to `insert`, `transit`, or `terminate` have failed with an exception.
    * This can be tolerated, as the SyncDomain should be restarted after such an exception and that will
    * reset the request journal.
    */
  def numberOfDirtyRequests: Int = numDirtyRequests.get()

  /** Insert a new request into the request journal.
    * The insertion will become visible immediately.
    * The request has the initial state [[RequestJournal.RequestState.Pending]].
    *
    * Preconditions:
    * <ul>
    * <li>The request counter must not have been previously inserted.</li>
    * <li>The request counter must be at least the front value of `pendingCursor`.</li>
    * <li>The request counter must not be `Long.MaxValue`.</li>
    * </ul>
    *
    * @param rc The request counter for the request.
    * @param requestTimestamp The timestamp on the request message.
    * @return A Future that will terminate as soon as the request has been stored or fail if a precondition is violated.
    *         The future will itself contain a future that terminates as soon as the pending cursor reaches `rc`.
    */
  def insert(rc: RequestCounter, requestTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Future[Unit]] =
    for {
      _ <- Future {
        ErrorUtil.requireArgument(
          rc.isNotMaxValue,
          s"The request counter ${Counter.MaxValue} cannot be used.",
        )
        val pendingFront = pendingCursor.front
        if (rc < pendingFront)
          ErrorUtil.internalError(
            new IllegalArgumentException(
              s"Request counter $rc is below Pending's front value ${pendingCursor.front}"
            )
          )

        val alreadyInserted = pendingCursor.alreadyInserted(rc)
        ErrorUtil.requireArgument(
          !alreadyInserted,
          s"Request counter $rc has already been inserted to the ${this.getClass}",
        )
      }

      _ = logger.debug(withRc(rc, s"Inserting request into journal at $requestTimestamp..."))

      data = RequestData(rc, RequestState.Pending, requestTimestamp)
      _ <- store.insert(data)

      _ = incrementNumDirtyRequests()
      _ = logger.debug(s"The number of dirty requests is $numberOfDirtyRequests.")

      // Synchronously add the new entry to the cursor queue
      info = CursorInfo(requestTimestamp, Promise())
      _ = pendingCursor.insert(rc, info)

      // Asynchronously drain the cursors and update the clean head
      _ = addToFlushAndLogError(s"Update Pending cursor for request $rc")(drainPending)
    } yield info.signal.future

  private def incrementNumDirtyRequests(): Unit = {
    discard {
      numDirtyRequests.incrementAndGet()
    }
    metrics.numDirtyRequests.inc()
  }

  private def decrementNumDirtyRequests(): Unit = {
    discard {
      numDirtyRequests.decrementAndGet()
    }
    metrics.numDirtyRequests.dec()
  }

  /** Tells the request journal that the given request is ready to transition to the given state.
    *
    * The `oldState` and `newState` arguments are not strictly needed,
    * since the request journal always knows the old state and can always infer the new state.
    * The explicit argument should make the API behavior more clear and provide a cheap assertion to detect bugs.
    * (Currently, the only valid values are [[RequestJournal.RequestState.Pending]] and [[RequestJournal.RequestState.Confirmed]].)
    *
    * Preconditions:
    * <ul>
    * <li>The request counter `rc` is in this request journal with the timestamp `requestTimestamp` and has the state `oldState`.</li>
    * <li>The methods [[transit]], [[insert]], and [[terminate]] are not called concurrently.</li>
    * </ul>
    *
    * @param rc       The request counter to transit
    * @param requestTimestamp The timestamp assigned to the request counter
    * @param oldState The old state the request counter is currently in.
    * @param newState The new state the request counter transits to.
    *                 Cannot be used for [[RequestJournal.RequestState.Clean]];
    *                 use [[terminate]] instead to set the commit time.
    * @return a future that completes as soon as the `newState` has been persisted.
    *         If `newState` has a cursor, the future will itself contain a future that completes as soon as the cursor reaches `rc`.
    *         Yields a failed future if a precondition is violated.
    * @throws java.lang.IllegalArgumentException if `oldState` is not a predecessor state of `newState`
    */
  def transit(
      rc: RequestCounter,
      requestTimestamp: CantonTimestamp,
      oldState: RequestState,
      newState: NonterminalRequestState,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[Future[Unit]]] = {
    advanceTo(rc, requestTimestamp, oldState, newState, None)
  }

  /** Moves the given request to [[RequestJournal.RequestState.Clean]] and sets the commit time.
    * Does nothing if the request was already clean.
    *
    * The `oldState` argument is not strictly needed,
    * since the request journal always knows the old state and can always infer the new state.
    * The explicit argument should make the API behavior more clear and provide a cheap assertion to detect bugs.
    *
    * Preconditions:
    * <ul>
    * <li>The request counter `rc` is in this request journal with timestamp `requestTimestamp` and has the state `oldState`.</li>
    * <li>The commit time must be after or at the request time of the request.</li>
    * <li>The methods [[transit]], [[insert]], and [[terminate]] are not called concurrently.</li>
    * </ul>
    *
    * @param requestTimestamp The timestamp assigned to the request counter
    * @param oldState The old state the request counter is currently in.
    *                 (Currently, this can only be [[RequestJournal.RequestState.Confirmed]].
    * @return A future that completes as soon as the state change has been persisted
    *         or fails if a precondition is violated.
    *         The future itself contains a future that completes as soon as the clean cursor reaches `rc`.
    * @throws java.lang.IllegalArgumentException if `oldState` is not a predecessor state of `newState`
    */
  def terminate(
      rc: RequestCounter,
      requestTimestamp: CantonTimestamp,
      oldState: RequestState,
      commitTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Future[Unit]] =
    advanceTo(rc, requestTimestamp, oldState, Clean, Some(commitTime)).map(
      _.getOrElse( // Should not happen because Clean has a cursor
        ErrorUtil.internalError(
          new NoSuchElementException(s"Cursor future for request state $Clean")
        )
      )
    )

  private[this] def advanceTo(
      rc: RequestCounter,
      requestTimestamp: CantonTimestamp,
      oldState: RequestState,
      newState: RequestState,
      commitTime: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): Future[Option[Future[Unit]]] = {
    logger.debug(withRc(rc, s"Transitioning to state $newState"))
    ErrorUtil.requireArgument(
      newState.isSuccessorOf(oldState),
      withRc(rc, s"State $oldState is not a predecessor for $newState"),
    )

    def drainCursorsAndStoreNewCleanPrehead(): Future[Unit] = {
      newState
        .visitCursor {
          case RequestState.Pending => Some(drainPending)
          case RequestState.Clean => Some(drainClean)
        }
        .getOrElse(Future.unit)
    }

    def handleError(err: RequestJournalStoreError): Nothing = err match {
      case UnknownRequestCounter(requestCounter) =>
        ErrorUtil.internalError(
          new IllegalArgumentException(
            s"Cannot transit non-existing request with request counter $requestCounter"
          )
        )
      case CommitTimeBeforeRequestTime(requestCounter, requestTime, commitTime) =>
        ErrorUtil.internalError(
          new IllegalArgumentException(
            withRc(
              requestCounter,
              s"Commit time $commitTime must be at least the request timestamp $requestTime",
            )
          )
        )
      case InconsistentRequestState(requestCounter, storedState, expectedState) =>
        ErrorUtil.internalError(
          new ConcurrentModificationException(
            withRc(
              requestCounter,
              s"Concurrent request journal modification.\nPrevious state: $storedState\nExpected state: $expectedState",
            )
          )
        )
      case InconsistentRequestTimestamp(requestCounter, storedTimestamp, expectedTimestamp) =>
        ErrorUtil.internalError(
          new IllegalStateException(
            withRc(
              requestCounter,
              s"Inconsistent timestamps for request.\nStored: $storedTimestamp\nExpected: $expectedTimestamp",
            )
          )
        )
    }

    def updateCursors(): Option[Future[Unit]] = {
      logger.debug(withRc(rc, s"Transited to state $newState"))
      if (newState.hasCursor) {
        // Synchronously add the new entry to the cursor queue, if there is a cursor queue
        val info =
          CursorInfo(requestTimestamp, new SupervisedPromise[Unit]("cursor-info", futureSupervisor))
        newState.visitCursor {
          case Pending => Some(pendingCursor.insert(rc, info))
          case Clean => Some(cleanCursor.insert(rc, info))
        }.discard

        // Asynchronously drain the cursors and update the clean head
        addToFlushAndLogError(s"Update cursors for request $rc")(
          drainCursorsAndStoreNewCleanPrehead()
        )

        Some(info.signal.future)
      } else {
        None
      }
    }

    store
      .replace(rc, requestTimestamp, oldState, newState, commitTime)
      .fold(
        handleError,
        { _ =>
          if (newState == Clean) decrementNumDirtyRequests()
          updateCursors()
        },
      )
  }

  /** When the returned future completes, pending updates and deletions that have been initiated before the call
    * will be performed such that they are visible to subsequent queries.
    * Prevents accidental deletion of subsequent reinsertion due to pending deletes.
    */
  @VisibleForTesting
  def flush(): Future[Unit] = doFlush()

  /** Counts requests whose timestamps lie between the given timestamps (inclusive).
    *
    * @param start Count all requests after or at the given timestamp
    * @param end   Count all requests before or at the given timestamp; use None to impose no upper limit
    */
  @VisibleForTesting
  def size(start: CantonTimestamp = CantonTimestamp.Epoch, end: Option[CantonTimestamp] = None)(
      implicit traceContext: TraceContext
  ): Future[Int] =
    store.size(start, end)

  private def withRc(rc: RequestCounter, msg: String): String = s"Request $rc: $msg"

  private def drainPending(implicit traceContext: TraceContext): Future[Unit] = Future {
    val newPrehead = drain(pendingCursor)
    newPrehead.foreach { case (_prehead, completionPromise) => completionPromise.success(()) }
  }

  private def drainClean(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    for {
      newPrehead <- Future { drain(cleanCursor) }
      _ <- newPrehead.fold(Future.unit) { case (prehead, completionPromise) =>
        store.advancePreheadCleanTo(prehead).map { _ =>
          completionPromise.success(())
        }
      }
    } yield ()
  }

  /** Drains elements from the given [[PeanoQueue]] until no more elements can be polled.
    * The promises in the drained elements are fulfilled
    * to indicate that the head cursor reached the corresponding request.
    *
    * @return The new prehead request counter if there is a new one
    *         and a promise on which all cursor futures are waiting that get completed during the draining.
    *         This promise must be completed after the prehead has been persisted.
    */
  private def drain(
      pq: PeanoQueue[RequestCounter, CursorInfo]
  )(implicit traceContext: TraceContext): Option[(RequestCounterCursorPrehead, Promise[Unit])] = {
    val completionPromise = new SupervisedPromise[Unit]("drain", futureSupervisor)

    @tailrec def drain(
        currentPrehead: Option[RequestCounterCursorPrehead]
    ): Option[RequestCounterCursorPrehead] =
      pq.poll() match {
        case None => currentPrehead
        case Some((requestCounter, CursorInfo(requestTimestamp, promise))) =>
          promise.completeWith(completionPromise.future)
          drain(Some(CursorPrehead(requestCounter, requestTimestamp)))
      }

    drain(None).map(_ -> completionPromise)
  }
}

object RequestJournal {

  /** Enumeration for the states a confirmation request can be in while it is being processed.
    *
    * A confirmation request must transit the states in the order given by this enumeration.
    */
  sealed trait RequestState
      extends Ordered[RequestState]
      with Product
      with Serializable
      with PrettyPrinting {
    def index: Int

    override def compare(state: RequestState): Int = index - state.index

    /** Returns whether the request journal tracks the head of this state with a cursor. */
    def hasCursor: Boolean = false

    /** Returns whether this request state is a successor state of the `other` request state. */
    def isSuccessorOf(other: RequestState): Boolean = other.index == this.index - 1

    /** Returns the successor request state, if any, in the order of request states. */
    def next: Option[RequestState]

    /** Runs the given function if the request state has a cursor. */
    def visitCursor[A](f: RequestStateWithCursor => Option[A]): Option[A] = None

    // All implementations of this trait are case objects, so prettyOfObject does the right thing.
    override def pretty: Pretty[this.type] = prettyOfObject[this.type]
  }

  /** State of a confirmation request whose head value the request journal tracks with a cursor
    */
  sealed trait RequestStateWithCursor extends RequestState {
    override def hasCursor: Boolean = true
    override def visitCursor[A](f: RequestStateWithCursor => Option[A]): Option[A] = f(this)
  }

  sealed trait NonterminalRequestState extends RequestState {
    override def next: Option[RequestState] = Some(RequestState.states(index + 1))
  }

  object RequestState {

    /** In this state, the activated contracts have been marked as pending to the [[com.digitalasset.canton.participant.store.StoredContractManager]]. */
    case object Pending extends RequestStateWithCursor with NonterminalRequestState {
      override def index: Int = 0
    }

    /** This state is reached when the participant is ready to send a verdict
      * (confirmation, partial confirmation, timeout, or rejection).
      * After this state, the response is sent to the mediator.
      */
    case object Confirmed extends RequestState with NonterminalRequestState {
      override def index: Int = 1
    }

    /** This state is reached when the result of a request is fully committed and no further processing is required,
      * and the ACS is up to date with respect to this request.
      */
    case object Clean extends RequestStateWithCursor {
      override def index: Int = 2
      override def next: Option[RequestState] = None
    }

    private[protocol] val states: Array[RequestState] = Array(Pending, Confirmed, Clean)

    def apply(index: Int): Option[RequestState] =
      if (index >= 0 && states.lengthCompare(index) > 0) Some(states(index)) else None
  }

  /** Summarizes the data to be stored for a request in the request journal.
    *
    * @param rc The request counter to identify the request.
    * @param state The state the request is in.
    * @param requestTimestamp The timestamp on the request message
    * @param commitTime The commit time of the request. May be set only for state [[RequestState.Clean]].
    * @param repairContext The repair context to mark if the request originated from a repair command.
    * @throws java.lang.IllegalArgumentException if the `commitTime` is specified and `state` is not [[RequestState.Clean]].
    *                                            or if the `commitTime` is before the `requestTimestamp`
    */
  final case class RequestData(
      rc: RequestCounter,
      state: RequestState,
      requestTimestamp: CantonTimestamp,
      commitTime: Option[CantonTimestamp],
      repairContext: Option[RepairContext], // only populated for repair requests
  ) extends PrettyPrinting
      with NoCopy {

    require(
      commitTime.isEmpty == (state != RequestState.Clean),
      s"Request $rc: The commit time $commitTime must be set iff the request is clean, not for $state",
    )
    require(
      commitTime.forall(_ >= requestTimestamp),
      s"Request $rc: The commit time $commitTime must be at least the request timestamp $requestTimestamp",
    )

    override def pretty: Pretty[RequestData] = prettyOfClass(
      param("requestCounter", _.rc),
      param("state", _.state),
      param("requestTimestamp", _.requestTimestamp),
      paramIfDefined("commitTime", _.commitTime),
      paramIfDefined(name = "repairContext", _.repairContext),
    )

    /** Sets the `newState`, and the `commitTime` unless the commit time has previously been set.
      * @throws java.lang.IllegalArgumentException if the `commitTime` is set or has been set previously
      *                                            and the new state is not [[RequestState.Clean]],
      *                                            or if the `commitTime` is before [[requestTimestamp]].
      */
    def tryAdvance(newState: RequestState, commitTime: Option[CantonTimestamp]): RequestData =
      new RequestData(rc, newState, requestTimestamp, commitTime.orElse(commitTime), repairContext)
  }

  object RequestData {
    def apply(
        requestCounter: RequestCounter,
        state: NonterminalRequestState,
        requestTimestamp: CantonTimestamp,
        repairContext: Option[RepairContext] = None,
    ): RequestData =
      new RequestData(requestCounter, state, requestTimestamp, None, repairContext)

    def initial(requestCounter: RequestCounter, requestTimestamp: CantonTimestamp): RequestData =
      RequestData(requestCounter, RequestState.Pending, requestTimestamp)

    def clean(
        requestCounter: RequestCounter,
        requestTimestamp: CantonTimestamp,
        commitTime: CantonTimestamp,
        repairContext: Option[RepairContext] = None,
    ): RequestData =
      new RequestData(requestCounter, Clean, requestTimestamp, Some(commitTime), repairContext)
  }

  private final case class CursorInfo(timestamp: CantonTimestamp, signal: Promise[Unit])
}

trait RequestJournalReader {
  import RequestJournal.*

  /** Returns the [[RequestJournal.RequestData]] associated with the given request counter, if any.
    * Modifications done through the [[RequestJournal]] interface show up eventually, not necessarily immediately.
    */
  def query(rc: RequestCounter)(implicit traceContext: TraceContext): OptionT[Future, RequestData]
}
