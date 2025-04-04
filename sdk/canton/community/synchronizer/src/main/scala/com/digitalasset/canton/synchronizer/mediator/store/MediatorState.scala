// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.store

import cats.data.OptionT
import cats.syntax.functor.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsHelper
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.synchronizer.mediator.{
  FinalizedResponse,
  ResponseAggregation,
  ResponseAggregator,
}
import com.digitalasset.canton.synchronizer.metrics.MediatorMetrics
import com.digitalasset.canton.time.{Clock, TimeAwaiter}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

/** Provides state management for messages received by the mediator. Non-finalized response
  * aggregations are kept in memory, such that in case of the node shutting down, they are lost but
  * the participants waiting for a transaction result will simply timeout. The finalized response
  * aggregations are stored in the provided [[FinalizedResponseStore]]. It is expected that
  * `fetchPendingRequestIdsBefore` operation is not called concurrently with operations to modify
  * the pending requests.
  */
private[mediator] class MediatorState(
    val finalizedResponseStore: FinalizedResponseStore,
    val deduplicationStore: MediatorDeduplicationStore,
    val clock: Clock,
    val metrics: MediatorMetrics,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends MediatorStateInitialization
    with NamedLogging
    with FlagCloseable {

  // outstanding requests are kept in memory while finalized requests will be stored
  // a skip list is used to optimise for when we fetch all keys below a given timestamp
  private val pendingRequests =
    new ConcurrentSkipListMap[RequestId, ResponseAggregation[?]](implicitly[Ordering[RequestId]])

  // tracks the highest record time that is safe to query for verdicts, in case there are no pending requests
  // consider the following scenario:
  // 1. pending requests: t0, t1, t2
  // 2. t1 and t2 get finalized before t0
  // 3. once t0 gets finalized, we want to signal that the verdicts for all requests up to including t2 are now
  //    save to load from the store without potentially messing up the order
  @VisibleForTesting
  private[canton] val youngestFinalizedRequest = new AtomicReference(CantonTimestamp.MinValue)

  // returns either the predecessor of the oldest pending request (because the request itself is not yet finalized),
  // or the youngest finalized request in case there are no pending requests.
  private def oldestPendingOrYoungestFinalized: CantonTimestamp =
    Option(pendingRequests.ceilingKey(RequestId(CantonTimestamp.MinValue)))
      // the oldest request itself is still pending, therefore we can only query up to the predecessor
      .map(_.unwrap.immediatePredecessor)
      .getOrElse(youngestFinalizedRequest.get())

  // the TimeAwaiter that keeps track of recorder order
  val recordOrderTimeAwaiter =
    new TimeAwaiter(() => oldestPendingOrYoungestFinalized, timeouts, loggerFactory)

  private def updateNumRequests(num: Int): Unit =
    metrics.outstanding.updateValue(_ + num)

  // Initialize the request tracking with
  override protected def doInitialize(firstEventTs: CantonTimestamp)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] =
    for {
      recordTimeO <- finalizedResponseStore.highestRecordTime()
    } yield {
      val highestValidTimestampBeforeReplayEvent =
        if (firstEventTs == CantonTimestamp.MinValue) firstEventTs
        // replay may start at firstEventTs, therefore we consider firstEventTs' predecessor to be the highest
        // valid timestamp
        else firstEventTs.immediatePredecessor
      youngestFinalizedRequest.set(
        // use the lowest known event timestamp as a safe timestamp up to which we can safely serve verdicts.
        // consider the following timeline: (rx = request #x, vx = verdict #x, tx = sequencing time at timestamp x)
        //    t1 t2 t3 t4
        //   ──┼──┼──┼──┼─>
        //    r1 r2 v2 v1
        // scenario 1: highest record time in store is lower than the timestamp of the first replayed event.
        //    timestamp of first replayed event: t4
        //    highest record time in store: t2 (from r2)
        //    We know that any verdict coming for requests lower than t2 will be just ignored, because mediator doesn't do
        //    crash recovery. Only new requests with a request timestamp (aka record time) >= t4 will be registered and
        //    properly processed.
        //
        // scenario 2: highest record time in store is higher than the record time of the first replayed event.
        //    timestamp of first replayed event: t1
        //    highest record time in store: t2 (from r2)
        //    Since we replay events from before the highest record time, it could be (like in this example) that the
        //    replayed events contain requests with a lower record time. Since we replay these requests, they will be properly
        //    processed and the emission of the highest record time must wait for the earlier record times to be processed.
        recordTimeO
          .map(_.min(highestValidTimestampBeforeReplayEvent))
          // if no timestamp was found in the finalized response store, then we can just use the timestamp based on the replay
          // event as the highest record time that is safe to read from the store
          .getOrElse(highestValidTimestampBeforeReplayEvent)
      )
    }

  /** Adds an incoming ResponseAggregation */
  def add(
      responseAggregation: ResponseAggregation[?]
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] = {
    requireInitialized()
    responseAggregation.asFinalized(protocolVersion) match {
      case None =>
        val requestId = responseAggregation.requestId
        ErrorUtil.requireState(
          Option(pendingRequests.putIfAbsent(requestId, responseAggregation)).isEmpty,
          s"Unexpected pre-existing request for $requestId",
        )

        metrics.requests.mark()
        updateNumRequests(1)
        FutureUnlessShutdown.unit
      case Some(finalizedResponse) => add(finalizedResponse)
    }
  }

  def add(
      finalizedResponse: FinalizedResponse
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] = {
    requireInitialized()
    finalizedResponseStore
      .store(finalizedResponse)
      .map(_ => checkAndPublishNewRecordTime(finalizedResponse.requestId))
  }

  def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): OptionT[FutureUnlessShutdown, ResponseAggregator] =
    Option(pendingRequests.get(requestId)) match {
      case Some(response) => OptionT.pure[FutureUnlessShutdown](response)
      case None => finalizedResponseStore.fetch(requestId).widen[ResponseAggregator]
    }

  /** Replaces a [[ResponseAggregation]] for the `requestId` if the stored version matches
    * `currentVersion`. You can only use this to update non-finalized aggregations
    *
    * @return
    *   Whether the replacement was successful
    */
  def replace(oldValue: ResponseAggregator, newValue: ResponseAggregation[?])(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Boolean] = {
    requireInitialized()
    ErrorUtil.requireArgument(
      oldValue.requestId == newValue.requestId,
      s"RequestId ${oldValue.requestId} cannot be replaced with ${newValue.requestId}",
    )
    ErrorUtil.requireArgument(!oldValue.isFinalized, s"Already finalized ${oldValue.requestId}")

    val requestId = oldValue.requestId

    def storeFinalized(finalizedResponse: FinalizedResponse): FutureUnlessShutdown[Unit] =
      finalizedResponseStore.store(finalizedResponse) map { _ =>
        Option(pendingRequests.remove(requestId)) foreach { _ =>
          updateNumRequests(-1)
        }
        checkAndPublishNewRecordTime(requestId)
      }

    (for {
      // I'm not really sure about these validations or errors...
      currentValue <- OptionT.fromOption[FutureUnlessShutdown](
        Option(pendingRequests.get(requestId)).orElse {
          MediatorError.InternalError
            .Reject(
              s"Request $requestId has unexpectedly disappeared (expected version: ${oldValue.version}, new version: ${newValue.version})."
            )
            .log()
          None
        }
      )
      _ <-
        if (currentValue.version == oldValue.version) OptionT.some[FutureUnlessShutdown](())
        else {
          MediatorError.InternalError
            .Reject(
              s"Request $requestId has an unexpected version ${currentValue.version} (expected version: ${oldValue.version}, new version: ${newValue.version})."
            )
            .log()
          OptionT.none[FutureUnlessShutdown, Unit]
        }
      _ <- OptionT.liftF[FutureUnlessShutdown, Unit] {
        newValue.asFinalized(protocolVersion) match {
          case None =>
            pendingRequests.put(requestId, newValue)
            FutureUnlessShutdown.unit
          case Some(finalizedResponse) => storeFinalized(finalizedResponse)
        }
      }
    } yield true).getOrElse(false)
  }

  private def checkAndPublishNewRecordTime(
      requestId: RequestId
  )(implicit traceContext: TraceContext): Unit = {
    // remember the timestamp if it's the youngest
    youngestFinalizedRequest.updateAndGet(_.max(requestId.unwrap)).discard
    // if there are no more pending requests before this requestId,
    if (pendingRequestIdsBefore(requestId.unwrap).isEmpty) {
      recordOrderTimeAwaiter.notifyAwaitedFutures(oldestPendingOrYoungestFinalized)
    }
  }

  /** Fetch pending requests that have a timestamp below the provided `cutoff` */
  def pendingRequestIdsBefore(cutoff: CantonTimestamp): List[RequestId] =
    pendingRequests.keySet().headSet(RequestId(cutoff)).asScala.toList

  /** Fetch pending requests that have a timeout below the provided `cutoff` */
  def pendingTimedoutRequest(cutoff: CantonTimestamp): List[RequestId] =
    pendingRequests
      .values()
      .asScala
      .filter(resp => resp.timeout < cutoff)
      .map(resp => resp.requestId)
      .toList

  /** Fetch a response aggregation from the pending requests collection. */
  def getPending(requestId: RequestId): Option[ResponseAggregation[?]] = Option(
    pendingRequests.get(requestId)
  )

  /** Prune unnecessary data from before and including the given timestamp which should be
    * calculated by the [[Mediator]] based on the confirmation response timeout synchronizer
    * parameters. In practice a much larger retention period may be kept.
    *
    * Also updates the current age of the oldest finalized response after pruning.
    */
  def prune(pruneRequestsBeforeAndIncludingTs: CantonTimestamp)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] = finalizedResponseStore.prune(pruneRequestsBeforeAndIncludingTs)

  /** Locate the timestamp of the finalized response at or, if skip > 0, near the beginning of the
    * sequence of finalized responses.
    *
    * If skip == 0, returns the timestamp of the oldest, unpruned finalized response.
    */
  def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = for {
    ts <- finalizedResponseStore.locatePruningTimestamp(skip.value)
    _ = if (skip.value == 0) MetricsHelper.updateAgeInHoursGauge(clock, metrics.maxEventAge, ts)
  } yield ts

  /** Report the max-event-age metric based on the oldest response timestamp and the current clock
    * time or zero if no oldest timestamp exists (e.g. mediator fully pruned).
    */
  def reportMaxResponseAgeMetric(oldestResponseTimestamp: Option[CantonTimestamp]): Unit =
    MetricsHelper.updateAgeInHoursGauge(clock, metrics.maxEventAge, oldestResponseTimestamp)

  override def onClosed(): Unit =
    LifeCycle.close(
      // deduplicationStore, Not closed on purpose, because the deduplication store methods all receive their close context directly from the caller
      finalizedResponseStore,
      recordOrderTimeAwaiter,
    )(logger)
}
