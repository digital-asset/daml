// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.SubmissionTracker.SubmissionData
import com.digitalasset.canton.participant.store.SubmissionTrackerStore
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** Tracker for submission, backed by a `SubmissionTrackerStore`.
  *
  * The purpose of this tracker is to detect replayed requests, and allow the participant to emit
  * a command completion only for genuine requests that it has submitted.
  *
  * A request R1 is considered fresh iff
  * it has the minimal requestId among all requests that have the same root hash,
  * for which SubmissionData has been provided.
  * In particular, for a given root hash there is at most one fresh request.
  *
  * The ScalaDocs of the individual methods prescribe when to call the methods.
  * Calling the methods in a different order will result in undefined behavior.
  * Failure to call either `cancelRegistration()` or `provideSubmissionData()` after calling `register()` for a request
  * may result in a deadlock.
  */
trait SubmissionTracker extends AutoCloseable {

  /** Register an ongoing transaction in the tracker.
    * This method must be called for every request id and with monotonically increasing request ids, i.e.,
    * in the synchronous part of Phase 3.
    * The return value should be used to determine whether to emit a command completion in Phase 7.
    * If the return value is `false`, the submitting participant of the underlying request should reject in Phase 3.
    *
    * @return a `Future` that represents the conditions:
    *         * the transaction is fresh, i.e. it is not a replay;
    *         * the transaction was submitted by this participant;
    *         * the transaction is timely, i.e. it was sequenced within its max sequencing time.
    */
  def register(rootHash: RootHash, requestId: RequestId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean]

  /** Cancel a previously registered submission and perform the necessary cleanup.
    * This method must be called for a request if and only if `provideSubmissionData` cannot be called.
    *
    * As a consequence, the associated `Future` returned by `register()` will be completed with `false`.
    */
  def cancelRegistration(rootHash: RootHash, requestId: RequestId)(implicit
      traceContext: TraceContext
  ): Unit

  /** Provide the submission data necessary to decide on transaction validity.
    */
  def provideSubmissionData(
      rootHash: RootHash,
      requestId: RequestId,
      submissionData: SubmissionData,
  )(implicit traceContext: TraceContext): Unit
}

object SubmissionTracker {
  final case class SubmissionData(
      submitterParticipant: ParticipantId,
      maxSequencingTimeO: Option[CantonTimestamp],
  )

  def apply(protocolVersion: ProtocolVersion)(
      participantId: ParticipantId,
      store: SubmissionTrackerStore,
      futureSupervisor: FutureSupervisor,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): SubmissionTracker =
    new SubmissionTrackerImpl(protocolVersion)(
      participantId,
      store,
      futureSupervisor,
      timeouts,
      loggerFactory,
    )
}

class SubmissionTrackerImpl private[protocol] (protocolVersion: ProtocolVersion)(
    participantId: ParticipantId,
    store: SubmissionTrackerStore,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends SubmissionTracker
    with FlagCloseableAsync
    with NamedLogging {
  // The incoming requests are stored in a map keyed by root hash and request ID when registered, so their associated
  // data can be retrieved in the later calls.
  //
  // The main replay detection relies on a store that keeps track of which request ID was actually associated to a
  // given root hash. When replays (or at least requests sharing the same root hash) come concurrently (that is, there
  // is more than one between phases 3 and 7), the situation becomes more complicated. The following describes the
  // mechanism to handle them.
  //
  // Requests that have the same root hash (potential replays) are linked together in a list following registration
  // order. This order represents the priority for each request sharing the same root hash to become the "real" request
  // that can effect the emission of a command completion.
  //
  // When a request is cancelled, it gives the opportunity to other requests registered later but sharing the same root
  // hash to be selected.
  //
  // When a request is provided with its submission data, and it is the "first in line" (that is either the first in the
  // linked list, or all the previous ones have been cancelled), the actual replay evaluation takes place for this
  // request. The slot is effectively taken, and all other requests registered later (for the same root hash) will be
  // denied the opportunity to be selected, whether they are cancelled or provided submission data.
  //
  // In other words: given a list of requests for a given root hash, ordered by sequencing time, only the first of these
  // which has not been cancelled will be evaluated and possibly result in the emission of a command completion.
  //
  // Behavior in relation to crash recovery:
  //
  //  * Crash recovery does not clean up the submission tracker store. Therefore, if a request with ID `ts` was marked
  //    as a fresh submission, it will be in the tracker store even if it is being reprocessed after crash recovery.
  //    The idempotence of the store ensures that the resulting `requestIsValidFUS` nevertheless returns `true`.
  //
  //  * Suppose that, after a crash, the store may contain a request with a later request ID than the current request
  //    (for the same root hash). This contradicts our deterministic reprocessing assumption: if the current request
  //    attempts to be entered into the store in the current crash epoch, then it also would have attempted to do so
  //    in the previous crash epoch and therefore the later request would not have gotten in.
  //
  //  * Repair requests do not modify request IDs or root hashes, only request counters. Therefore they cannot interfere
  //    with this logic here.

  /** Information about a registered request. These entries are linked together in order of registration on
    * the same root hash.
    *
    * @param prevFUS   Future that results in the availability status from the previous request
    * @param nextPUS   Promise that gives availability status for the next request when completed
    * @param resultPUS Promise that says whether this request requires a command completion when completed
    */
  private case class Entry(
      prevFUS: FutureUnlessShutdown[Boolean],
      nextPUS: PromiseUnlessShutdown[Boolean],
      resultPUS: PromiseUnlessShutdown[Boolean],
  )

  /** Data structure associated to a given root hash.
    *
    * @param tailFUS Future that results in the availability status from the tail request, i.e. the latest request
    *                that was registered. This is NOT equivalent to the request in `reqMap` with the highest
    *                request ID, because the tail request could already have been completed and been removed from `reqMap`.
    * @param reqMap  Map of request entries currently registered with this root hash (for quick retrieval)
    */
  private case class RequestList(
      tailFUS: FutureUnlessShutdown[Boolean],
      reqMap: NonEmpty[Map[RequestId, Entry]],
  )

  private val pendingRequests = TrieMap[RootHash, RequestList]()

  @VisibleForTesting
  private[protocol] def internalSize: Int = pendingRequests.size

  //   -----------------      -----------------      -----------------
  //   |     Req 1     |      |     Req 2     |      |     Req 3     |
  //   | prevF | nextP |----->| prevF | nextP |----->| prevF | nextP |
  //   -----------------      -----------------      ------------^----
  //                                                             |
  //                                                             |
  //             tail for the root hash H: ----(future of)-------/
  //
  // When a new request is registered for root hash H, it gets its `prevF` from the tail,
  // creates a new `nextP` and its future becomes the new tail.
  //
  // `prevF` tells a request whether the slot is available.
  // `nextP` is used by a request to tell the next in line whether the slot is available.

  override def register(rootHash: RootHash, requestId: RequestId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    performUnlessClosing(functionFullName) {
      val nextPUS = new PromiseUnlessShutdown[Boolean](
        s"available-for-next-$rootHash-$requestId",
        futureSupervisor,
      )
      val resultPUS =
        new PromiseUnlessShutdown[Boolean](s"result-$rootHash-$requestId", futureSupervisor)

      pendingRequests
        .updateWith(rootHash) {
          case Some(RequestList(tailFUS, reqMap)) =>
            val newEntry = Entry(tailFUS, nextPUS, resultPUS)
            Some(RequestList(nextPUS.futureUS, reqMap.updated(requestId, newEntry)))

          case None => // We are the first request for this root hash
            val newEntry = Entry(FutureUnlessShutdown.pure(true), nextPUS, resultPUS)
            Some(RequestList(nextPUS.futureUS, NonEmpty(Map, requestId -> newEntry)))
        }
        .discard

      resultPUS.futureUS
    }.onShutdown(FutureUnlessShutdown.abortedDueToShutdown)

  private def tryGetEntry(rootHash: RootHash, requestId: RequestId)(implicit
      traceContext: TraceContext
  ): Entry = {
    val RequestList(_, reqMap) = pendingRequests.getOrElse(
      rootHash,
      ErrorUtil.internalError(new InternalError(s"Unknown rootHash: $rootHash")),
    )
    reqMap.getOrElse(
      requestId,
      ErrorUtil.internalError(new InternalError(s"Unknown requestId: $requestId")),
    )
  }

  private def cleanup(rootHash: RootHash, requestId: RequestId)(implicit
      traceContext: TraceContext
  ): Unit = {
    pendingRequests
      .updateWith(rootHash) {
        case Some(RequestList(_, reqMap)) if reqMap.size == 1 =>
          // We are the last request for this root hash
          // Consistency check
          if (!reqMap.contains(requestId)) {
            ErrorUtil.internalError(
              new InternalError(
                s"Invalid state: requestId = $requestId, reqMap.keys = ${reqMap.keys}"
              )
            )
          }
          None

        case Some(RequestList(tailFUS, reqMap)) => // reqMap.size >= 2
          // Consistency check
          if (!reqMap.contains(requestId)) {
            ErrorUtil.internalError(
              new InternalError(
                s"Invalid state: requestId = $requestId, reqMap.keys = ${reqMap.keys}"
              )
            )
          }
          // We can use `fromUnsafe` as the size is guaranteed >= 2
          Some(RequestList(tailFUS, NonEmptyUtil.fromUnsafe(reqMap.removed(requestId))))

        case None =>
          ErrorUtil.internalError(new InternalError(s"Unknown rootHash: $rootHash"))
      }
      .discard
  }

  override def cancelRegistration(rootHash: RootHash, requestId: RequestId)(implicit
      traceContext: TraceContext
  ): Unit = {
    val Entry(prevFUS, nextPUS, resultPUS) = tryGetEntry(rootHash, requestId)

    // Fail this request
    resultPUS.outcome(false)

    // Propagate current availability
    nextPUS.completeWith(prevFUS.thereafter(_ => cleanup(rootHash, requestId)))
  }

  override def provideSubmissionData(
      rootHash: RootHash,
      requestId: RequestId,
      submissionData: SubmissionData,
  )(implicit traceContext: TraceContext): Unit = {
    val Entry(prevFUS, nextPUS, resultPUS) = tryGetEntry(rootHash, requestId)

    // No matter what, the slot is no longer available for further requests
    nextPUS.outcome(false)

    FutureUtil.doNotAwait(
      prevFUS.map { isAvailable =>
        if (isAvailable) {
          // The slot is available to us -- yay!
          val amSubmitter = submissionData.submitterParticipant == participantId

          val requestIsValidFUS = if (protocolVersion <= ProtocolVersion.v4) {
            // Replay mitigation was introduced in PV=5; before that, we fall back on the previous behavior
            FutureUnlessShutdown.pure(amSubmitter)
          } else {
            val maxSequencingTime = submissionData.maxSequencingTimeO.getOrElse(
              ErrorUtil.internalError(
                new InternalError(
                  s"maxSequencingTime in SubmissionData for PV > 4 must be defined"
                )
              )
            )
            if (amSubmitter && requestId.unwrap <= maxSequencingTime) {
              store.registerFreshRequest(rootHash, requestId, maxSequencingTime)
            } else {
              FutureUnlessShutdown.pure(false)
            }
          }

          resultPUS.completeWith(requestIsValidFUS.thereafter { _ =>
            // We can remove the tail of the chain if we are the last request, because at this point it
            // is guaranteed that the store has been updated. All further requests on this root hash which
            // are not cancelled will be blocked by the maxSequencingTime or the store.
            cleanup(rootHash, requestId)
          })
        } else {
          // The slot was already taken -- fail the request
          resultPUS.outcome(false)
          cleanup(rootHash, requestId)
        }
      }.unwrap,
      s"Propagating availability when validating $requestId failed",
    )
  }

  private def shutdown(): Unit = {
    pendingRequests.values.foreach { case RequestList(_, reqMap) =>
      reqMap.values.foreach { case Entry(_, nextPUS, resultPUS) =>
        nextPUS.shutdown()
        resultPUS.shutdown()
      }
    }
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    Seq(SyncCloseable("submission-tracker-promises", shutdown()))
}
