// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.{RequestCounter, SequencerCounter}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.blocking

/** Allocates [[com.digitalasset.canton.RequestCounter]]s for the transaction processor. */
trait RequestCounterAllocator {

  /** Allocates the next request counter to the confirmation request with the given
    * [[com.digitalasset.canton.SequencerCounter]].
    *
    * The assigned request counters form a contiguous sequence that can be monotonically embedded
    * into the sequence of [[com.digitalasset.canton.SequencerCounter]]s.
    *
    * All calls must be sequential with increasing sequencer counters, although they can run in different threads.
    * So if [[allocateFor]] is called for two sequencer counters `sc1` and `sc2` with `sc1 < sc2`,
    * then returning from the call for `sc1` must happen before the call for `sc2`.
    *
    * Consecutive calls are idempotent. So if [[allocateFor]] is called for the same sequencer counter twice,
    * without another intervening call for a difference sequencer counter,
    * then the second call returns the same result as the first one.
    *
    * @return [[scala.None]] if the sequencer counter is before the clean replay starting point and request processing should be skipped
    * @throws java.lang.IllegalArgumentException if the values `Long.MaxValue` is used as a sequence counter
    * @throws java.lang.IllegalStateException if no request counter can be assigned
    *                                         because an earlier call assigned already a
    *                                         [[com.digitalasset.canton.RequestCounter]]
    *                                         to a higher [[com.digitalasset.canton.SequencerCounter]],
    *                                         or because all request counters have been exhausted.
    *                                         The request counter `Long.MaxValue` cannot be used.
    */
  def allocateFor(sc: SequencerCounter)(implicit traceContext: TraceContext): Option[RequestCounter]

  /** Skips the next request counter.
    *
    * This allows for "holes" in the mapping of sequencer to request counters allowing for request counters for which no
    * sequencer counters exist (for example "repair" requests made outside the realm of a sequencer).
    *
    * All calls must be sequential, and the only request counter that can be skipped is the "next" request counter that
    * would have been allocated by [[allocateFor]].
    *
    * @throws java.lang.IllegalArgumentException if the specified [[com.digitalasset.canton.RequestCounter]] is not the
    *                                            one that would otherwise be allocated next.
    * @throws java.lang.IllegalStateException if all request counters have been exhausted.
    *                                         The request counter `Long.MaxValue` cannot be used.
    */
  def skipRequestCounter(rc: RequestCounter)(implicit traceContext: TraceContext): Unit

  /** Peeks at what would be the next request counter to allocate. */
  @VisibleForTesting
  def peek: RequestCounter
}

/** Allocator for [[com.digitalasset.canton.RequestCounter]]s.
  *
  * This class is not thread safe.
  *
  * @param initRc The request counter to start from. Must not be `Long.MaxValue`.
  * @throws java.lang.IllegalArgumentException if `initRc` is `Long.MaxValue`.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class RequestCounterAllocatorImpl(
    initRc: RequestCounter,
    cleanReplaySequencerCounter: SequencerCounter,
    override val loggerFactory: NamedLoggerFactory,
) extends RequestCounterAllocator
    with NamedLogging {
  require(
    initRc.isNotMaxValue,
    s"Request counter ${RequestCounter.MaxValue} cannot be used.",
  )

  private var nextRc: RequestCounter = initRc
  private var boundSequenceCounter: SequencerCounter = cleanReplaySequencerCounter

  private val lock = new Object()
  private def withLock[A](body: => A): A = blocking { lock.synchronized { body } }

  override def allocateFor(
      sc: SequencerCounter
  )(implicit traceContext: TraceContext): Option[RequestCounter] = {
    if (sc < cleanReplaySequencerCounter) {
      logger.debug(
        s"Skipping request counter allocation for sequencer counter $sc because it preceded the clean replay starting point"
      )
      None
    } else {
      val allocatedRc = withLock {
        ErrorUtil.requireArgument(
          sc.isNotMaxValue,
          s"Sequencer counter ${SequencerCounter.MaxValue} cannot be used.",
        )
        ErrorUtil.requireState(
          nextRc.isNotMaxValue,
          s"No more request counters can be allocated because the request counters have reached ${RequestCounter.MaxValue - 1}.",
        )

        val boundSC = boundSequenceCounter
        if (sc == boundSC - 1) {
          logger.debug(
            s"Repeated call to allocate request counter for sequencer counter $sc. Re-used request counter ${nextRc - 1}"
          )
          nextRc - 1
        } else {
          ErrorUtil.requireState(
            sc >= boundSC,
            s"Cannot allocate request counter for confirmation request with counter $sc because a lower request counter has already been allocated to ${boundSC - 1}",
          )
          val rc = nextRc
          logger.debug(s"Allocating request counter $rc to sequencer counter $sc")
          boundSequenceCounter = sc + 1
          nextRc = rc + 1
          rc
        }
      }
      Some(allocatedRc)
    }
  }

  override def skipRequestCounter(rc: RequestCounter)(implicit traceContext: TraceContext): Unit =
    withLock {
      val nextRequestCounter = nextRc
      ErrorUtil.requireState(
        nextRequestCounter.isNotMaxValue,
        s"No more request counters can be allocated because the request counters have reached ${RequestCounter.MaxValue - 1}.",
      )
      ErrorUtil.requireArgument(
        rc == nextRequestCounter,
        s"Cannot skip request counter $rc other than the next request counter $nextRc",
      )

      logger.debug(s"Asked to skip request counter $rc. Setting nextRc to ${rc + 1}.")
      nextRc = rc + 1
    }

  override def peek: RequestCounter = withLock { nextRc }
}
