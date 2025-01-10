// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.memory.{
  InMemoryReassignmentStore,
  ReassignmentCache,
}
import com.digitalasset.canton.participant.store.{ActiveContractStore, ReassignmentStoreTest}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{BaseTest, HasExecutorService, RequestCounter, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

class NaiveRequestTrackerTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutorService
    with ConflictDetectionHelpers
    with RequestTrackerTest {

  private val clock: SimClock = new SimClock(loggerFactory = loggerFactory)

  def mk(
      sc: SequencerCounter,
      ts: CantonTimestamp,
      acs: ActiveContractStore,
  ): NaiveRequestTracker = {
    val reassignmentCache =
      new ReassignmentCache(
        new InMemoryReassignmentStore(
          ReassignmentStoreTest.targetSynchronizerId,
          loggerFactory,
        ),
        FutureSupervisor.Noop,
        timeouts,
        loggerFactory,
      )

    val conflictDetector =
      new ConflictDetector(
        acs,
        reassignmentCache,
        loggerFactory,
        checkedInvariant = true,
        parallelExecutionContext,
        exitOnFatalFailures = true,
        timeouts,
        futureSupervisor,
      )

    new NaiveRequestTracker(
      sc,
      ts,
      conflictDetector,
      ParticipantTestMetrics.synchronizer.conflictDetection,
      exitOnFatalFailures = false,
      timeouts,
      loggerFactory,
      FutureSupervisor.Noop,
      clock,
    )
  }

  "NaiveRequestTracker" should {
    behave like requestTracker(mk)
  }

  "requests are evicted when they are finalized" inUS {
    for {
      acs <- mkAcs()
      rt = mk(SequencerCounter(0), CantonTimestamp.MinValue, acs)
      (cdF, toF) <- enterCR(
        rt,
        RequestCounter(0),
        SequencerCounter(1),
        CantonTimestamp.ofEpochMilli(1),
        CantonTimestamp.ofEpochMilli(10),
        ActivenessSet.empty,
      )
      _ = assert(rt.requestInFlight(RequestCounter(0)), "Request present immediately after adding")
      _ = enterTick(rt, SequencerCounter(0), CantonTimestamp.Epoch)
      _ <- checkConflictResult(
        RequestCounter(0),
        cdF,
        ConflictDetectionHelpers.mkActivenessResult(),
      )
      _ = assert(
        rt.requestInFlight(RequestCounter(0)),
        "Request present immediately after conflict detection",
      )
      finalize0 <- enterTR(
        rt,
        RequestCounter(0),
        SequencerCounter(2),
        CantonTimestamp.ofEpochMilli(2),
        CommitSet.empty,
        1L,
        toF,
      )
      _ = assert(
        rt.requestInFlight(RequestCounter(0)),
        "Request present immediately after transaction result",
      )
      _ = enterTick(rt, SequencerCounter(3), CantonTimestamp.ofEpochMilli(3))
      _ <- finalize0.map(result => assert(result == Either.unit))
      _ = assert(
        !rt.requestInFlight(RequestCounter(0)),
        "Request evicted immediately after finalization",
      )
    } yield succeed
  }

  "requests are evicted when they time out" inUS {
    for {
      acs <- mkAcs()
      rt = mk(SequencerCounter(0), CantonTimestamp.Epoch, acs)
      (cdF, toF) <- enterCR(
        rt,
        RequestCounter(0),
        SequencerCounter(0),
        CantonTimestamp.ofEpochMilli(1),
        CantonTimestamp.ofEpochMilli(10),
        ActivenessSet.empty,
      )
      _ <- checkConflictResult(
        RequestCounter(0),
        cdF,
        ConflictDetectionHelpers.mkActivenessResult(),
      )
      _ = enterTick(rt, SequencerCounter(1), CantonTimestamp.ofEpochMilli(10))
      _ <- toF.map(timeout => assert(timeout.timedOut))
      _ = assert(
        !rt.requestInFlight(RequestCounter(0)),
        "Request evicted immediately after timeout",
      )
    } yield succeed
  }
}
