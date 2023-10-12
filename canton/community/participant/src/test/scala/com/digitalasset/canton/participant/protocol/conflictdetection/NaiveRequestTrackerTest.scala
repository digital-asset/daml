// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.memory.{InMemoryTransferStore, TransferCache}
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  ContractKeyJournal,
  TransferStoreTest,
}
import com.digitalasset.canton.{BaseTest, HasExecutorService, RequestCounter, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

class NaiveRequestTrackerTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutorService
    with ConflictDetectionHelpers
    with RequestTrackerTest {

  def mk(
      rc: RequestCounter,
      sc: SequencerCounter,
      ts: CantonTimestamp,
      acs: ActiveContractStore,
      ckj: ContractKeyJournal,
  ): NaiveRequestTracker = {
    val transferCache =
      new TransferCache(
        new InMemoryTransferStore(TransferStoreTest.targetDomain, loggerFactory),
        loggerFactory,
      )

    val conflictDetector =
      new ConflictDetector(
        acs,
        ckj,
        transferCache,
        loggerFactory,
        checkedInvariant = true,
        parallelExecutionContext,
        timeouts,
        futureSupervisor,
        testedProtocolVersion,
      )

    new NaiveRequestTracker(
      sc,
      ts,
      conflictDetector,
      ParticipantTestMetrics.domain.conflictDetection,
      timeouts,
      loggerFactory,
      FutureSupervisor.Noop,
    )
  }

  "NaiveRequestTracker" should {
    behave like requestTracker(mk)
  }

  "requests are evicted when they are finalized" in {
    for {
      acs <- mkAcs()
      ckj <- mkCkj()
      rt = mk(RequestCounter(0), SequencerCounter(0), CantonTimestamp.MinValue, acs, ckj)
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
      _ <- finalize0.map(result => assert(result == Right(())))
      _ = assert(
        !rt.requestInFlight(RequestCounter(0)),
        "Request evicted immediately after finalization",
      )
    } yield succeed
  }

  "requests are evicted when they time out" in {
    for {
      acs <- mkAcs()
      ckj <- mkCkj()
      rt = mk(RequestCounter(0), SequencerCounter(0), CantonTimestamp.Epoch, acs, ckj)
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
