// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.participant.pruning.{
  SortedReconciliationIntervals,
  SortedReconciliationIntervalsHelpers,
}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import java.time.Duration as JDuration
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.DurationConverters.ScalaDurationOps

final case class IntervalDuration(interval: JDuration) extends AnyVal

trait CommitmentTestUtil extends BaseTest with SortedReconciliationIntervalsHelpers {
  // advance the time sufficiently past the topology transaction registration timeout,
  // so that the ticks to detect a timeout for topology submissions does not seep into the following tests
  protected def passTopologyRegistrationTimeout(
      environment: TestConsoleEnvironment
  )(implicit traceContext: TraceContext): Unit =
    environment.environment.simClock.value.advance(
      environment.participant1.config.topology.topologyTransactionRegistrationTimeout.asFiniteApproximation.toJava
    )

  protected def deployOnP1P2AndCheckContract(
      synchronizerId: SynchronizerId,
      iouContract: AtomicReference[Iou.Contract],
      observers: Seq[LocalParticipantReference] = Seq.empty,
  )(implicit
      env: TestConsoleEnvironment,
      traceContext: TraceContext,
  ): Iou.Contract = {
    import env.*

    logger.info(s"Deploying the iou contract on both participants")
    val iou = IouSyntax
      .createIou(participant1, Some(synchronizerId))(
        participant1.adminParty,
        participant2.adminParty,
        observers = observers.toList.map(_.adminParty),
      )

    iouContract.set(iou)

    logger.info(s"Waiting for the participants to see the contract in their ACS")
    eventually() {
      (Seq(participant1, participant2) ++ observers).foreach(p =>
        p.ledger_api.state.acs
          .of_all()
          .filter(_.contractId == iou.id.contractId) should not be empty
      )
    }

    iou
  }

  protected def tickBeforeOrAt(
      timestamp: CantonTimestamp
  )(implicit duration: IntervalDuration): CantonTimestampSecond =
    SortedReconciliationIntervals
      .create(
        Seq(mkParameters(CantonTimestamp.MinValue, duration.interval.getSeconds)),
        CantonTimestamp.MaxValue,
      )
      .value
      .tickBeforeOrAt(timestamp)
      .value

  protected def tickAfter(timestamp: CantonTimestamp)(implicit
      duration: IntervalDuration
  ): CantonTimestampSecond =
    tickBeforeOrAt(timestamp) + PositiveSeconds.tryOfSeconds(duration.interval.getSeconds)
}
