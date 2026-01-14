// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.LtHash16
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.participant.pruning.{
  AcsCommitmentProcessor,
  CommitmentContractMetadata,
  SortedReconciliationIntervals,
  SortedReconciliationIntervalsHelpers,
}
import com.digitalasset.canton.protocol.messages.{AcsCommitment, CommitmentPeriod}
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

  protected def deployOnTwoParticipantsAndCheckContract(
      synchronizerId: SynchronizerId,
      iouContract: AtomicReference[Iou.Contract],
      firstParticipant: LocalParticipantReference,
      secondParticipant: LocalParticipantReference,
      observers: Seq[LocalParticipantReference] = Seq.empty,
  )(implicit
      env: TestConsoleEnvironment,
      traceContext: TraceContext,
  ): Iou.Contract = {
    import env.*

    logger.info(s"Deploying the iou contract on both participants")
    val iou = IouSyntax
      .createIou(firstParticipant, Some(synchronizerId))(
        firstParticipant.adminParty,
        secondParticipant.adminParty,
        observers = observers.toList.map(_.adminParty),
      )

    iouContract.set(iou)

    logger.info(s"Waiting for the participants to see the contract in their ACS")
    eventually() {
      (Seq(firstParticipant, secondParticipant) ++ observers).foreach(p =>
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

  def deployThreeAndCheck(
      synchronizerId: SynchronizerId,
      alreadyDeployedContracts: AtomicReference[Seq[Iou.Contract]],
      firstParticipant: LocalParticipantReference,
      secondParticipant: LocalParticipantReference,
  )(implicit
      env: TestConsoleEnvironment,
      intervalDuration: IntervalDuration,
  ): (Seq[Iou.Contract], CommitmentPeriod, AcsCommitment.HashedCommitmentType) =
    deployManyAndCheck(
      synchronizerId,
      PositiveInt.three,
      alreadyDeployedContracts,
      firstParticipant,
      secondParticipant,
    )

  def deployOneAndCheck(
      synchronizerId: SynchronizerId,
      alreadyDeployedContracts: AtomicReference[Seq[Iou.Contract]],
      firstParticipant: LocalParticipantReference,
      secondParticipant: LocalParticipantReference,
  )(implicit
      env: TestConsoleEnvironment,
      intervalDuration: IntervalDuration,
  ): (Seq[Iou.Contract], CommitmentPeriod, AcsCommitment.HashedCommitmentType) =
    deployManyAndCheck(
      synchronizerId,
      PositiveInt.one,
      alreadyDeployedContracts,
      firstParticipant,
      secondParticipant,
    )

  def deployManyAndCheck(
      synchronizerId: SynchronizerId,
      nContracts: PositiveInt,
      alreadyDeployedContracts: AtomicReference[Seq[Iou.Contract]],
      firstParticipant: LocalParticipantReference,
      secondParticipant: LocalParticipantReference,
  )(implicit
      env: TestConsoleEnvironment,
      intervalDuration: IntervalDuration,
  ): (Seq[Iou.Contract], CommitmentPeriod, AcsCommitment.HashedCommitmentType) = {
    import env.*

    val iouContract = new AtomicReference[Iou.Contract]

    val simClock = environment.simClock.value
    simClock.advanceTo(simClock.uniqueTime().immediateSuccessor)

    val createdCids =
      (1 to nContracts.value).map(_ =>
        deployOnTwoParticipantsAndCheckContract(
          synchronizerId,
          iouContract,
          firstParticipant,
          secondParticipant,
        )
      )

    val tick1 = tickAfter(simClock.uniqueTime())
    simClock.advanceTo(tick1.forgetRefinement.immediateSuccessor)
    // just fetch_synchronizer_times() is sometimes not enough to trigger commitment generation
    firstParticipant.health.ping(firstParticipant)

    firstParticipant.testing.fetch_synchronizer_times()

    val p1Computed = eventually() {
      val p1Computed = firstParticipant.commitments
        .computed(
          daName,
          tick1.toInstant.minusMillis(1),
          tick1.toInstant,
          Some(secondParticipant),
        )
      p1Computed.size shouldBe 1
      p1Computed
    }

    val (period, _participant, commitment) = p1Computed.loneElement

    alreadyDeployedContracts.set(alreadyDeployedContracts.get().concat(createdCids))
    (createdCids, period, commitment)
  }
}

object CommitmentTestUtil {
  def computeHashedCommitment(
      contracts: Seq[CommitmentContractMetadata]
  ): AcsCommitment.HashedCommitmentType = {
    val h = LtHash16()
    contracts.map(item => item.cid -> item.reassignmentCounter).toMap.foreach {
      case (cid, reassignmentCounter) =>
        AcsCommitmentProcessor.addContractToCommitmentDigest(h, cid, reassignmentCounter)
    }

    val commitment = h.getByteString()
    val sumHash = LtHash16()
    sumHash.add(commitment.toByteArray)

    AcsCommitment.hashCommitment(sumHash.getByteString())
  }
}
