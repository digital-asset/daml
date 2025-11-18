// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.participant.admin.data.{
  FlagNotSet,
  FlagSet,
  PartyOnboardingFlagStatus,
}
import com.digitalasset.canton.participant.sync.{ConnectedSynchronizer, SyncEphemeralState}
import com.digitalasset.canton.time.SynchronizerTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreTestData
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.mockito.ArgumentMatchers.eq as isEq
import org.mockito.MockitoSugar
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

class PartyOnboardingClearanceSchedulerTest
    extends AnyWordSpec
    with BaseTest
    with MockitoSugar
    with HasExecutionContext {

  /** A fake implementation of the workflow for stable testing. */
  private class FakePartyReplicationTopologyWorkflow(loggerFactory: NamedLoggerFactory)
      extends PartyReplicationTopologyWorkflow(
        participantId,
        ProcessingTimeout(),
        loggerFactory,
      ) {

    // Queue of predefined responses (success or failure) for the fake workflow.
    private val responses = new mutable.Queue[Either[String, PartyOnboardingFlagStatus]]()

    // Records the arguments passed to `authorizeOnboardedTopology` for assertion.
    val calls = new ConcurrentLinkedQueue[(PartyId, CantonTimestamp)]

    def addResponse(response: PartyOnboardingFlagStatus): Unit =
      responses.enqueue(Right(response))

    def addFailure(error: String): Unit = responses.enqueue(Left(error))

    override def authorizeOnboardedTopology(
        partyId: PartyId,
        targetParticipantId: ParticipantId,
        onboardingEffectiveAt: CantonTimestamp,
        connectedSynchronizer: ConnectedSynchronizer,
        requestId: Option[Hash] = None,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, PartyOnboardingFlagStatus] = {
      calls.add((partyId, onboardingEffectiveAt))
      val result = if (responses.isEmpty) {
        EitherT.leftT[FutureUnlessShutdown, PartyOnboardingFlagStatus](
          "FakePartyReplicationTopologyWorkflow has no more responses"
        )
      } else {
        EitherT.fromEither[FutureUnlessShutdown](responses.dequeue())
      }
      result
    }
  }

  // Mock synchronizer, provides access to ephemeral state and PSId.
  private val mockConnectedSynchronizer = mock[ConnectedSynchronizer]
  // Mock ephemeral state, provides access to the time tracker.
  private val mockEphemeralState = mock[SyncEphemeralState]
  // Mock time tracker, used to simulate time progression via `awaitTick`.
  private val mockTimeTracker = mock[SynchronizerTimeTracker]

  // Helper for generating standard topology test data (parties, participants, keys).
  private lazy val testData = new TopologyStoreTestData(
    testedProtocolVersion,
    loggerFactory,
    this.directExecutionContext,
  )
  private lazy val partyId = testData.party1
  private lazy val participantId = testData.p1Id
  private lazy val psid = testData.synchronizer1_p1p2_physicalSynchronizerId
  private val earliestClearanceTime = CantonTimestamp.now()
  private val onboardingEffectiveAt = CantonTimestamp.Epoch

  // A sample task, used for comparing against the scheduler's pending clearances.
  private lazy val task = OnboardingClearanceTask(
    partyId = partyId,
    earliestClearanceTime = earliestClearanceTime,
    onboardingEffectiveAt = onboardingEffectiveAt,
  )

  private def setup(): (FakePartyReplicationTopologyWorkflow, OnboardingClearanceScheduler) = {
    val fakeWorkflow = new FakePartyReplicationTopologyWorkflow(loggerFactory)
    // The scheduler instance under test.
    val scheduler =
      new OnboardingClearanceScheduler(
        participantId,
        psid,
        () => Some(mockConnectedSynchronizer),
        loggerFactory,
        fakeWorkflow,
      )
    (fakeWorkflow, scheduler)
  }

  override def withFixture(test: NoArgTest) = {
    reset(mockTimeTracker, mockConnectedSynchronizer, mockEphemeralState)
    when(mockConnectedSynchronizer.ephemeral).thenReturn(mockEphemeralState)
    when(mockEphemeralState.timeTracker).thenReturn(mockTimeTracker)
    when(mockConnectedSynchronizer.psid).thenReturn(psid)
    super.withFixture(test)
  }

  "PartyOnboardingClearanceScheduler" when {

    "requestOnboardingFlagClearance is called" should {

      "fail if the synchronizer PSId mismatches" in {
        val (fakeWorkflow, _) = setup()
        // A mock synchronizer configured with an incorrect PSId.
        val mockWrongSync = mock[ConnectedSynchronizer]
        // A different PSId used to trigger the mismatch error.
        val wrongPsid = DefaultTestIdentities.physicalSynchronizerId
        when(mockWrongSync.psid).thenReturn(wrongPsid)

        // Create a scheduler that provides the *wrong* synchronizer
        val scheduler = new OnboardingClearanceScheduler(
          participantId,
          psid,
          () => Some(mockWrongSync), // Provider returns the wrong sync
          loggerFactory,
          fakeWorkflow,
        )

        val error = scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .leftOrFailShutdown("Request should have failed due to PSId mismatch")
          .futureValue

        error should include(s"PSId mismatch: Expected $psid, got $wrongPsid")
      }

      "fail if the synchronizer provider returns None" in {
        val (fakeWorkflow, _) = setup()

        // Create a scheduler whose provider returns None
        val scheduler = new OnboardingClearanceScheduler(
          participantId,
          psid,
          () => None, // <-- Provider returns None
          loggerFactory,
          fakeWorkflow,
        )

        val error = scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .leftOrFailShutdown("Request should have failed as synchronizer provider returned None")
          .futureValue

        error shouldBe s"Synchronizer connection is not ready (absent): Onboarding flag clearance request for $psid (party: $partyId, participant: $participantId)."
      }

      "not schedule a task if the flag is already FlagNotSet" in {
        val (fakeWorkflow, scheduler) = setup()
        fakeWorkflow.addResponse(FlagNotSet)

        scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .valueOrFailShutdown("Initial check failed")
          .map { result =>
            result shouldBe FlagNotSet
            fakeWorkflow.calls.size shouldBe 1
            scheduler.pendingClearances.size shouldBe 0
            verify(mockTimeTracker, never).awaitTick(any[CantonTimestamp])(any[TraceContext])
            succeed
          }
      }

      "schedule a task only once for the same party" in {
        val (fakeWorkflow, scheduler) = setup()
        val tickPromise = Promise[Unit]()
        when(mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
          .thenReturn(Some(tickPromise.future))

        val flagSet = FlagSet(earliestClearanceTime)
        fakeWorkflow.addResponse(flagSet)
        fakeWorkflow.addResponse(flagSet)

        val f1 = scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .value
        val f2 = scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .value

        FutureUnlessShutdown
          .sequence(Seq(f1, f2))
          .map { results =>
            results.foreach(_ shouldBe Right(flagSet))
            scheduler.pendingClearances.size shouldBe 1
            scheduler.pendingClearances(partyId) shouldBe task
            verify(mockTimeTracker, times(1)).awaitTick(any[CantonTimestamp])(any[TraceContext])
            fakeWorkflow.calls.size shouldBe 2
            succeed
          }
          .failOnShutdown
      }

      "handle concurrent requests correctly" in {
        val (fakeWorkflow, scheduler) = setup()
        val tickPromise = Promise[Unit]()
        when(mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
          .thenReturn(Some(tickPromise.future))

        val flagSet = FlagSet(earliestClearanceTime)
        (1 to 10).foreach(_ => fakeWorkflow.addResponse(flagSet))

        val allCalls = Future
          .traverse((1 to 10).toList) { _ =>
            scheduler
              .requestClearance(
                partyId,
                onboardingEffectiveAt,
              )
              .value
              .unwrap
          }

        allCalls
          .map { _ =>
            scheduler.pendingClearances.size shouldBe 1
            verify(mockTimeTracker, times(1)).awaitTick(any[CantonTimestamp])(any[TraceContext])
            fakeWorkflow.calls.size shouldBe 10
          }
      }

      "propose a transaction when the trigger fires" in {
        val (fakeWorkflow, scheduler) = setup()
        val tickPromise = Promise[Unit]()

        when(
          mockTimeTracker.awaitTick(
            isEq(task.earliestClearanceTime.immediateSuccessor)
          )(
            any[TraceContext]
          )
        )
          .thenReturn(Some(tickPromise.future))

        fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))
        fakeWorkflow.addResponse(FlagNotSet)

        scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .valueOrFailShutdown("make the initial request")
          .flatMap { firstResult =>
            firstResult shouldBe FlagSet(earliestClearanceTime)
            scheduler.pendingClearances.size shouldBe 1
            fakeWorkflow.calls.size shouldBe 1

            tickPromise.success(())

            eventuallyAsync() {
              fakeWorkflow.calls.size shouldBe 2
            }.unwrap.map { _ =>
              fakeWorkflow.calls.poll() shouldBe ((partyId, onboardingEffectiveAt))
              scheduler.pendingClearances.size shouldBe 1
              verify(mockTimeTracker, times(1))
                .awaitTick(isEq(task.earliestClearanceTime.immediateSuccessor))(any[TraceContext])
              succeed
            }
          }
      }

      "log an error if the triggered proposal fails" in {
        val (fakeWorkflow, scheduler) = setup()
        val tickPromise = Promise[Unit]()

        when(mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
          .thenReturn(Some(tickPromise.future))

        fakeWorkflow.addResponse(FlagSet(earliestClearanceTime))
        fakeWorkflow.addFailure("Topology workflow failed")

        scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .valueOrFailShutdown("Initial onboarding request failed")
          .flatMap { firstResult =>
            firstResult shouldBe FlagSet(earliestClearanceTime)
            scheduler.pendingClearances.size shouldBe 1
            fakeWorkflow.calls.size shouldBe 1

            loggerFactory.suppress(SuppressionRule.Level(Level.ERROR)) {
              tickPromise.success(())

              eventuallyAsync() {
                val logs = loggerFactory.fetchRecordedLogEntries
                logs.loneElement.errorMessage should include(
                  s"Onboarding flag clearance proposal for party $partyId failed: Topology workflow failed"
                )
              }.unwrap
            }
          }
          .map { assertionAsUnlessShutdown =>
            assertionAsUnlessShutdown shouldBe UnlessShutdown.Outcome(succeed)
            fakeWorkflow.calls.size shouldBe 2
            scheduler.pendingClearances.size shouldBe 1
            succeed
          }
      }
    }

    "observing transactions" should {
      // Helper for creating "Replace" transactions (onboarding/clearance)
      def createTx(
          party: PartyId,
          participant: ParticipantId,
          onboarding: Boolean,
      ): SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant] = {
        val mapping = PartyToParticipant
          .tryCreate(
            partyId = party,
            threshold = PositiveInt.one,
            participants =
              Seq(HostingParticipant(participant, ParticipantPermission.Submission, onboarding)),
          )
        testData.makeSignedTx(mapping, isProposal = false)(testData.p1Key, testData.p2Key)
      }

      // NEW HELPER: Helper for creating "Remove" transactions (offboarding)
      def createRemoveTx(
          party: PartyId,
          participant: ParticipantId,
      ): SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant] = {
        val mapping = PartyToParticipant
          .tryCreate(
            partyId = party,
            threshold = PositiveInt.one,
            participants =
              Seq(HostingParticipant(participant, ParticipantPermission.Submission, false)),
          )
        // Create a Remove transaction
        testData.makeSignedTx(mapping, TopologyChangeOp.Remove, isProposal = false)(
          testData.p1Key,
          testData.p2Key,
        )
      }

      "remove a pending task if clearance is effective" in {
        val (_, scheduler) = setup()
        scheduler.pendingClearances.put(task.partyId, task)
        scheduler.pendingClearances.size shouldBe 1

        val clearanceTx = createTx(partyId, participantId, onboarding = false)

        scheduler
          .observed(
            SequencedTime(CantonTimestamp.now()),
            EffectiveTime(CantonTimestamp.now()),
            SequencerCounter.One,
            Seq(clearanceTx),
          )
          .futureValueUS

        scheduler.pendingClearances.size shouldBe 0
        succeed
      }

      "not remove a task if the onboarding flag is still true" in {
        val (_, scheduler) = setup()
        scheduler.pendingClearances.put(task.partyId, task)
        val clearanceTx = createTx(partyId, participantId, onboarding = true)

        scheduler
          .observed(
            SequencedTime(CantonTimestamp.now()),
            EffectiveTime(CantonTimestamp.now()),
            SequencerCounter.One,
            Seq(clearanceTx),
          )
          .futureValueUS

        scheduler.pendingClearances.size shouldBe 1
        succeed
      }

      "not remove a task if the participantId does not match" in {
        val (_, scheduler) = setup()
        scheduler.pendingClearances.put(task.partyId, task)
        val otherParticipant = testData.p2Id
        val clearanceTx = createTx(partyId, otherParticipant, onboarding = false)

        scheduler
          .observed(
            SequencedTime(CantonTimestamp.now()),
            EffectiveTime(CantonTimestamp.now()),
            SequencerCounter.One,
            Seq(clearanceTx),
          )
          .futureValueUS

        scheduler.pendingClearances.size shouldBe 1
        succeed
      }

      "only remove the task matching the partyId" in {
        val (_, scheduler) = setup()
        val otherParty = testData.party2
        val taskOtherParty = task.copy(partyId = otherParty)

        scheduler.pendingClearances.put(task.partyId, task)
        scheduler.pendingClearances.put(taskOtherParty.partyId, taskOtherParty)
        scheduler.pendingClearances.size shouldBe 2

        val clearanceTx = createTx(partyId, participantId, onboarding = false)

        scheduler
          .observed(
            SequencedTime(CantonTimestamp.now()),
            EffectiveTime(CantonTimestamp.now()),
            SequencerCounter.One,
            Seq(clearanceTx),
          )
          .futureValueUS

        scheduler.pendingClearances.size shouldBe 1
        scheduler.pendingClearances.get(task.partyId) shouldBe None
        scheduler.pendingClearances.get(taskOtherParty.partyId) shouldBe Some(taskOtherParty)
      }

      /** "onboard, offboard, onboard" the same party in a sequence:
        *
        *   1. Onboard (1): First, onboard a party and verify that a clearance task is successfully
        *      scheduled.
        *   1. Offboard: Then simulate the party being offboarded by feeding a `Remove` topology
        *      transaction into the `observed` method. – Assert that this action correctly removes
        *      the pending task.
        *   1. Onboard (2): Finally, onboard the same party again.
        *   1. Verification: It checks that a *new* clearance task is successfully scheduled,
        *      proving that the stale task from step 1 was properly cleaned up.
        *
        * The goal is to assert that the implementation prevents a stale clearance task from
        * blocking future onboarding.
        *
        * The original implementation had this issue: If a party is onboarded (scheduling task 1),
        * then offboarded (which wasn't clearing the task), and then onboarded again (scheduling
        * task 2), the stale task 1 would remain. This would cause the scheduler to ignore task 2,
        * and the party's flag would never be cleared.
        */
      "remove a pending task on offboarding to allow subsequent onboarding" in {
        val (fakeWorkflow, scheduler) = setup()
        val tickPromise1 = Promise[Unit]()
        val tickPromise2 = Promise[Unit]()

        when(mockTimeTracker.awaitTick(any[CantonTimestamp])(any[TraceContext]))
          .thenReturn(Some(tickPromise1.future), Some(tickPromise2.future))

        val flagSet = FlagSet(earliestClearanceTime)
        fakeWorkflow.addResponse(flagSet) // For first request
        fakeWorkflow.addResponse(flagSet) // For second request

        // 1. Onboard (1)
        scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .valueOrFailShutdown("First onboarding request failed")
          .futureValue

        scheduler.pendingClearances.size shouldBe 1
        verify(mockTimeTracker, times(1)).awaitTick(any[CantonTimestamp])(any[TraceContext])

        // 2. Offboard
        val removeTx = createRemoveTx(partyId, participantId)
        scheduler
          .observed(
            SequencedTime(CantonTimestamp.now()),
            EffectiveTime(CantonTimestamp.now()),
            SequencerCounter.One,
            Seq(removeTx),
          )
          .futureValueUS

        scheduler.pendingClearances.size shouldBe 0

        // 3. Onboard (2)
        scheduler
          .requestClearance(
            partyId,
            onboardingEffectiveAt,
          )
          .valueOrFailShutdown("Second onboarding request failed")
          .futureValue

        scheduler.pendingClearances.size shouldBe 1
        verify(mockTimeTracker, times(2)).awaitTick(any[CantonTimestamp])(any[TraceContext])

        succeed
      }
    }
  }
}
