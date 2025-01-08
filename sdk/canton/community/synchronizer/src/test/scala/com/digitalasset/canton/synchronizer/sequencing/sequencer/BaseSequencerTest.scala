// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.{HashPurpose, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.HealthListener
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.scheduler.PruningScheduler
import com.digitalasset.canton.sequencing.client.SequencerClientSend
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencing.admin.data.{
  SequencerAdminStatus,
  SequencerHealthStatus,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.errors.{
  CreateSubscriptionError,
  SequencerError,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.traffic.TimestampSelector.TimestampSelector
import com.digitalasset.canton.synchronizer.sequencing.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerTrafficStatus,
}
import com.digitalasset.canton.time.{SimClock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  participant1,
  participant2,
  sequencerId,
}
import com.digitalasset.canton.topology.{Member, SequencerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown, SequencerCounter}
import com.google.protobuf.ByteString
import org.apache.pekko.Done
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.mutable

import Sequencer.RegisterError

class BaseSequencerTest extends AsyncWordSpec with BaseTest with FailOnShutdown {
  val messageId = MessageId.tryCreate("test-message-id")
  def mkBatch(recipients: Set[Member]): Batch[ClosedEnvelope] =
    Batch[ClosedEnvelope](
      ClosedEnvelope.create(
        ByteString.EMPTY,
        Recipients.ofSet(recipients).value,
        Seq.empty,
        testedProtocolVersion,
      ) :: Nil,
      testedProtocolVersion,
    )
  def submission(from: Member, to: Set[Member]) =
    SubmissionRequest.tryCreate(
      from,
      messageId,
      mkBatch(to),
      CantonTimestamp.MaxValue,
      None,
      None,
      None,
      testedProtocolVersion,
    )

  class StubSequencer(existingMembers: Set[Member])
      extends BaseSequencer(
        loggerFactory,
        None,
        new SimClock(CantonTimestamp.Epoch, loggerFactory),
        new SignatureVerifier {
          override def verifySignature[A <: ProtocolVersionedMemoizedEvidence](
              signedContent: SignedContent[A],
              hashPurpose: HashPurpose,
              sender: A => Member,
          )(implicit
              traceContext: TraceContext
          ): EitherT[FutureUnlessShutdown, String, SignedContent[A]] =
            EitherT.rightT(signedContent)
        },
      )
      with FlagCloseable {
    val newlyRegisteredMembers =
      mutable
        .Set[Member]() // we're using the scalatest serial execution context so don't need a concurrent collection
    override protected def sendAsyncInternal(submission: SubmissionRequest)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SendAsyncError, Unit] =
      EitherT.pure[FutureUnlessShutdown, SendAsyncError](())
    override protected def sendAsyncSignedInternal(
        signedSubmission: SignedContent[SubmissionRequest]
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SendAsyncError, Unit] =
      EitherT.pure[FutureUnlessShutdown, SendAsyncError](())
    override def isRegistered(member: Member)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Boolean] =
      FutureUnlessShutdown.pure(existingMembers.contains(member))

    override def registerMemberInternal(member: Member, timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, RegisterError, Unit] = {
      newlyRegisteredMembers.add(member)
      EitherT.pure(())
    }

    override def readInternal(member: Member, offset: SequencerCounter)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, Sequencer.EventSource] =
      EitherT.rightT[FutureUnlessShutdown, CreateSubscriptionError](
        Source.empty
          .viaMat(KillSwitches.single)(Keep.right)
          .mapMaterializedValue(_ -> FutureUnlessShutdown.pure(Done))
      )

    override protected def acknowledgeSignedInternal(
        signedAcknowledgeRequest: SignedContent[AcknowledgeRequest]
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = ???

    override def pruningStatus(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[SequencerPruningStatus] = ???
    override def prune(requestedTimestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, PruningError, String] = ???
    override def locatePruningTimestamp(index: PositiveInt)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, PruningSupportError, Option[CantonTimestamp]] = ???
    override def reportMaxEventAgeMetric(
        oldestEventTimestamp: Option[CantonTimestamp]
    ): Either[PruningSupportError, Unit] = ???
    override def pruningSchedulerBuilder: Option[Storage => PruningScheduler] = ???
    override def pruningScheduler: Option[PruningScheduler] = ???
    override def snapshot(timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SequencerError, SequencerSnapshot] =
      ???
    override protected val localSequencerMember: Member = sequencerId
    override protected def disableMemberInternal(member: Member)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit
    override protected def healthInternal(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[SequencerHealthStatus] =
      FutureUnlessShutdown.pure(SequencerHealthStatus(isActive = true))

    override def adminStatus: SequencerAdminStatus = ???
    override private[sequencing] def firstSequencerCounterServeableForSequencer(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[SequencerCounter] =
      ???
    override def trafficStatus(members: Seq[Member], selector: TimestampSelector)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[SequencerTrafficStatus] = ???

    override protected def timeouts: ProcessingTimeout = ProcessingTimeout()
    override def setTrafficPurchased(
        member: Member,
        serial: PositiveInt,
        totalTrafficPurchased: NonNegativeLong,
        sequencerClient: SequencerClientSend,
        synchronizerTimeTracker: SynchronizerTimeTracker,
    )(implicit
        traceContext: TraceContext
    ): EitherT[
      FutureUnlessShutdown,
      TrafficControlErrors.TrafficControlError,
      Unit,
    ] = ???

    override def getTrafficStateAt(member: Member, timestamp: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[
      TrafficState
    ]] =
      EitherT.pure(Some(TrafficState.empty(timestamp)))

    override def isEnabled(member: Member)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Boolean] =
      FutureUnlessShutdown.pure(existingMembers.contains(member))
  }

  Seq(("sendAsync", false), ("sendAsyncSigned", true)).foreach { case (name, useSignedSend) =>
    def send(sequencer: Sequencer)(submission: SubmissionRequest) =
      if (useSignedSend)
        sequencer.sendAsyncSigned(
          SignedContent(submission, Signature.noSignature, None, testedProtocolVersion)
        )
      else sequencer.sendAsync(submission)

    name should {

      "sends should not auto register" in {
        val sequencer = new StubSequencer(existingMembers = Set(participant1))
        val request = submission(from = participant1, to = Set(participant1, participant2))

        for {
          _ <- send(sequencer)(request).value.failOnShutdown
        } yield sequencer.newlyRegisteredMembers shouldBe empty
      }
    }
  }

  "health" should {
    "onHealthChange should register listener and immediately call it with current status" in {
      val sequencer = new StubSequencer(Set())
      var status = SequencerHealthStatus(isActive = false)
      sequencer.registerOnHealthChange(HealthListener("") { status = sequencer.getState })

      status shouldBe SequencerHealthStatus(isActive = true)
    }

    "health status change should trigger registered health listener" in {
      val sequencer = new StubSequencer(Set())
      var status = SequencerHealthStatus(isActive = true)
      sequencer.registerOnHealthChange(HealthListener("") { status = sequencer.getState })

      val badHealth = SequencerHealthStatus(isActive = false, Some("something bad happened"))
      sequencer.reportHealthState(badHealth)

      status shouldBe badHealth
    }

    "trigger high priority listeners before others" in {
      val sequencer = new StubSequencer(Set())
      var highPriorityStatus = SequencerHealthStatus(true)
      var status = SequencerHealthStatus(true)
      val badHealth = SequencerHealthStatus(false, Some("something bad happened"))
      sequencer.registerOnHealthChange(HealthListener("")({
        // High prio should already have been set when we switch to the bad health state
        if (sequencer.getState == badHealth) highPriorityStatus shouldBe badHealth
        status = sequencer.getState
      }))
      sequencer.registerHighPriorityOnHealthChange(HealthListener("") {
        highPriorityStatus = sequencer.getState
      })

      sequencer.reportHealthState(badHealth)

      status shouldBe badHealth

    }
  }

  "disableMember" should {
    "disableMember should only allow disabling non-local sequencer member" in {
      val sequencer = new StubSequencer(Set(participant1))
      for {
        _ <- sequencer.disableMember(participant1).valueOrFail("Can disable regular member")
        err <- sequencer.disableMember(sequencerId).leftOrFail("Fail to disable local sequencer")
        _ <- sequencer
          .disableMember(SequencerId(UniqueIdentifier.tryFromProtoPrimitive("seq::other")))
          .valueOrFail("Can disable other sequencer")
      } yield {
        err.asGrpcError.getMessage should include("CANNOT_DISABLE_LOCAL_SEQUENCER_MEMBER")
      }
    }
  }
}
