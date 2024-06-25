// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitError.AboveTrafficLimit
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
}
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.{
  InMemoryTrafficConsumedStore,
  InMemoryTrafficPurchasedStore,
}
import com.digitalasset.canton.protocol.DomainParameters
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.{
  EventCostCalculator,
  TrafficPurchased,
  TrafficReceipt,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member, TestingTopology}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasTestCloseContext, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import java.util.UUID

class EnterpriseSequencerRateLimitManagerTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasTestCloseContext
    with RateLimitManagerTesting {

  private val maxBaseTrafficRemainder = NonNegativeLong.tryCreate(5)
  private val trafficConfig: TrafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = maxBaseTrafficRemainder,
    maxBaseTrafficAccumulationDuration = NonNegativeFiniteDuration.tryOfSeconds(1),
  )

  private val senderTs = CantonTimestamp.Epoch
  private val sequencerTs = CantonTimestamp.Epoch.plusSeconds(1)
  private val sequencingTs = CantonTimestamp.Epoch.plusSeconds(2)

  private val senderFactor = PositiveInt.one
  private val sequencerFactor = PositiveInt.two
  private val sequencingFactor = PositiveInt.three

  private val defaultDDP = DefaultTestIdentities.defaultDynamicDomainParameters
  private val ddp1 = defaultDDP
    .tryUpdate(trafficControlParameters =
      Some(trafficConfig.copy(readVsWriteScalingFactor = senderFactor))
    )
  private val ddp2 = defaultDDP
    .tryUpdate(trafficControlParameters =
      Some(trafficConfig.copy(readVsWriteScalingFactor = sequencerFactor))
    )
  private val ddp3 = defaultDDP
    .tryUpdate(trafficControlParameters =
      Some(trafficConfig.copy(readVsWriteScalingFactor = sequencingFactor))
    )

  override lazy val cryptoClient =
    TestingTopology()
      .copy(
        domainParameters = List(
          DomainParameters.WithValidity(
            validFrom = CantonTimestamp.MinValue,
            validUntil = Some(sequencerTs.immediatePredecessor),
            parameter = ddp1,
          ),
          DomainParameters.WithValidity(
            validFrom = sequencerTs.immediatePredecessor,
            validUntil = Some(sequencingTs.immediatePredecessor),
            parameter = ddp2,
          ),
          DomainParameters.WithValidity(
            validFrom = sequencingTs.immediatePredecessor,
            validUntil = None,
            parameter = ddp3,
          ),
        )
      )
      .build(loggerFactory)
      .forOwnerAndDomain(
        DefaultTestIdentities.participant1,
        currentSnapshotApproximationTimestamp = sequencerTs,
      )

  private val sender: Member = participant1.member
  private val eventCost = 10L
  private val eventCostNonNegative = NonNegativeLong.tryCreate(eventCost)
  private val expectedExtraTrafficConsumed =
    NonNegativeLong.tryCreate(eventCostNonNegative.value - maxBaseTrafficRemainder.value)

  private val incorrectSubmissionCost = 6L
  private val incorrectSubmissionCostNN = NonNegativeLong.tryCreate(incorrectSubmissionCost)
  private val trafficPurchased = NonNegativeLong.tryCreate(15L)
  private val serial = Some(PositiveInt.one)

  private lazy val defaultSubmissionRequest =
    SubmissionRequest.tryCreate(
      DefaultTestIdentities.participant1,
      MessageId.fromUuid(new UUID(1L, 1L)),
      Batch.empty(testedProtocolVersion),
      maxSequencingTime = CantonTimestamp.MaxValue,
      topologyTimestamp = None,
      Option.empty[AggregationRule],
      Some(SequencingSubmissionCost(eventCostNonNegative, testedProtocolVersion)),
      testedProtocolVersion,
    )

  case class Env(
      trafficConfig: TrafficControlParameters,
      eventCostCalculator: EventCostCalculator,
      rlm: SequencerRateLimitManager,
      balanceManager: TrafficPurchasedManager,
  )

  override type FixtureParam = Env

  private def assertTrafficConsumed(
      expectedExtraTrafficPurchased: NonNegativeLong = trafficPurchased,
      expectedTrafficConsumed: NonNegativeLong = expectedExtraTrafficConsumed,
      expectedBaseTrafficRemainder: NonNegativeLong = NonNegativeLong.zero,
      expectedSerial: Option[PositiveInt] = serial,
      timestamp: CantonTimestamp = sequencingTs,
  )(implicit f: Env) = for {
    states <- f.rlm
      .getStates(Set(sender), Some(sequencingTs), None, warnIfApproximate = false)
      .failOnShutdown
  } yield states.get(sender) shouldBe Some(
    Right(
      TrafficState(
        expectedExtraTrafficPurchased,
        expectedTrafficConsumed,
        expectedBaseTrafficRemainder,
        timestamp,
        expectedSerial,
      )
    )
  )

  private def assertTrafficNotConsumed(
      expectedExtraTrafficPurchased: NonNegativeLong = NonNegativeLong.zero,
      expectedTrafficConsumed: NonNegativeLong = NonNegativeLong.zero,
      expectedBaseTrafficRemainder: NonNegativeLong = maxBaseTrafficRemainder,
      expectedSerial: Option[PositiveInt] = None,
  )(implicit f: Env) = for {
    states <- f.rlm
      .getStates(Set(sender), Some(sequencingTs), None, warnIfApproximate = false)
      .failOnShutdown
  } yield states.get(sender) shouldBe Some(
    Right(
      TrafficState(
        expectedExtraTrafficPurchased,
        expectedTrafficConsumed,
        expectedBaseTrafficRemainder,
        sequencingTs,
        expectedSerial,
      )
    )
  )

  private def validate(
      cost: Option[SequencingSubmissionCost] = Some(
        SequencingSubmissionCost(eventCostNonNegative, testedProtocolVersion)
      ),
      submissionTimestamp: Option[CantonTimestamp] = Some(senderTs),
      lastKnownSequencedEvent: CantonTimestamp = sequencerTs,
  )(implicit f: Env) = {
    f.rlm
      .validateRequestAtSubmissionTime(
        defaultSubmissionRequest.copy(submissionCost = cost),
        submissionTimestamp,
        lastKnownSequencedEvent,
      )
      .value
      .failOnShutdown
  }

  private def mkEnvelope(content: String): ClosedEnvelope = {
    ClosedEnvelope.create(
      ByteString.copyFromUtf8(content),
      Recipients.cc(DefaultTestIdentities.participant1),
      Seq.empty,
      testedProtocolVersion,
    )
  }

  private def mkBatch(content: String) = Batch(List(mkEnvelope(content)), testedProtocolVersion)

  private def consume(
      cost: Option[NonNegativeLong] = Some(eventCostNonNegative),
      submissionTimestamp: Option[CantonTimestamp] = Some(senderTs),
      sequencingTimestamp: CantonTimestamp = sequencingTs,
      correctCost: NonNegativeLong = eventCostNonNegative,
      content: String = "hello",
  )(implicit f: Env) = {
    val batch = mkBatch(content)
    when(
      f.eventCostCalculator
        .computeEventCost(
          same(batch),
          same(sequencingFactor),
          any[Map[GroupRecipient, Set[Member]]],
          any[ProtocolVersion],
        )(any[TraceContext])
    ).thenReturn(correctCost)

    f.rlm
      .validateRequestAndConsumeTraffic(
        defaultSubmissionRequest.copy(
          submissionCost = cost.map(SequencingSubmissionCost(_, testedProtocolVersion)),
          batch = batch,
        ),
        sequencingTimestamp,
        submissionTimestamp = submissionTimestamp,
        None,
        warnIfApproximate = false,
        sequencerSignature = Signature.noSignature,
      )
      .value
      .failOnShutdown
  }

  private def purchaseTraffic(implicit f: Env) = {
    f.balanceManager.addTrafficPurchased(
      TrafficPurchased(
        sender,
        PositiveInt.one,
        trafficPurchased,
        sequencerTs.immediatePredecessor,
      )
    )
  }

  private def returnIncorrectCostFromSender(
      cost: NonNegativeLong = incorrectSubmissionCostNN
  )(implicit f: Env) = {
    when(
      f.eventCostCalculator
        .computeEventCost(
          any[Batch[ClosedEnvelope]],
          same(senderFactor),
          any[Map[GroupRecipient, Set[Member]]],
          any[ProtocolVersion],
        )(any[TraceContext])
    )
      .thenReturn(cost)
  }

  private def returnCorrectCost(implicit f: Env) = {
    when(
      f.eventCostCalculator
        .computeEventCost(
          any[Batch[ClosedEnvelope]],
          any[PositiveInt],
          any[Map[GroupRecipient, Set[Member]]],
          any[ProtocolVersion],
        )(any[TraceContext])
    )
      .thenReturn(eventCostNonNegative)
  }

  "traffic control when processing submission request" should {
    "let requests through if enough traffic" in { implicit f =>
      for {
        _ <- purchaseTraffic
        res <- validate()
      } yield {
        res shouldBe Right(())
      }
    }

    "fail if not enough traffic" in { implicit f =>
      for {
        res <- validate()
      } yield {
        res shouldBe Left(
          SequencerRateLimitError.AboveTrafficLimit(
            participant1.member,
            eventCostNonNegative,
            TrafficState(
              NonNegativeLong.zero,
              NonNegativeLong.zero,
              maxBaseTrafficRemainder,
              sequencerTs,
              None,
            ),
          )
        )
      }
    }

    "succeed if the cost is incorrect according to validation time but correct according to submission time and within the tolerance window" in {
      implicit f =>
        returnIncorrectCostFromSender()

        for {
          _ <- purchaseTraffic
          res <- validate(cost =
            Some(SequencingSubmissionCost(incorrectSubmissionCostNN, testedProtocolVersion))
          )
        } yield {
          res shouldBe Right(())
        }
    }

    "succeed if the submission cost is greater than the correct cost" in { implicit f =>
      returnIncorrectCostFromSender(NonNegativeLong.tryCreate(11))
      // Just outside the tolerance window
      val submissionTimestamp = sequencerTs
        .minusSeconds(defaultDDP.submissionCostTimestampTopologyTolerance.duration.toSeconds)
        .immediatePredecessor
      for {
        _ <- purchaseTraffic
        res <- validate(
          cost =
            Some(SequencingSubmissionCost(NonNegativeLong.tryCreate(11), testedProtocolVersion)),
          submissionTimestamp = Some(submissionTimestamp),
        )
      } yield {
        res shouldBe Right(())
      }
    }

    "fail if the cost is incorrect according to sequencing time but correct according to submission time but outside the tolerance window" in {
      implicit f =>
        returnIncorrectCostFromSender()
        // Just outside the tolerance window
        val submissionTimestamp = sequencerTs
          .minusSeconds(defaultDDP.submissionCostTimestampTopologyTolerance.duration.toSeconds)
          .immediatePredecessor
        for {
          _ <- purchaseTraffic
          res <- validate(
            cost = Some(SequencingSubmissionCost(incorrectSubmissionCostNN, testedProtocolVersion)),
            submissionTimestamp = Some(submissionTimestamp),
          )
        } yield {
          res shouldBe Left(
            SequencerRateLimitError.OutdatedEventCost(
              participant1.member,
              Some(incorrectSubmissionCostNN),
              submissionTimestamp,
              eventCostNonNegative,
              sequencerTs,
            )
          )
        }
    }

    "fail if the cost is incorrect according to sequencing time and incorrect according to submission time inside the tolerance window" in {
      implicit f =>
        // Mock an incorrect cost event according to submission time topology
        returnIncorrectCostFromSender(NonNegativeLong.tryCreate(incorrectSubmissionCost + 1))

        for {
          _ <- purchaseTraffic
          res <- validate(cost =
            Some(SequencingSubmissionCost(incorrectSubmissionCostNN, testedProtocolVersion))
          )
        } yield {
          res shouldBe Left(
            SequencerRateLimitError.IncorrectEventCost.Error(
              participant1.member,
              Some(senderTs),
              Some(incorrectSubmissionCostNN),
              eventCostNonNegative,
              sequencerTs,
              None,
            )
          )
        }
    }

    "fail if the cost is incorrect according to sequencing time and incorrect according to submission time outside the tolerance window" in {
      implicit f =>
        // Mock an incorrect cost event according to submission time topology
        returnIncorrectCostFromSender(NonNegativeLong.tryCreate(incorrectSubmissionCost + 1))
        // Just outside the tolerance window
        val submissionTimestamp = sequencerTs
          .minusSeconds(defaultDDP.submissionCostTimestampTopologyTolerance.duration.toSeconds)
          .immediatePredecessor
        for {
          res <- validate(
            cost = Some(SequencingSubmissionCost(incorrectSubmissionCostNN, testedProtocolVersion)),
            submissionTimestamp = Some(submissionTimestamp),
          )
        } yield {
          res shouldBe Left(
            SequencerRateLimitError.IncorrectEventCost.Error(
              participant1.member,
              Some(submissionTimestamp),
              Some(incorrectSubmissionCostNN),
              eventCostNonNegative,
              sequencerTs,
              None,
            )
          )
        }
    }

    "fail if the cost is incorrect according to sequencing time and no submission timestamp was provided" in {
      implicit f =>
        for {
          res <- validate(
            cost = Some(SequencingSubmissionCost(incorrectSubmissionCostNN, testedProtocolVersion)),
            submissionTimestamp = None,
          )
        } yield {
          res shouldBe Left(
            SequencerRateLimitError.IncorrectEventCost.Error(
              participant1.member,
              None,
              Some(incorrectSubmissionCostNN),
              eventCostNonNegative,
              sequencerTs,
              None,
            )
          )
        }
    }

    "fail if the cost is incorrect according to sequencing time and submission time is in the future" in {
      implicit f =>
        for {
          res <- validate(
            cost = Some(SequencingSubmissionCost(incorrectSubmissionCostNN, testedProtocolVersion)),
            submissionTimestamp = Some(sequencerTs.immediateSuccessor),
          )
        } yield {
          res shouldBe Left(
            SequencerRateLimitError.IncorrectEventCost.Error(
              participant1.member,
              Some(sequencerTs.immediateSuccessor),
              Some(incorrectSubmissionCostNN),
              eventCostNonNegative,
              sequencerTs,
              None,
            )
          )
        }
    }
  }

  "traffic control after sequencing" should {
    "consume traffic" in { implicit f =>
      returnCorrectCost

      for {
        _ <- purchaseTraffic
        res <- consume()
        _ <- assertTrafficConsumed()
      } yield {
        res shouldBe Right(
          Some(
            TrafficReceipt(
              consumedCost = eventCostNonNegative,
              extraTrafficConsumed = NonNegativeLong.tryCreate(
                eventCostNonNegative.value - maxBaseTrafficRemainder.value
              ),
              baseTrafficRemainder = NonNegativeLong.zero,
            )
          )
        )
      }
    }

    "advance traffic consumed timestamp even when not consuming because not enough traffic" in {
      implicit f =>
        returnCorrectCost

        val expected = Left(
          AboveTrafficLimit(
            sender,
            eventCostNonNegative,
            TrafficState(
              NonNegativeLong.zero,
              NonNegativeLong.zero,
              maxBaseTrafficRemainder,
              sequencingTs,
              None,
            ),
          )
        )

        for {
          res <- consume()
          _ <- assertTrafficNotConsumed()
        } yield {
          res shouldBe expected
        }
    }

    "support replaying events that have already been consumed" in { implicit f =>
      for {
        _ <- purchaseTraffic
        // Consume at sequencingTs (default)
        res1 <- consume(correctCost = NonNegativeLong.one, cost = Some(NonNegativeLong.one))
        _ <- assertTrafficConsumed(
          expectedTrafficConsumed = NonNegativeLong.zero,
          expectedBaseTrafficRemainder = NonNegativeLong.tryCreate(4),
        )
        // then at sequencingTs.plusMillis(1)
        res2 <- consume(
          correctCost = NonNegativeLong.one,
          sequencingTimestamp = sequencingTs.plusMillis(1),
          cost = Some(NonNegativeLong.one),
        )
        _ <- assertTrafficConsumed(
          expectedTrafficConsumed = NonNegativeLong.zero,
          expectedBaseTrafficRemainder = NonNegativeLong.tryCreate(3),
          timestamp = sequencingTs.plusMillis(1),
        )
        // then repeat consume at sequencingTs, which simulates a crash recovery that replays the event
        res3 <- consume(correctCost = NonNegativeLong.one, cost = Some(NonNegativeLong.one))
        // Traffic consumed should stay the same
        _ <- assertTrafficConsumed(
          expectedTrafficConsumed = NonNegativeLong.zero,
          expectedBaseTrafficRemainder = NonNegativeLong.tryCreate(3),
          timestamp = sequencingTs.plusMillis(1),
        )
      } yield {
        res1 shouldBe Right(
          Some(
            TrafficReceipt(
              consumedCost = NonNegativeLong.one,
              extraTrafficConsumed = NonNegativeLong.zero,
              baseTrafficRemainder = NonNegativeLong.tryCreate(maxBaseTrafficRemainder.value - 1),
            )
          )
        )
        res2 shouldBe Right(
          Some(
            TrafficReceipt(
              consumedCost = NonNegativeLong.one,
              extraTrafficConsumed = NonNegativeLong.zero,
              baseTrafficRemainder = NonNegativeLong.tryCreate(maxBaseTrafficRemainder.value - 2),
            )
          )
        )
        // Make sure the receipt is the same as for res1
        res3 shouldBe res1
      }
    }

    "succeed if the cost is incorrect according to sequencing time but correct according to submission time and within the tolerance window" in {
      implicit f =>
        returnIncorrectCostFromSender()
        for {
          _ <- purchaseTraffic
          res <- consume(cost = Some(incorrectSubmissionCostNN))
          _ <- assertTrafficConsumed(expectedTrafficConsumed = NonNegativeLong.one)
        } yield {
          res shouldBe Right(
            Some(
              TrafficReceipt(
                consumedCost = incorrectSubmissionCostNN,
                extraTrafficConsumed = NonNegativeLong.tryCreate(
                  incorrectSubmissionCostNN.value - maxBaseTrafficRemainder.value
                ),
                baseTrafficRemainder = NonNegativeLong.zero,
              )
            )
          )
        }
    }

    "fail and not consume if the cost is incorrect according to sequencing time but correct according to submission time and outside the tolerance window" in {
      implicit f =>
        returnIncorrectCostFromSender()
        val submissionTs =
          (sequencingTs - defaultDDP.submissionCostTimestampTopologyTolerance).immediatePredecessor
        for {
          _ <- purchaseTraffic
          res <- consume(
            cost = Some(incorrectSubmissionCostNN),
            submissionTimestamp = Some(submissionTs),
          )
          _ <- assertTrafficNotConsumed(
            expectedExtraTrafficPurchased = trafficPurchased,
            expectedSerial = serial,
          )
        } yield {
          res shouldBe Left(
            SequencerRateLimitError.OutdatedEventCost(
              participant1.member,
              Some(incorrectSubmissionCostNN),
              submissionTs,
              eventCostNonNegative,
              cryptoClient.headSnapshot.ipsSnapshot.timestamp,
              Some(
                TrafficReceipt(
                  consumedCost = NonNegativeLong.zero,
                  extraTrafficConsumed = NonNegativeLong.zero,
                  baseTrafficRemainder = maxBaseTrafficRemainder,
                )
              ),
            )
          )
        }
    }

    "fail if the cost is incorrect according to sequencing time and incorrect according to submission time inside the tolerance window" in {
      implicit f =>
        // Mock an incorrect cost event according to submission time topology
        returnIncorrectCostFromSender(NonNegativeLong.tryCreate(incorrectSubmissionCost + 1))
        for {
          res <- consume(cost = Some(incorrectSubmissionCostNN))
          _ <- assertTrafficNotConsumed()
        } yield {
          res shouldBe Left(
            SequencerRateLimitError.IncorrectEventCost.Error(
              participant1.member,
              Some(senderTs),
              Some(incorrectSubmissionCostNN),
              eventCostNonNegative,
              cryptoClient.headSnapshot.ipsSnapshot.timestamp,
              Some(Signature.noSignature.signedBy),
              Some(
                TrafficReceipt(
                  consumedCost = NonNegativeLong.zero,
                  extraTrafficConsumed = NonNegativeLong.zero,
                  baseTrafficRemainder = maxBaseTrafficRemainder,
                )
              ),
            )
          )
        }
    }

    "fail if the cost is incorrect according to sequencing time and incorrect according to submission time outside the tolerance window" in {
      implicit f =>
        val submissionCost = 6L
        val submissionCostNN = NonNegativeLong.tryCreate(submissionCost)

        // Mock an incorrect cost event according to submission time topology
        returnIncorrectCostFromSender(NonNegativeLong.tryCreate(submissionCost + 1))
        val submissionTs =
          (sequencingTs - defaultDDP.submissionCostTimestampTopologyTolerance).immediatePredecessor
        for {
          res <- consume(
            cost = Some(incorrectSubmissionCostNN),
            submissionTimestamp = Some(submissionTs),
          )
          _ <- assertTrafficNotConsumed()
        } yield {
          res shouldBe Left(
            SequencerRateLimitError.IncorrectEventCost.Error(
              participant1.member,
              Some(submissionTs),
              Some(submissionCostNN),
              eventCostNonNegative,
              cryptoClient.headSnapshot.ipsSnapshot.timestamp,
              Some(Signature.noSignature.signedBy),
              Some(
                TrafficReceipt(
                  consumedCost = NonNegativeLong.zero,
                  extraTrafficConsumed = NonNegativeLong.zero,
                  baseTrafficRemainder = maxBaseTrafficRemainder,
                )
              ),
            )
          )
        }
    }

    "fail if the cost is incorrect according to sequencing time and no submission timestamp was provided" in {
      implicit f =>
        val submissionCost = 6L
        val submissionCostNN = NonNegativeLong.tryCreate(submissionCost)
        for {
          res <- consume(cost = Some(incorrectSubmissionCostNN), submissionTimestamp = None)
          _ <- assertTrafficNotConsumed()
        } yield {
          res shouldBe Left(
            SequencerRateLimitError.IncorrectEventCost.Error(
              participant1.member,
              None,
              Some(submissionCostNN),
              eventCostNonNegative,
              cryptoClient.headSnapshot.ipsSnapshot.timestamp,
              Some(Signature.noSignature.signedBy),
              Some(
                TrafficReceipt(
                  consumedCost = NonNegativeLong.zero,
                  extraTrafficConsumed = NonNegativeLong.zero,
                  baseTrafficRemainder = maxBaseTrafficRemainder,
                )
              ),
            )
          )
        }
    }

    "fail if the cost is incorrect according to sequencing time and submission time is in the future" in {
      implicit f =>
        for {
          res <- consume(
            cost = Some(incorrectSubmissionCostNN),
            submissionTimestamp = Some(sequencingTs.immediateSuccessor),
          )
          _ <- assertTrafficNotConsumed()
        } yield {
          res shouldBe Left(
            SequencerRateLimitError.IncorrectEventCost.Error(
              participant1.member,
              Some(sequencingTs.immediateSuccessor),
              Some(incorrectSubmissionCostNN),
              eventCostNonNegative,
              cryptoClient.headSnapshot.ipsSnapshot.timestamp,
              Some(Signature.noSignature.signedBy),
              Some(
                TrafficReceipt(
                  consumedCost = NonNegativeLong.zero,
                  extraTrafficConsumed = NonNegativeLong.zero,
                  baseTrafficRemainder = maxBaseTrafficRemainder,
                )
              ),
            )
          )
        }
    }
  }

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val store = new InMemoryTrafficPurchasedStore(loggerFactory)
    val consumedStore = new InMemoryTrafficConsumedStore(loggerFactory)
    val manager = mkTrafficPurchasedManager(store)
    val eventCostCalculator = mock[EventCostCalculator]

    when(
      eventCostCalculator
        .computeEventCost(
          any[Batch[ClosedEnvelope]],
          same(sequencerFactor),
          any[Map[GroupRecipient, Set[Member]]],
          any[ProtocolVersion],
        )(any[TraceContext])
    )
      .thenReturn(eventCostNonNegative)

    val rateLimiter = mkRateLimiter(manager, consumedStore, eventCostCalculator)
    val env = Env(
      trafficConfig,
      eventCostCalculator,
      rateLimiter,
      manager,
    )

    withFixture(test.toNoArgAsyncTest(env))
  }
}
