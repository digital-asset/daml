// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.HealthComponent.AlwaysHealthyComponent
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.sequencing.client.SequencedEventValidationError.*
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, SequencedEvent}
import com.digitalasset.canton.store.SequencedEventStore.IgnoredSequencedEvent
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.util.PekkoUtilTest.{noOpKillSwitch, withNoOpKillSwitch}
import com.digitalasset.canton.util.ResourceUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import com.google.protobuf.ByteString
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.wordspec.FixtureAnyWordSpec
import org.scalatest.{Assertion, Outcome}

class SequencedEventValidatorTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext {

  override type FixtureParam = SequencedEventTestFixture

  override def withFixture(test: OneArgTest): Outcome =
    ResourceUtil.withResource(
      new SequencedEventTestFixture(
        loggerFactory,
        testedProtocolVersion,
        timeouts,
        futureSupervisor,
      )
    ) { env => withFixture(test.toNoArgTest(env)) }

  "validate on reconnect" should {
    "accept the prior event" in { fixture =>
      import fixture.*
      val priorEvent = createEvent().futureValue
      val validator = mkValidator()
      validator
        .validateOnReconnect(Some(priorEvent), priorEvent, DefaultTestIdentities.sequencerId)
        .valueOrFail("successful reconnect")
        .failOnShutdown
        .futureValue
    }

    "accept a new signature on the prior event" in { fixture =>
      import fixture.*
      val priorEvent = createEvent().futureValue
      val validator = mkValidator()
      val sig = sign(
        priorEvent.signedEvent.content.getCryptographicEvidence,
        CantonTimestamp.Epoch,
      ).futureValue
      assert(sig != priorEvent.signedEvent.signature)
      val eventWithNewSig =
        priorEvent.copy(priorEvent.signedEvent.copy(signatures = NonEmpty(Seq, sig)))(
          fixtureTraceContext
        )
      validator
        .validateOnReconnect(Some(priorEvent), eventWithNewSig, DefaultTestIdentities.sequencerId)
        .valueOrFail("event with regenerated signature")
        .failOnShutdown
        .futureValue
    }

    "accept a different serialization of the same content" in { fixture =>
      import fixture.*
      val deliver1 = createEventWithCounterAndTs(1L, CantonTimestamp.Epoch).futureValue
      val deliver2 = createEventWithCounterAndTs(
        1L,
        CantonTimestamp.Epoch,
        customSerialization = Some(ByteString.copyFromUtf8("Different serialization")),
      ).futureValue // changing serialization, but not the contents

      val validator = mkValidator()
      validator
        .validateOnReconnect(Some(deliver1), deliver2, DefaultTestIdentities.sequencerId)
        .valueOrFail("Different serialization should be accepted")
        .failOnShutdown
        .futureValue
    }

    "check the domain Id" in { fixture =>
      import fixture.*
      val incorrectDomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("wrong-domain::id"))
      val validator = mkValidator()
      val wrongDomain = createEvent(incorrectDomainId).futureValue
      val err = validator
        .validateOnReconnect(
          Some(
            IgnoredSequencedEvent(CantonTimestamp.MinValue, SequencerCounter(updatedCounter), None)(
              fixtureTraceContext
            )
          ),
          wrongDomain,
          DefaultTestIdentities.sequencerId,
        )
        .leftOrFail("wrong domain ID on reconnect")
        .failOnShutdown
        .futureValue

      err shouldBe BadDomainId(defaultDomainId, incorrectDomainId)
    }

    "check for a fork" in { fixture =>
      import fixture.*

      def expectLog[E](
          cmd: => FutureUnlessShutdown[SequencedEventValidationError[E]]
      ): SequencedEventValidationError[E] = {
        loggerFactory
          .assertLogs(cmd, _.shouldBeCantonErrorCode(ResilientSequencerSubscription.ForkHappened))
          .failOnShutdown
          .futureValue
      }

      val priorEvent = createEvent().futureValue
      val validator = mkValidator()
      val differentCounter = createEvent(counter = 43L).futureValue

      val errCounter = expectLog(
        validator
          .validateOnReconnect(
            Some(priorEvent),
            differentCounter,
            DefaultTestIdentities.sequencerId,
          )
          .leftOrFail("fork on counter")
      )
      val differentTimestamp = createEvent(timestamp = CantonTimestamp.MaxValue).futureValue
      val errTimestamp = expectLog(
        validator
          .validateOnReconnect(
            Some(priorEvent),
            differentTimestamp,
            DefaultTestIdentities.sequencerId,
          )
          .leftOrFail("fork on timestamp")
      )

      val differentContent = createEventWithCounterAndTs(
        counter = updatedCounter,
        CantonTimestamp.Epoch,
      ).futureValue

      val errContent = expectLog(
        validator
          .validateOnReconnect(
            Some(priorEvent),
            differentContent,
            DefaultTestIdentities.sequencerId,
          )
          .leftOrFail("fork on content")
      )

      def assertFork[E](err: SequencedEventValidationError[E])(
          counter: SequencerCounter,
          suppliedEvent: SequencedEvent[ClosedEnvelope],
          expectedEvent: Option[SequencedEvent[ClosedEnvelope]],
      ): Assertion = {
        err match {
          case ForkHappened(counterRes, suppliedEventRes, expectedEventRes) =>
            (
              counter,
              suppliedEvent,
              expectedEvent,
            ) shouldBe (counterRes, suppliedEventRes, expectedEventRes)
          case x => fail(s"${x} is not ForkHappened")
        }
      }

      assertFork(errCounter)(
        SequencerCounter(updatedCounter),
        differentCounter.signedEvent.content,
        Some(priorEvent.signedEvent.content),
      )

      assertFork(errTimestamp)(
        SequencerCounter(updatedCounter),
        differentTimestamp.signedEvent.content,
        Some(priorEvent.signedEvent.content),
      )

      assertFork(errContent)(
        SequencerCounter(updatedCounter),
        differentContent.signedEvent.content,
        Some(priorEvent.signedEvent.content),
      )
    }

    "verify the signature" in { fixture =>
      import fixture.*
      val priorEvent = createEvent().futureValue
      val badSig =
        sign(ByteString.copyFromUtf8("not-the-message"), CantonTimestamp.Epoch).futureValue
      val badEvent = createEvent(signatureOverride = Some(badSig)).futureValue
      val validator = mkValidator()
      val result = validator
        .validateOnReconnect(Some(priorEvent), badEvent, DefaultTestIdentities.sequencerId)
        .leftOrFail("invalid signature on reconnect")
        .failOnShutdown
        .futureValue
      result shouldBe a[SignatureInvalid]
    }
  }

  "validate" should {
    "reject messages with unexpected domain ids" in { fixture =>
      import fixture.*
      val incorrectDomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("wrong-domain::id"))
      val event = createEvent(incorrectDomainId, counter = 0L).futureValue
      val validator = mkValidator()
      val result = validator
        .validate(None, event, DefaultTestIdentities.sequencerId)
        .leftOrFail("wrong domain ID")
        .failOnShutdown
        .futureValue
      result shouldBe BadDomainId(`defaultDomainId`, `incorrectDomainId`)
    }

    "reject messages with invalid signatures" in { fixture =>
      import fixture.*
      val priorEvent =
        createEvent(timestamp = CantonTimestamp.Epoch.immediatePredecessor).futureValue
      val badSig =
        sign(ByteString.copyFromUtf8("not-the-message"), CantonTimestamp.Epoch).futureValue
      val badEvent = createEvent(
        signatureOverride = Some(badSig),
        counter = priorEvent.counter.v + 1L,
      ).futureValue
      val validator = mkValidator()
      val result = validator
        .validate(Some(priorEvent), badEvent, DefaultTestIdentities.sequencerId)
        .leftOrFail("invalid signature")
        .failOnShutdown
        .futureValue
      result shouldBe a[SignatureInvalid]
    }

    "validate correctly with explicit signing timestamp" in { fixture =>
      import fixture.*
      val syncCrypto = mock[DomainSyncCryptoClient]
      when(syncCrypto.pureCrypto).thenReturn(subscriberCryptoApi.pureCrypto)
      when(syncCrypto.snapshotUS(timestamp = ts(1))(fixtureTraceContext))
        .thenAnswer[CantonTimestamp](tm => subscriberCryptoApi.snapshotUS(tm)(fixtureTraceContext))
      when(syncCrypto.topologyKnownUntilTimestamp).thenReturn(CantonTimestamp.MaxValue)
      val validator = mkValidator(syncCryptoApi = syncCrypto)
      val priorEvent = IgnoredSequencedEvent(ts(0), SequencerCounter(41), None)(fixtureTraceContext)
      val deliver =
        createEventWithCounterAndTs(42, ts(2), timestampOfSigningKey = Some(ts(1))).futureValue

      valueOrFail(validator.validate(Some(priorEvent), deliver, DefaultTestIdentities.sequencerId))(
        "validate"
      ).failOnShutdown.futureValue
    }

    "reject the same counter-timestamp if passed in repeatedly" in { fixture =>
      import fixture.*
      val priorEvent = IgnoredSequencedEvent(CantonTimestamp.MinValue, SequencerCounter(41), None)(
        fixtureTraceContext
      )
      val validator = mkValidator()

      val deliver = createEventWithCounterAndTs(42, CantonTimestamp.Epoch).futureValue
      validator
        .validate(Some(priorEvent), deliver, DefaultTestIdentities.sequencerId)
        .valueOrFail("validate1")
        .failOnShutdown
        .futureValue
      val err = validator
        .validate(Some(deliver), deliver, DefaultTestIdentities.sequencerId)
        .leftOrFail("validate2")
        .failOnShutdown
        .futureValue

      err shouldBe GapInSequencerCounter(SequencerCounter(42), SequencerCounter(42))
    }

    "fail if the counter or timestamp do not increase" in { fixture =>
      import fixture.*
      val priorEvent = IgnoredSequencedEvent(CantonTimestamp.Epoch, SequencerCounter(41), None)(
        fixtureTraceContext
      )
      val validator =
        mkValidator()

      val deliver1 = createEventWithCounterAndTs(42, CantonTimestamp.MinValue).futureValue
      val deliver2 = createEventWithCounterAndTs(0L, CantonTimestamp.MaxValue).futureValue
      val deliver3 = createEventWithCounterAndTs(42L, CantonTimestamp.ofEpochSecond(2)).futureValue

      val error1 = validator
        .validate(Some(priorEvent), deliver1, DefaultTestIdentities.sequencerId)
        .leftOrFail("deliver1")
        .failOnShutdown
        .futureValue
      val error2 = validator
        .validate(Some(priorEvent), deliver2, DefaultTestIdentities.sequencerId)
        .leftOrFail("deliver2")
        .failOnShutdown
        .futureValue
      validator
        .validate(Some(priorEvent), deliver3, DefaultTestIdentities.sequencerId)
        .valueOrFail("deliver3")
        .failOnShutdown
        .futureValue
      val error3 = validator
        .validate(Some(deliver3), deliver2, DefaultTestIdentities.sequencerId)
        .leftOrFail("deliver4")
        .failOnShutdown
        .futureValue

      error1 shouldBe NonIncreasingTimestamp(
        CantonTimestamp.MinValue,
        SequencerCounter(42),
        CantonTimestamp.Epoch,
        SequencerCounter(41),
      )
      error2 shouldBe DecreasingSequencerCounter(SequencerCounter(0), SequencerCounter(41))
      error3 shouldBe DecreasingSequencerCounter(SequencerCounter(0), SequencerCounter(42))
    }

    "fail if there is a counter cap" in { fixture =>
      import fixture.*
      val priorEvent = IgnoredSequencedEvent(CantonTimestamp.Epoch, SequencerCounter(41), None)(
        fixtureTraceContext
      )
      val validator = mkValidator()

      val deliver1 = createEventWithCounterAndTs(43L, CantonTimestamp.ofEpochSecond(1)).futureValue
      val deliver2 = createEventWithCounterAndTs(42L, CantonTimestamp.ofEpochSecond(2)).futureValue
      val deliver3 = createEventWithCounterAndTs(44L, CantonTimestamp.ofEpochSecond(3)).futureValue

      val result1 = validator
        .validate(Some(priorEvent), deliver1, DefaultTestIdentities.sequencerId)
        .leftOrFail("deliver1")
        .failOnShutdown
        .futureValue

      validator
        .validate(Some(priorEvent), deliver2, DefaultTestIdentities.sequencerId)
        .valueOrFail("deliver2")
        .failOnShutdown
        .futureValue

      val result3 = validator
        .validate(Some(deliver2), deliver3, DefaultTestIdentities.sequencerId)
        .leftOrFail("deliver3")
        .failOnShutdown
        .futureValue

      result1 shouldBe GapInSequencerCounter(SequencerCounter(43), SequencerCounter(41))
      result3 shouldBe GapInSequencerCounter(SequencerCounter(44), SequencerCounter(42))
    }
  }

  "validatePekko" should {
    implicit val prettyString = Pretty.prettyString
    lazy val alwaysHealthyComponent = new AlwaysHealthyComponent("validatePekko source", logger)

    "propagate the first subscription errors" in { fixture =>
      import fixture.*

      val validator = mkValidator()

      val errors = Seq("error1", "error2")
      val source =
        Source(errors.map(err => withNoOpKillSwitch(Left(err))))
          .watchTermination()((_, doneF) => noOpKillSwitch -> doneF)
      val subscription = SequencerSubscriptionPekko(source, alwaysHealthyComponent)
      val validatedSubscription =
        validator.validatePekko(subscription, None, DefaultTestIdentities.sequencerId)
      val validatedEventsF = validatedSubscription.source.runWith(Sink.seq)
      validatedEventsF.futureValue.map(_.unwrap) shouldBe Seq(
        Left(UpstreamSubscriptionError("error1"))
      )
    }

    "deal with stuttering" in { fixture =>
      import fixture.*

      val validator = mkValidator()
      val deliver1 = createEventWithCounterAndTs(42L, CantonTimestamp.Epoch).futureValue
      val deliver2 = createEventWithCounterAndTs(43L, CantonTimestamp.ofEpochSecond(1)).futureValue
      val deliver3 = createEventWithCounterAndTs(44L, CantonTimestamp.ofEpochSecond(2)).futureValue

      val source = Source(
        Seq(deliver1, deliver1, deliver2, deliver2, deliver2, deliver3).map(event =>
          withNoOpKillSwitch(Either.right(event))
        )
      ).watchTermination()((_, doneF) => noOpKillSwitch -> doneF)
      val subscription = SequencerSubscriptionPekko[String](source, alwaysHealthyComponent)
      val validatedSubscription =
        validator.validatePekko(subscription, Some(deliver1), DefaultTestIdentities.sequencerId)
      val validatedEventsF = validatedSubscription.source.runWith(Sink.seq)
      // deliver1 should be filtered out because it's the prior event
      validatedEventsF.futureValue.map(_.unwrap) shouldBe Seq(Right(deliver2), Right(deliver3))
    }

    "stop upon a validation error" in { fixture =>
      import fixture.*

      val validator = mkValidator()
      val deliver1 = createEventWithCounterAndTs(1L, CantonTimestamp.Epoch).futureValue
      val deliver2 = createEventWithCounterAndTs(2L, CantonTimestamp.ofEpochSecond(1)).futureValue
      val deliver3 = createEventWithCounterAndTs(4L, CantonTimestamp.ofEpochSecond(2)).futureValue
      val deliver4 = createEventWithCounterAndTs(5L, CantonTimestamp.ofEpochSecond(3)).futureValue

      val source = Source(
        Seq(deliver1, deliver2, deliver3, deliver4).map(event => withNoOpKillSwitch(Right(event)))
      ).watchTermination()((_, doneF) => noOpKillSwitch -> doneF)
      val subscription = SequencerSubscriptionPekko(source, alwaysHealthyComponent)
      val validatedSubscription =
        validator.validatePekko(subscription, Some(deliver1), DefaultTestIdentities.sequencerId)
      val validatedEventsF = validatedSubscription.source.runWith(Sink.seq)
      // deliver1 should be filtered out because it's the prior event
      validatedEventsF.futureValue.map(_.unwrap) shouldBe Seq(
        Right(deliver2),
        Left(GapInSequencerCounter(deliver3.counter, deliver2.counter)),
      )
    }

    "stop upon a validation error on reconnect" in { fixture =>
      import fixture.*

      val validator = mkValidator()
      val deliver1 = createEventWithCounterAndTs(1L, CantonTimestamp.Epoch).futureValue
      val deliver1a =
        createEventWithCounterAndTs(1L, CantonTimestamp.Epoch.immediateSuccessor).futureValue
      val deliver2 = createEventWithCounterAndTs(2L, CantonTimestamp.ofEpochSecond(1)).futureValue

      val source = Source(
        Seq(deliver1, deliver2).map(event => withNoOpKillSwitch(Right(event)))
      ).watchTermination()((_, doneF) => noOpKillSwitch -> doneF)
      val subscription = SequencerSubscriptionPekko(source, alwaysHealthyComponent)
      val validatedSubscription =
        validator.validatePekko(subscription, Some(deliver1a), DefaultTestIdentities.sequencerId)
      loggerFactory.assertLogs(
        validatedSubscription.source.runWith(Sink.seq).futureValue.map(_.unwrap) shouldBe Seq(
          Left(
            ForkHappened(
              SequencerCounter(1),
              deliver1.signedEvent.content,
              Some(deliver1a.signedEvent.content),
            )
          )
        ),
        // We get two log messages here: one from the validator that creates the error
        // and one from the test case that creates the error again for the comparison
        _.errorMessage should include(ResilientSequencerSubscription.ForkHappened.id),
        _.errorMessage should include(ResilientSequencerSubscription.ForkHappened.id),
      )
    }

    "not request a topology snapshot after a validation failure" in { fixture =>
      import fixture.*

      val syncCryptoApi = TestingIdentityFactory(loggerFactory)
        .forOwnerAndDomain(subscriberId, defaultDomainId, CantonTimestamp.ofEpochSecond(2))
      val validator = mkValidator(syncCryptoApi)
      val deliver1 = createEventWithCounterAndTs(1L, CantonTimestamp.Epoch).futureValue
      val deliver2 = createEventWithCounterAndTs(2L, CantonTimestamp.ofEpochSecond(1)).futureValue
      val deliver3 = createEventWithCounterAndTs(4L, CantonTimestamp.ofEpochSecond(2)).futureValue
      val deliver4 = createEventWithCounterAndTs(5L, CantonTimestamp.ofEpochSecond(300)).futureValue

      // sanity-check that the topology for deliver4 is really not available
      SyncCryptoClient
        .getSnapshotForTimestamp(
          syncCryptoApi,
          deliver4.timestamp,
          Some(deliver3.timestamp),
          testedProtocolVersion,
          warnIfApproximate = false,
        )
        .failed
        .futureValue shouldBe a[IllegalArgumentException]

      val source = Source(
        Seq(deliver1, deliver2, deliver3, deliver4).map(event => withNoOpKillSwitch(Right(event)))
      ).watchTermination()((_, doneF) => noOpKillSwitch -> doneF)
      val subscription = SequencerSubscriptionPekko(source, alwaysHealthyComponent)
      val validatedSubscription =
        validator.validatePekko(subscription, Some(deliver1), DefaultTestIdentities.sequencerId)
      val ((killSwitch, doneF), validatedEventsF) =
        validatedSubscription.source.toMat(Sink.seq)(Keep.both).run()
      // deliver1 should be filtered out because it's the prior event
      validatedEventsF.futureValue.map(_.unwrap) shouldBe Seq(
        Right(deliver2),
        Left(GapInSequencerCounter(deliver3.counter, deliver2.counter)),
      )
      killSwitch.shutdown()
      doneF.futureValue
    }
  }

}
