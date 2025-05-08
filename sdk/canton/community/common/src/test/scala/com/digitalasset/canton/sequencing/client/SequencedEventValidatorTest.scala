// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.util.PekkoUtil.noOpKillSwitch
import com.digitalasset.canton.util.PekkoUtilTest.withNoOpKillSwitch
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
    )(env => withFixture(test.toNoArgTest(env)))

  "validate on reconnect" should {
    "accept the prior event" in { fixture =>
      import fixture.*
      val priorEvent = createEvent().futureValueUS
      val validator = mkValidator()
      validator
        .validateOnReconnect(Some(priorEvent), priorEvent, DefaultTestIdentities.sequencerId)
        .valueOrFail("successful reconnect")
        .failOnShutdown
        .futureValue
    }

    "accept a new signature on the prior event" in { fixture =>
      import fixture.*
      val priorEvent = createEvent().futureValueUS
      val validator = mkValidator()
      val sig = sign(
        priorEvent.signedEvent.content.getCryptographicEvidence,
        CantonTimestamp.Epoch,
      ).futureValueUS
      assert(sig != priorEvent.signedEvent.signature)
      val eventWithNewSig =
        priorEvent.copy(signedEvent = priorEvent.signedEvent.copy(signatures = NonEmpty(Seq, sig)))(
          traceContext = fixtureTraceContext
        )
      validator
        .validateOnReconnect(Some(priorEvent), eventWithNewSig, DefaultTestIdentities.sequencerId)
        .valueOrFail("event with regenerated signature")
        .failOnShutdown
        .futureValue
    }

    "accept a different serialization of the same content" in { fixture =>
      import fixture.*
      val deliver1 = createEventWithCounterAndTs(1L, CantonTimestamp.Epoch).futureValueUS
      val deliver2 = createEventWithCounterAndTs(
        1L,
        CantonTimestamp.Epoch,
        customSerialization = Some(ByteString.copyFromUtf8("Different serialization")),
      ).futureValueUS // changing serialization, but not the contents

      val validator = mkValidator()
      validator
        .validateOnReconnect(
          Some(deliver1),
          deliver2.asSequencedSerializedEvent,
          DefaultTestIdentities.sequencerId,
        )
        .valueOrFail("Different serialization should be accepted")
        .failOnShutdown
        .futureValue
    }

    "check the synchronizer id" in { fixture =>
      import fixture.*
      val incorrectSynchronizerId =
        SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("wrong-synchronizer::id"))
      val validator = mkValidator()
      val wrongSynchronizer = createEvent(incorrectSynchronizerId).futureValueUS
      val err = validator
        .validateOnReconnect(
          Some(
            IgnoredSequencedEvent(
              CantonTimestamp.MinValue,
              SequencerCounter(updatedCounter),
              None,
            )(
              fixtureTraceContext
            )
          ),
          wrongSynchronizer,
          DefaultTestIdentities.sequencerId,
        )
        .leftOrFail("wrong synchronizer id on reconnect")
        .failOnShutdown
        .futureValue

      err shouldBe BadSynchronizerId(defaultSynchronizerId, incorrectSynchronizerId)
    }

    "check for a fork" in { fixture =>
      import fixture.*

      def expectLog[E](
          cmd: => FutureUnlessShutdown[SequencedEventValidationError[E]]
      ): SequencedEventValidationError[E] =
        loggerFactory
          .assertLogs(cmd, _.shouldBeCantonErrorCode(ResilientSequencerSubscription.ForkHappened))
          .failOnShutdown
          .futureValue

      val priorEvent = createEvent(timestamp = CantonTimestamp.Epoch).futureValueUS
      val validator = mkValidator()
      val differentTimestamp = createEvent(timestamp = CantonTimestamp.MaxValue).futureValueUS
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
      ).futureValueUS

      val errContent = expectLog(
        validator
          .validateOnReconnect(
            Some(priorEvent),
            differentContent.asSequencedSerializedEvent,
            DefaultTestIdentities.sequencerId,
          )
          .leftOrFail("fork on content")
      )

      def assertFork[E](err: SequencedEventValidationError[E])(
          timestamp: CantonTimestamp,
          suppliedEvent: SequencedEvent[ClosedEnvelope],
          expectedEvent: Option[SequencedEvent[ClosedEnvelope]],
      ): Assertion =
        err match {
          case ForkHappened(timestampRes, suppliedEventRes, expectedEventRes) =>
            (
              timestamp,
              suppliedEvent,
              expectedEvent,
            ) shouldBe (timestampRes, suppliedEventRes, expectedEventRes)
          case x => fail(s"$x is not ForkHappened")
        }

      assertFork(errTimestamp)(
        CantonTimestamp.Epoch,
        differentTimestamp.signedEvent.content,
        Some(priorEvent.signedEvent.content),
      )

      assertFork(errContent)(
        CantonTimestamp.Epoch,
        differentContent.signedEvent.content,
        Some(priorEvent.signedEvent.content),
      )
    }

    "verify the signature" in { fixture =>
      import fixture.*
      val priorEvent = createEvent(previousTimestamp = Some(CantonTimestamp.MinValue)).futureValueUS
      val badSig =
        sign(ByteString.copyFromUtf8("not-the-message"), CantonTimestamp.Epoch).futureValueUS
      val badEvent = createEvent(
        signatureOverride = Some(badSig),
        previousTimestamp = Some(CantonTimestamp.MinValue),
      ).futureValueUS
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
    "reject messages with unexpected synchronizer ids" in { fixture =>
      import fixture.*
      val incorrectSynchronizerId =
        SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("wrong-synchronizer::id"))
      val event = createEvent(incorrectSynchronizerId, counter = 0L).futureValueUS
      val validator = mkValidator()
      val result = validator
        .validate(None, event, DefaultTestIdentities.sequencerId)
        .leftOrFail("wrong synchronizer id")
        .failOnShutdown
        .futureValue
      result shouldBe BadSynchronizerId(`defaultSynchronizerId`, `incorrectSynchronizerId`)
    }

    "reject messages with invalid signatures" in { fixture =>
      import fixture.*
      val priorEvent =
        createEvent(
          previousTimestamp = Some(CantonTimestamp.MinValue),
          timestamp = CantonTimestamp.Epoch.immediatePredecessor,
          counter = 42L,
        ).futureValueUS
      val badSig =
        sign(ByteString.copyFromUtf8("not-the-message"), CantonTimestamp.Epoch).futureValueUS
      val badEvent = createEvent(
        previousTimestamp = Some(priorEvent.timestamp),
        signatureOverride = Some(badSig),
        timestamp = CantonTimestamp.Epoch.immediateSuccessor.immediateSuccessor,
        counter = 43L,
      ).futureValueUS
      val validator = mkValidator()
      val result = validator
        .validate(Some(priorEvent), badEvent, DefaultTestIdentities.sequencerId)
        .leftOrFail("invalid signature")
        .failOnShutdown
        .futureValue
      result shouldBe a[SignatureInvalid]
    }

    "validate correctly with explicit topology timestamp" in { fixture =>
      import fixture.*
      val syncCrypto = mock[SynchronizerCryptoClient]
      when(syncCrypto.pureCrypto).thenReturn(subscriberCryptoApi.pureCrypto)
      when(syncCrypto.snapshot(timestamp = ts(1))(fixtureTraceContext))
        .thenAnswer[CantonTimestamp](tm => subscriberCryptoApi.snapshot(tm)(fixtureTraceContext))
      when(syncCrypto.topologyKnownUntilTimestamp).thenReturn(CantonTimestamp.MaxValue)
      val validator = mkValidator(syncCryptoApi = syncCrypto)
      val priorEvent =
        IgnoredSequencedEvent(
          previousTimestamp = Some(CantonTimestamp.MinValue), // PT=None skips the signature check
          timestamp = ts(0),
          counter = SequencerCounter(41),
          underlying = None,
        )(fixtureTraceContext)
      val deliver =
        createEventWithCounterAndTs(
          previousTimestamp = Some(priorEvent.timestamp),
          timestamp = ts(2),
          counter = 42,
          topologyTimestampO = Some(ts(1)),
        ).futureValueUS

      valueOrFail(
        validator.validate(
          Some(priorEvent),
          deliver.asSequencedSerializedEvent,
          DefaultTestIdentities.sequencerId,
        )
      )(
        "validate"
      ).failOnShutdown.futureValue
    }

    "reject the same previous timestamp, timestamp if passed in repeatedly" in { fixture =>
      import fixture.*
      val priorEvent =
        IgnoredSequencedEvent(
          previousTimestamp = Some(CantonTimestamp.MinValue),
          timestamp = CantonTimestamp.Epoch,
          counter = SequencerCounter(41),
          underlying = None,
        )(
          fixtureTraceContext
        )
      val validator = mkValidator()

      val deliver = createEventWithCounterAndTs(
        counter = 42,
        timestamp = CantonTimestamp.ofEpochSecond(1),
        previousTimestamp = Some(priorEvent.timestamp),
      ).futureValueUS
      validator
        .validate(
          Some(priorEvent),
          deliver.asSequencedSerializedEvent,
          DefaultTestIdentities.sequencerId,
        )
        .valueOrFail("validate1")
        .failOnShutdown
        .futureValue
      val err = validator
        .validate(
          Some(deliver),
          deliver.asSequencedSerializedEvent,
          DefaultTestIdentities.sequencerId,
        )
        .leftOrFail("validate2")
        .failOnShutdown
        .futureValue

      err shouldBe PreviousTimestampMismatch(deliver.previousTimestamp, Some(deliver.timestamp))
    }

    "fail if the timestamp do not increase" in { fixture =>
      import fixture.*
      val priorEvent =
        IgnoredSequencedEvent(
          previousTimestamp = Some(CantonTimestamp.MinValue.immediateSuccessor),
          timestamp = CantonTimestamp.Epoch,
          counter = SequencerCounter(41),
          underlying = None,
        )(
          fixtureTraceContext
        )
      val validator =
        mkValidator()

      val deliver = createEventWithCounterAndTs(
        previousTimestamp = Some(priorEvent.timestamp),
        timestamp = CantonTimestamp.MinValue,
        counter = 42L,
      ).futureValueUS

      val error = validator
        .validate(
          Some(priorEvent),
          deliver.asSequencedSerializedEvent,
          DefaultTestIdentities.sequencerId,
        )
        .leftOrFail("deliver1")
        .failOnShutdown
        .futureValue

      error shouldBe NonIncreasingTimestamp(
        newTimestamp = CantonTimestamp.MinValue,
        newPreviousTimestamp = Some(priorEvent.timestamp),
        oldTimestamp = CantonTimestamp.Epoch,
        oldPreviousTimestamp = Some(CantonTimestamp.MinValue.immediateSuccessor),
      )
    }

    "fail if there is a previous timestamp mismatch" in { fixture =>
      import fixture.*
      val priorEventIgnore0 =
        IgnoredSequencedEvent(
          previousTimestamp = None,
          timestamp = CantonTimestamp.Epoch,
          counter = SequencerCounter(41),
          underlying = None,
        )(fixtureTraceContext)
      val validator = mkValidator()

      val deliver1 =
        createEventWithCounterAndTs(
          previousTimestamp = Some(CantonTimestamp.Epoch),
          timestamp = CantonTimestamp.ofEpochSecond(1),
          counter = 42L,
        ).futureValueUS
      val deliver2 =
        createEventWithCounterAndTs(
          previousTimestamp = Some(CantonTimestamp.ofEpochSecond(1)),
          timestamp = CantonTimestamp.ofEpochSecond(2),
          counter = 43L,
        ).futureValueUS
      val deliver3 =
        createEventWithCounterAndTs(
          previousTimestamp = Some(CantonTimestamp.ofEpochSecond(2)),
          timestamp = CantonTimestamp.ofEpochSecond(3),
          counter = 44L,
        ).futureValueUS

      val result1 = validator
        .validate(
          priorEventO = Some(priorEventIgnore0),
          event = deliver2.asSequencedSerializedEvent,
          DefaultTestIdentities.sequencerId,
        )
        .leftOrFail("deliver1")
        .failOnShutdown
        .futureValue

      validator
        .validate(
          Some(priorEventIgnore0),
          deliver1.asSequencedSerializedEvent,
          DefaultTestIdentities.sequencerId,
        )
        .valueOrFail("deliver2")
        .failOnShutdown
        .futureValue

      val result3 = validator
        .validate(
          Some(deliver1),
          deliver3.asSequencedSerializedEvent,
          DefaultTestIdentities.sequencerId,
        )
        .leftOrFail("deliver3")
        .failOnShutdown
        .futureValue

      result1 shouldBe PreviousTimestampMismatch(
        receivedPreviousTimestamp = deliver2.previousTimestamp,
        expectedPreviousTimestamp = Some(priorEventIgnore0.timestamp),
      )
      result3 shouldBe PreviousTimestampMismatch(
        receivedPreviousTimestamp = deliver3.previousTimestamp,
        expectedPreviousTimestamp = Some(deliver1.timestamp),
      )
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
      validatedEventsF.futureValue.map(_.value) shouldBe Seq(
        Left(UpstreamSubscriptionError("error1"))
      )
    }

    "deal with stuttering" in { fixture =>
      import fixture.*

      val validator = mkValidator()
      val deliver1 = createEventWithCounterAndTs(
        counter = 42L,
        timestamp = CantonTimestamp.Epoch,
        previousTimestamp = None,
      ).futureValueUS
      val deliver2 =
        createEventWithCounterAndTs(
          counter = 43L,
          timestamp = CantonTimestamp.ofEpochSecond(1),
          previousTimestamp = Some(deliver1.timestamp),
        ).futureValueUS
      val deliver3 =
        createEventWithCounterAndTs(
          counter = 44L,
          timestamp = CantonTimestamp.ofEpochSecond(2),
          previousTimestamp = Some(deliver2.timestamp),
        ).futureValueUS

      val source = Source(
        Seq(deliver1, deliver1, deliver2, deliver2, deliver2, deliver3).map(event =>
          withNoOpKillSwitch(Either.right(event.asSequencedSerializedEvent))
        )
      ).watchTermination()((_, doneF) => noOpKillSwitch -> doneF)
      val subscription = SequencerSubscriptionPekko[String](source, alwaysHealthyComponent)
      val validatedSubscription =
        validator.validatePekko(
          subscription,
          Some(deliver1.asSequencedSerializedEvent),
          DefaultTestIdentities.sequencerId,
        )
      val validatedEventsF = validatedSubscription.source.runWith(Sink.seq)
      // deliver1 should be filtered out because it's the prior event
      validatedEventsF.futureValue.map(_.value) shouldBe Seq(
        Right(deliver2.asSequencedSerializedEvent),
        Right(deliver3.asSequencedSerializedEvent),
      )
    }

    "stop upon a validation error" in { fixture =>
      import fixture.*

      val validator = mkValidator()
      val deliver1 = createEventWithCounterAndTs(
        counter = 1L,
        timestamp = CantonTimestamp.Epoch,
        previousTimestamp = None,
      ).futureValueUS
      val deliver2 = createEventWithCounterAndTs(
        counter = 2L,
        timestamp = CantonTimestamp.ofEpochSecond(1),
        previousTimestamp = Some(CantonTimestamp.Epoch),
      ).futureValueUS
      val deliver3 = createEventWithCounterAndTs(
        counter = 4L,
        timestamp = CantonTimestamp.ofEpochSecond(3),
        previousTimestamp = Some(CantonTimestamp.ofEpochSecond(2)),
      ).futureValueUS
      val deliver4 = createEventWithCounterAndTs(
        counter = 5L,
        timestamp = CantonTimestamp.ofEpochSecond(4),
        previousTimestamp = Some(CantonTimestamp.ofEpochSecond(3)),
      ).futureValueUS

      val source = Source(
        Seq(deliver1, deliver2, deliver3, deliver4).map(event =>
          withNoOpKillSwitch(Right(event.asSequencedSerializedEvent))
        )
      ).watchTermination()((_, doneF) => noOpKillSwitch -> doneF)
      val subscription = SequencerSubscriptionPekko(source, alwaysHealthyComponent)
      val validatedSubscription =
        validator.validatePekko(
          subscription,
          Some(deliver1.asSequencedSerializedEvent),
          DefaultTestIdentities.sequencerId,
        )
      val validatedEventsF = validatedSubscription.source.runWith(Sink.seq)
      // deliver1 should be filtered out because it's the prior event
      validatedEventsF.futureValue.map(_.value) shouldBe Seq(
        Right(deliver2.asSequencedSerializedEvent),
        Left(
          PreviousTimestampMismatch(
            receivedPreviousTimestamp = deliver3.previousTimestamp,
            expectedPreviousTimestamp = Some(deliver2.timestamp),
          )
        ),
      )
    }

    "stop upon a validation error on reconnect" in { fixture =>
      import fixture.*

      val validator = mkValidator()
      val deliver1 = createEventWithCounterAndTs(
        counter = 1L,
        timestamp = CantonTimestamp.Epoch,
        previousTimestamp = None,
      ).futureValueUS
      // Forked event, the fork is on the previous timestamp field
      val deliver1a =
        createEventWithCounterAndTs(
          counter = 1L,
          timestamp = CantonTimestamp.Epoch,
          previousTimestamp = Some(CantonTimestamp.MinValue),
        ).futureValueUS
      val deliver2 = createEventWithCounterAndTs(
        counter = 2L,
        timestamp = CantonTimestamp.ofEpochSecond(1),
        previousTimestamp = Some(CantonTimestamp.Epoch),
      ).futureValueUS

      val source = Source(
        Seq(deliver1, deliver2).map(event =>
          withNoOpKillSwitch(Right(event.asSequencedSerializedEvent))
        )
      ).watchTermination()((_, doneF) => noOpKillSwitch -> doneF)
      val subscription = SequencerSubscriptionPekko(source, alwaysHealthyComponent)
      val validatedSubscription =
        validator.validatePekko(
          subscription,
          Some(deliver1a.asSequencedSerializedEvent),
          DefaultTestIdentities.sequencerId,
        )
      loggerFactory.assertLogs(
        validatedSubscription.source.runWith(Sink.seq).futureValue.map(_.value) shouldBe Seq(
          Left(
            ForkHappened(
              CantonTimestamp.Epoch,
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
        .forOwnerAndSynchronizer(
          subscriberId,
          defaultSynchronizerId,
          CantonTimestamp.ofEpochSecond(2),
        )
      val validator = mkValidator(syncCryptoApi)
      val deliver1 = createEventWithCounterAndTs(
        counter = 1L,
        timestamp = CantonTimestamp.Epoch,
        previousTimestamp = None,
      ).futureValueUS
      val deliver2 = createEventWithCounterAndTs(
        counter = 2L,
        timestamp = CantonTimestamp.ofEpochSecond(1),
        previousTimestamp = Some(CantonTimestamp.Epoch),
      ).futureValueUS
      val deliver3 = createEventWithCounterAndTs(
        counter = 4L,
        timestamp = CantonTimestamp.ofEpochSecond(3),
        previousTimestamp = Some(CantonTimestamp.ofEpochSecond(2)),
      ).futureValueUS
      val deliver4 =
        createEventWithCounterAndTs(
          counter = 5L,
          timestamp = CantonTimestamp.ofEpochSecond(300),
          previousTimestamp = Some(CantonTimestamp.ofEpochSecond(3)),
        ).futureValueUS

      // sanity-check that the topology for deliver4 is really not available
      SyncCryptoClient
        .getSnapshotForTimestamp(
          syncCryptoApi,
          deliver4.timestamp,
          Some(deliver3.timestamp),
          testedProtocolVersion,
          warnIfApproximate = false,
        )
        .failOnShutdown
        .failed
        .futureValue shouldBe a[IllegalArgumentException]

      val source = Source(
        Seq(deliver1, deliver2, deliver3, deliver4).map(event =>
          withNoOpKillSwitch(Right(event.asSequencedSerializedEvent))
        )
      ).watchTermination()((_, doneF) => noOpKillSwitch -> doneF)
      val subscription = SequencerSubscriptionPekko(source, alwaysHealthyComponent)
      val validatedSubscription =
        validator.validatePekko(
          subscription,
          Some(deliver1.asSequencedSerializedEvent),
          DefaultTestIdentities.sequencerId,
        )
      val ((killSwitch, doneF), validatedEventsF) =
        validatedSubscription.source.toMat(Sink.seq)(Keep.both).run()
      // deliver1 should be filtered out because it's the prior event
      validatedEventsF.futureValue.map(_.value) shouldBe Seq(
        Right(deliver2.asSequencedSerializedEvent),
        Left(
          PreviousTimestampMismatch(
            receivedPreviousTimestamp = deliver3.previousTimestamp,
            expectedPreviousTimestamp = Some(deliver2.timestamp),
          )
        ),
      )
      killSwitch.shutdown()
      doneF.futureValue
    }
  }

}
