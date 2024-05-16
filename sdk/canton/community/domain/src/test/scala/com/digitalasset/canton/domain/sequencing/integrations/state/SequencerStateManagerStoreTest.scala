// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.integrations.state

import cats.syntax.either.*
import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{HashPurpose, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.domain.sequencing.sequencer.store.SaveLowerBoundError.BoundLowerThanExisting
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InFlightAggregation,
  InternalSequencerPruningStatus,
  SequencerMemberStatus,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, SequencerTestUtils}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  Member,
  ParticipantId,
  SequencerGroup,
  TestingTopology,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAsyncWordSpec, SequencerCounter}
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

trait SequencerStateManagerStoreTest
    extends AsyncWordSpec
    with BaseTest
    with BeforeAndAfterAll
    with ProtocolVersionChecksAsyncWordSpec {

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private var actorSystem: ActorSystem = _
  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private var materializer: Materializer = _
  private lazy val domainId = DefaultTestIdentities.domainId
  private lazy val syncCryptoApi =
    TestingTopology(
      domains = Set(domainId),
      sequencerGroup = SequencerGroup(
        active = NonEmpty.mk(Seq, DefaultTestIdentities.daSequencerId),
        passive = Seq.empty,
        threshold = PositiveInt.one,
      ),
    )
      .build()
      .forOwnerAndDomain(DefaultTestIdentities.daSequencerId, domainId)
      .currentSnapshotApproximation

  // we don't do any signature verification in these tests so any signature that will deserialize with the testing crypto api is fine
  private lazy val signature = {
    val hash =
      syncCryptoApi.pureCrypto.digest(
        HashPurpose.SequencedEventSignature,
        ByteString.copyFromUtf8("signature"),
      )
    Await
      .result(syncCryptoApi.sign(hash).value, 10.seconds)
      .failOnShutdown
      .valueOr(err => fail(err.toString))
  }

  override def beforeAll(): Unit = {
    actorSystem = ActorSystem("sequencer-store-test")
    materializer = Materializer(actorSystem)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try super.afterAll()
    finally {
      materializer.shutdown()
      Await.result(actorSystem.terminate(), 10.seconds)
    }
  }

  def sequencerStateManagerStore(mk: () => SequencerStateManagerStore): Unit = {
    val alice = ParticipantId(UniqueIdentifier.tryCreate("participant", "alice"))
    val bob = ParticipantId(UniqueIdentifier.tryCreate("participant", "bob"))
    val carlos = ParticipantId(UniqueIdentifier.tryCreate("participant", "carlos"))
    val message = ByteString.copyFromUtf8("test-message")

    def ts(epochSeconds: Int): CantonTimestamp =
      CantonTimestamp.Epoch.plusSeconds(epochSeconds.toLong)

    val t1 = ts(1)
    val t2 = ts(2)
    val t3 = ts(3)
    val t4 = ts(4)

    "read at timestamp" should {
      "hydrate a correct empty state when there have been no updates" in {
        val store = mk()
        for {
          head <- store.readAtBlockTimestamp(CantonTimestamp.Epoch)
        } yield {
          head.registeredMembers shouldBe empty
          head.checkpoints shouldBe empty
        }
      }

      "hydrate correct state from previous updates" in
        withNewTraceContext { implicit traceContext =>
          val store = mk()

          for {
            _ <- store.addMember(alice, t1)
            _ <- store.addMember(bob, t1)
            _ <- store.addEvents(
              Map(alice -> send(alice, SequencerCounter(0), t1, message)),
              Map.empty,
            )
            _ <- store.addEvents(
              Map(
                alice -> mockDeliver(1, t2),
                bob -> mockDeliver(0, t2),
              ),
              Map.empty,
            )
            _ <- store.addMember(carlos, t2)
            stateAtT1 <- store.readAtBlockTimestamp(t1)
            head <- store.readAtBlockTimestamp(t2)
          } yield {
            stateAtT1.registeredMembers should contain.only(alice, bob)
            stateAtT1.headCounter(alice) should contain(SequencerCounter(0))
            stateAtT1.checkpoints.keys should contain only alice

            head.registeredMembers should contain.only(alice, bob, carlos)
            head.headCounter(alice) should contain(SequencerCounter(1))
            head.headCounter(bob) should contain(SequencerCounter(0))
            head.checkpoints.keys should not contain carlos
          }
        }

      "hydrate traffic state from previous updates" in
        withNewTraceContext { implicit traceContext =>
          val store = mk()
          val trafficStateAlice = TrafficState(
            NonNegativeLong.tryCreate(5L),
            NonNegativeLong.tryCreate(6L),
            NonNegativeLong.tryCreate(7L),
            t1,
          )
          val trafficStateAlice2 =
            trafficStateAlice.copy(
              extraTrafficRemainder = NonNegativeLong.tryCreate(64L),
              timestamp = t2,
            )
          val trafficStateBob =
            trafficStateAlice.copy(
              extraTrafficRemainder = NonNegativeLong.tryCreate(54L),
              timestamp = t2,
            )
          for {
            _ <- store.addMember(alice, t1)
            _ <- store.addMember(bob, t1)
            _ <- store.addEvents(
              Map(alice -> send(alice, SequencerCounter(0), t1, message)),
              Map(alice -> trafficStateAlice),
            )
            _ <- store.addEvents(
              Map(
                alice -> mockDeliver(1, t2),
                bob -> mockDeliver(0, t2),
              ),
              Map(alice -> trafficStateAlice2, bob -> trafficStateBob),
            )
            _ <- store.addMember(carlos, t2)
            stateAtT1 <- store.readAtBlockTimestamp(t1)
            head <- store.readAtBlockTimestamp(t2)
          } yield {
            stateAtT1.registeredMembers should contain.only(alice, bob)
            stateAtT1.headCounter(alice) should contain(SequencerCounter(0))
            stateAtT1.checkpoints.keys should contain only alice
            stateAtT1.trafficState(alice) shouldBe trafficStateAlice
            stateAtT1.trafficState.keys should contain only alice

            head.registeredMembers should contain.only(alice, bob, carlos)
            head.headCounter(alice) should contain(SequencerCounter(1))
            head.headCounter(bob) should contain(SequencerCounter(0))
            head.checkpoints.keys should not contain carlos
            head.trafficState(alice) shouldBe trafficStateAlice2
            head.trafficState(bob) shouldBe trafficStateBob
            head.trafficState.keys should not contain carlos
          }
        }

      "take into consideration timestamps of adding members" in withNewTraceContext {
        implicit traceContext =>
          val store = mk()

          for {
            _ <- store.addMember(alice, t1)
            _ <- store.addEvents(
              Map(alice -> send(alice, SequencerCounter(0), t1, message)),
              Map.empty,
            )
            _ <- store.addMember(bob, t2)
            head <- store.readAtBlockTimestamp(t2)
          } yield {
            head.registeredMembers should contain.only(alice, bob)
            head.headCounter(alice) should contain(SequencerCounter(0))
          }
      }

      "reconstruct the aggregation state" in withNewTraceContext { implicit traceContext =>
        val store = mk()
        val aggregationId1 = AggregationId(TestHash.digest(1))
        val aggregationId2 = AggregationId(TestHash.digest(2))
        val aggregationId3 = AggregationId(TestHash.digest(3))
        val rule = AggregationRule(
          NonEmpty(Seq, alice, bob),
          threshold = PositiveInt.tryCreate(2),
          testedProtocolVersion,
        )
        val signatureAlice1 = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureAlice1"),
          alice.fingerprint,
        )
        val signatureAlice2 = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureAlice2"),
          alice.fingerprint,
        )
        val signatureAlice3 = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureAlice3"),
          alice.fingerprint,
        )
        val signatureBob = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureBob"),
          bob.fingerprint,
        )

        val inFlightAggregation1 = InFlightAggregation(
          rule,
          t4,
          alice -> AggregationBySender(
            t2,
            Seq(Seq(signatureAlice1), Seq(signatureAlice2, signatureAlice3)),
          ),
          bob -> AggregationBySender(t3, Seq(Seq(signatureBob), Seq.empty)),
        )
        val inFlightAggregation2 = InFlightAggregation(rule, t3)
        val inFlightAggregation3 = InFlightAggregation(
          rule,
          t4,
          alice -> AggregationBySender(
            t4.immediatePredecessor,
            Seq(Seq(signatureAlice1), Seq.empty, Seq(signatureAlice2)),
          ),
        )

        for {
          _ <- store.addMember(alice, t1)
          _ <- store.addMember(bob, t1)
          _ <- store.addInFlightAggregationUpdates(
            Map(
              aggregationId1 -> inFlightAggregation1.asUpdate,
              aggregationId2 -> inFlightAggregation2.asUpdate,
              aggregationId3 -> inFlightAggregation3.asUpdate,
            )
          )
          head2pred <- store.readAtBlockTimestamp(t2.immediatePredecessor)
          head2 <- store.readAtBlockTimestamp(t2)
          head3 <- store.readAtBlockTimestamp(t3)
          head4pred <- store.readAtBlockTimestamp(t4.immediatePredecessor)
          head4 <- store.readAtBlockTimestamp(t4)
        } yield {
          // All aggregations by sender have later timestamps and the in-flight aggregations are therefore considered inexistent
          head2pred.inFlightAggregations shouldBe Map.empty
          head2.inFlightAggregations shouldBe Map(
            aggregationId1 -> inFlightAggregation1
              .focus(_.aggregatedSenders)
              // bob's aggregation happened later
              .modify(_.removed(bob))
          )
          head3.inFlightAggregations shouldBe Map(
            aggregationId1 -> inFlightAggregation1
          )
          head4pred.inFlightAggregations shouldBe Map(
            aggregationId1 -> inFlightAggregation1,
            // aggregationId2 has already expired
            aggregationId3 -> inFlightAggregation3,
          )
          // All in-flight aggregations have expired
          head4.inFlightAggregations shouldBe Map.empty
        }
      }
    }

    "registering a member" should {
      "include them in registered members" in {
        val store = mk()

        for {
          _ <- store.addMember(alice, t1)
          state <- store.readAtBlockTimestamp(t1)
        } yield {
          state.registeredMembers should contain only alice
        }
      }

      "not immediately give them a head counter as they have no events persisted" in {
        val store = mk()
        for {
          _ <- store.addMember(alice, t1)
          state <- store.readAtBlockTimestamp(t1)
        } yield {
          state.headCounter(alice) shouldBe None
        }
      }
    }

    "disabling a member" should {
      "be able to disable" in {
        val store = mk()
        for {
          _ <- store.addMember(alice, t1)
          state <- store.readAtBlockTimestamp(t1)
          enabled1 <- store.isEnabled(alice)
          _ <- store.disableMember(alice)
          enabled2 <- store.isEnabled(alice)
        } yield {
          state.registeredMembers should contain only alice
          enabled1 shouldBe true
          enabled2 shouldBe false
        }
      }
      "disabled member can still be addressed" in {
        val store = mk()
        for {
          _ <- store.addMember(alice, t1)
          _ <- store.addMember(bob, t1)
          _ <- store.disableMember(bob)
          _ <- store.addEvents(Map(bob -> send(bob, SequencerCounter(0), t2, message)), Map.empty)
          _ <- store.addEvents(Map(bob -> send(bob, SequencerCounter(1), t3, message)), Map.empty)
          state <- store.readAtBlockTimestamp(t3)
        } yield state.headCounter(bob) should contain(SequencerCounter(1))
      }
    }

    "add event" should {
      "throw an error if a counter is invalid" in withNewTraceContext { implicit traceContext =>
        val store = mk()
        loggerFactory.assertInternalError[IllegalArgumentException](
          store.addEvents(Map(alice -> mockDeliver(-1, t1)), Map.empty),
          _.getMessage shouldBe "all counters must be greater or equal to the genesis counter",
        )
        Future.successful(succeed)
      }

      "throw an error if timestamps are different" in withNewTraceContext { implicit traceContext =>
        val store = mk()
        loggerFactory.assertInternalError[IllegalArgumentException](
          store.addEvents(
            Map(
              alice -> mockDeliver(0, t1),
              bob -> mockDeliver(0, t2),
            ),
            Map.empty,
          ),
          _.getMessage shouldBe "events should all be for the same timestamp",
        )
        Future.successful(succeed)
      }
    }

    "read range" should {
      "throw argument exception if start and end are incorrect" in {
        val store = mk()
        loggerFactory.assertInternalError[IllegalArgumentException](
          store.readRange(alice, SequencerCounter(0), SequencerCounter(0)),
          _.getMessage shouldBe "startInclusive must be less than endExclusive",
        )
        Future.successful(succeed)
      }

      "return an empty range if the member is not registered" in {
        val store = mk()

        for {
          items <- rangeToSeq(store.readRange(alice, SequencerCounter(0), SequencerCounter(1)))
        } yield {
          items shouldBe empty
        }
      }

      "return an empty range if a registered member has no events" in {
        val store = mk()

        for {
          _ <- store.addMember(alice, t1)
          items <- rangeToSeq(store.readRange(alice, SequencerCounter(0), SequencerCounter(1)))
        } yield {
          items shouldBe empty
        }
      }

      "replay events correctly" in {
        val store = mk()
        val trafficStateAlice = TrafficState(
          NonNegativeLong.tryCreate(5L),
          NonNegativeLong.tryCreate(6L),
          NonNegativeLong.tryCreate(7L),
          t1,
        )
        val trafficStateAlice2 =
          trafficStateAlice.copy(
            extraTrafficRemainder = NonNegativeLong.tryCreate(64L),
            timestamp = t2,
          )
        for {
          _ <- store.addMember(alice, t1)
          _ <- store.addEvents(
            Map(
              alice -> send(
                alice,
                SequencerCounter(0),
                t1,
                message,
                Some(trafficStateAlice.toSequencedEventTrafficState),
              )
            ),
            Map(alice -> trafficStateAlice),
          )
          _ <- store.addEvents(
            Map(alice -> mockDeliver(1, t2, Some(trafficStateAlice2.toSequencedEventTrafficState))),
            Map(alice -> trafficStateAlice2),
          )
          items <- rangeToSeq(store.readRange(alice, SequencerCounter(0), SequencerCounter(2)))
        } yield {

          items.flatMap(_.trafficState) should contain theSameElementsInOrderAs Seq(
            trafficStateAlice.toSequencedEventTrafficState,
            trafficStateAlice2.toSequencedEventTrafficState,
          )

          items.map(e => e.signedEvent.content) should contain theSameElementsInOrderAs Seq(
            send(alice, SequencerCounter(0), t1, message).signedEvent.content,
            mockDeliver(1, t2).signedEvent.content,
          )
        }
      }
    }

    "acknowledgements" should {
      def acknowledgements(
          status: InternalSequencerPruningStatus
      ): Map[Member, Option[CantonTimestamp]] =
        status.members.map { case SequencerMemberStatus(member, _, lastAcknowledged, _) =>
          member -> lastAcknowledged
        }.toMap

      "latestAcknowledgements should return acknowledgements" in {
        val store = mk()

        for {
          _ <- store.addMember(alice, t1)
          _ <- store.addMember(bob, t2)
          _ <- store.acknowledge(alice, t3)
          latestAcknowledgements <- store.latestAcknowledgements()
        } yield {
          latestAcknowledgements shouldBe Map(alice -> t3)
        }
      }
      "acknowledge should ignore earlier timestamps" in {
        val store = mk()

        for {
          _ <- store.addMember(alice, t1)
          _ <- store.acknowledge(alice, t3)
          _ <- store.acknowledge(alice, t2)
          acknowledgements <- store.status().map(acknowledgements)
        } yield acknowledgements shouldBe Map(
          alice -> Some(t3)
        )
      }
    }

    "lower bound" should {
      "initially be empty" in {
        val store = mk()

        for {
          boundO <- store.fetchLowerBound()
        } yield boundO shouldBe empty
      }

      "return value once saved" in {
        val store = mk()

        for {
          _ <- store.saveLowerBound(t1).valueOrFail("saveLowerBound")
          _ <- store.saveLowerBound(t2).valueOrFail("saveLowerBound")
          fetchedBoundO <- store.fetchLowerBound()
        } yield fetchedBoundO.value shouldBe t2
      }

      "error if set bound is lower than previous bound but not if it is the same" in {
        val store = mk()
        val bound1 = CantonTimestamp.Epoch.plusSeconds(10)
        val bound2 = bound1.plusMillis(-1) // before prior bound

        for {
          fetchedBoundO <- store.fetchLowerBound()
          _ = fetchedBoundO shouldBe None

          _ <- store.saveLowerBound(bound1).valueOrFail("saveLowerBound1")
          _ <- store.saveLowerBound(bound1).valueOrFail("saveLowerBound2")
          error <- leftOrFail(store.saveLowerBound(bound2))("saveLowerBound3")
        } yield {
          error shouldBe BoundLowerThanExisting(bound1, bound2)
        }
      }
    }
    "pruning" should {
      "if data has been acknowledged and watermarked remove some now unnecessary data" in {
        val store = mk()
        val now = ts(10)
        for {
          _ <- store.addMember(alice, t1)
          _ <- store.addEvents(
            Map(alice -> send(alice, SequencerCounter(0), t2, message)),
            Map.empty,
          )
          _ <- store.addMember(bob, t3)
          _ <- store.addEvents(
            Map(
              alice -> mockDeliver(1, ts(5)),
              bob -> mockDeliver(0, ts(5)),
            ),
            Map.empty,
          )
          _ <- store.addEvents(
            Map(
              alice -> mockDeliver(2, ts(6)),
              bob -> mockDeliver(1, ts(6)),
            ),
            Map.empty,
          )
          _ <- store.acknowledge(alice, ts(6))
          _ <- store.acknowledge(bob, ts(6))
          statusBefore <- store.status()
          pruningTimestamp = statusBefore.safePruningTimestampFor(now)
          eventsToBeDeleted <- store.numberOfEventsToBeDeletedByPruneAt(pruningTimestamp)
          result <- {
            logger.debug(s"Pruning sequencer state manager store from $pruningTimestamp")
            store.prune(pruningTimestamp)
          }
          eventsToBeDeletedAfterPruning <- store.numberOfEventsToBeDeletedByPruneAt(
            pruningTimestamp
          )
          statusAfter <- store.status()
          lowerBound <- store.fetchLowerBound()
        } yield {
          result.eventsPruned shouldBe 3L
          eventsToBeDeleted shouldBe 3L
          eventsToBeDeletedAfterPruning shouldBe 0L
          statusBefore.lowerBound shouldBe <(statusAfter.lowerBound)
          lowerBound.value shouldBe ts(6) // to prevent reads from before this point
          result.newMinimumCountersSupported shouldBe Map(
            alice -> SequencerCounter(2),
            bob -> SequencerCounter(1),
          )
        }
      }
      "not worry about ignored members" in {
        val store = mk()
        for {
          _ <- store.addMember(alice, t1)
          _ <- store.addMember(bob, t2)
          _ <- store.addEvents(Map(alice -> mockDeliver(3, ts(3))), Map.empty)
          _ <- store.addEvents(Map(bob -> mockDeliver(5, ts(4))), Map.empty)
          // clients have acknowledgements at different points
          _ <- store.acknowledge(alice, ts(3))
          _ <- store.acknowledge(bob, ts(4))
          _ <- store.disableMember(alice)
          status <- store.status()
          safeTimestamp = status.safePruningTimestampFor(ts(10))
          result <- {
            logger.debug(s"Pruning sequencer state manager store from $safeTimestamp")
            store.prune(safeTimestamp)
          }
        } yield {
          safeTimestamp shouldBe ts(4) // as alice is ignored
          result.eventsPruned shouldBe 1L
          result.newMinimumCountersSupported shouldBe Map(bob -> SequencerCounter(5))
        }
      }
    }

    "aggregation expiry" should {
      "delete all aggregation whose max sequencing time has elapsed" in {
        val store = mk()
        val aggregationId1 = AggregationId(TestHash.digest(1))
        val aggregationId2 = AggregationId(TestHash.digest(2))
        val rule = AggregationRule(
          NonEmpty(Seq, alice, bob, carlos),
          threshold = PositiveInt.tryCreate(2),
          testedProtocolVersion,
        )
        val signatureAlice1 = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureAlice1"),
          alice.fingerprint,
        )
        val signatureAlice2 = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureAlice2"),
          alice.fingerprint,
        )
        val signatureAlice3 = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureAlice3"),
          alice.fingerprint,
        )
        val signatureBob = SymbolicCrypto.signature(
          ByteString.copyFromUtf8("signatureBob"),
          bob.fingerprint,
        )

        val inFlightAggregation1 = InFlightAggregation(
          rule,
          t3,
          alice -> AggregationBySender(
            t1,
            Seq(Seq(signatureAlice1), Seq(signatureAlice2, signatureAlice3)),
          ),
          bob -> AggregationBySender(t2, Seq(Seq(signatureBob), Seq.empty)),
        )
        val inFlightAggregation2 = InFlightAggregation(
          rule,
          t3.immediateSuccessor,
          alice -> AggregationBySender(t2, Seq.fill(3)(Seq.empty)),
        )

        for {
          _ <- store.addMember(alice, t1)
          _ <- store.addMember(bob, t1)
          _ <- store.addInFlightAggregationUpdates(
            Map(
              aggregationId1 -> inFlightAggregation1.asUpdate,
              aggregationId2 -> inFlightAggregation2.asUpdate,
            )
          )
          _ <- store.pruneExpiredInFlightAggregations(t2)
          head2 <- store.readAtBlockTimestamp(t2)
          _ <- store.pruneExpiredInFlightAggregations(t3)
          head3 <- store.readAtBlockTimestamp(t3)
          // We're using here the ability to read actually inconsistent data (for crash recovery)
          // for an already expired block to check that the expiry actually deletes the data.
          head2expired <- store.readAtBlockTimestamp(t2)
        } yield {
          head2.inFlightAggregations shouldBe Map(
            aggregationId1 -> inFlightAggregation1,
            aggregationId2 -> inFlightAggregation2,
          )
          head3.inFlightAggregations shouldBe Map(
            aggregationId2 -> inFlightAggregation2
          )
          head2expired.inFlightAggregations shouldBe Map(
            aggregationId2 -> inFlightAggregation2
          )
        }
      }
    }

    "initial topology timestamp" should {
      "not be set if not specified initially" in {
        val store = mk()
        for {
          tsEmptyStore <- store.getInitialTopologySnapshotTimestamp
          _ <- store.saveLowerBound(t1).valueOrFail("saveLowerBound1")
          tsTopologyTimestampNotSetInitially <- store.getInitialTopologySnapshotTimestamp
          // Now attempt to set an initial onboarding topology timestamp which should be ignored
          _ <- store.saveLowerBound(t2, t1.some).valueOrFail("saveLowerBound2")
          tsTopologyIgnoredOnSubsequentCalls <- store.getInitialTopologySnapshotTimestamp
        } yield {
          tsEmptyStore shouldBe None
          tsTopologyTimestampNotSetInitially shouldBe None
          tsTopologyIgnoredOnSubsequentCalls shouldBe None
        }
      }

      "be set initially and not over-writable thereafter" in {
        val store = mk()
        for {
          tsEmptyStore <- store.getInitialTopologySnapshotTimestamp
          _ <- store.saveLowerBound(t1, t1.some).valueOrFail("saveLowerBound1")
          tsTopologyTimestampSetInitially <- store.getInitialTopologySnapshotTimestamp
          // Now attempt to set an initial onboarding topology timestamp which should be ignored
          _ <- store.saveLowerBound(t2, t2.some).valueOrFail("saveLowerBound2")
          tsTopologyIgnoredOnSubsequentCalls <- store.getInitialTopologySnapshotTimestamp
        } yield {
          tsEmptyStore shouldBe None
          tsTopologyTimestampSetInitially shouldBe t1.some
          tsTopologyIgnoredOnSubsequentCalls shouldBe t1.some
        }
      }
    }

    "handle tombstones" should {
      "persist and retrieve a tombstone" in {
        val store = mk()
        for {
          _ <- store.addMember(alice, t1)
          _ <- store.addEvents(
            Map(alice -> mockTombstone(SequencerCounter(1), ts(3))),
            Map.empty,
          )
          eventOrTombstone <- rangeToSeq(
            store.readRange(alice, SequencerCounter(1), SequencerCounter(2))
          )
        } yield {
          eventOrTombstone.size shouldBe 1
          eventOrTombstone.head.signedEvent.content match {
            case error: DeliverError =>
              error.timestamp shouldBe ts(3)
              error.counter shouldBe SequencerCounter(1)
              error.reason.message should include("Sequencer signing key not available")
            case event =>
              fail(s"Expected tombstone, but got ${event}")
          }
        }
      }
    }

    def send(
        recipient: Member,
        counter: SequencerCounter,
        ts: CantonTimestamp,
        message: ByteString,
        trafficState: Option[SequencedEventTrafficState] = None,
    ): OrdinarySerializedEvent =
      OrdinarySequencedEvent(
        SignedContent(
          Deliver.create(
            counter,
            ts,
            domainId,
            Some(MessageId.tryCreate(s"$recipient-$counter")),
            Batch(
              List(
                ClosedEnvelope
                  .create(message, Recipients.cc(recipient), Seq.empty, testedProtocolVersion)
              ),
              testedProtocolVersion,
            ),
            None,
            testedProtocolVersion,
          ),
          signature,
          None,
          testedProtocolVersion,
        ),
        trafficState,
      )(TraceContext.empty)

    def mockDeliver(
        sc: Long,
        ts: CantonTimestamp,
        trafficState: Option[SequencedEventTrafficState] = None,
    ): OrdinarySerializedEvent =
      OrdinarySequencedEvent(
        SignedContent(
          SequencerTestUtils.mockDeliver(sc, ts, domainId),
          signature,
          None,
          testedProtocolVersion,
        ),
        trafficState,
      )(TraceContext.empty)

    def mockTombstone(
        sc: SequencerCounter,
        ts: CantonTimestamp,
    ): OrdinarySerializedEvent =
      OrdinarySequencedEvent(
        SignedContent(
          DeliverError.create(
            sc,
            ts,
            domainId,
            MessageId(String73.tryCreate("tombstone")),
            SequencerErrors.PersistTombstone(ts, sc),
            testedProtocolVersion,
          ),
          signature,
          None,
          testedProtocolVersion,
        ),
        None,
      )(TraceContext.empty)

    def rangeToSeq(
        range: Source[OrdinarySerializedEvent, NotUsed]
    ): Future[Seq[OrdinarySerializedEvent]] =
      range.runWith(Sink.seq)(this.materializer)
  }
}
