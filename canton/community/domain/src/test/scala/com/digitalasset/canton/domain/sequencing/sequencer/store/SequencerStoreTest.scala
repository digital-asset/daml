// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import cats.data.EitherT
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.CantonRequireTypes.String256M
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.DomainSequencingTestUtils.mockDeliverStoreEvent
import com.digitalasset.canton.domain.sequencing.sequencer.store.SaveLowerBoundError.BoundLowerThanExisting
import com.digitalasset.canton.domain.sequencing.sequencer.{
  CommitMode,
  DomainSequencingTestUtils,
  SequencerMemberStatus,
  SequencerPruningStatus,
}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.sequencing.protocol.{MessageId, SequencerErrors}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{
  Member,
  ParticipantId,
  UnauthenticatedMemberId,
  UniqueIdentifier,
}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAsyncWordSpec, SequencerCounter}
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.annotation.nowarn
import scala.collection.immutable.SortedSet
import scala.concurrent.Future

@nowarn("msg=match may not be exhaustive")
trait SequencerStoreTest
    extends AsyncWordSpec
    with BaseTest
    with HasCloseContext
    with FlagCloseable
    with ProtocolVersionChecksAsyncWordSpec {
  def sequencerStore(mk: () => SequencerStore): Unit = {

    val instanceIndex: Int = 0

    val alice: Member = ParticipantId("alice")
    val bob: Member = ParticipantId("bob")
    val carole: Member = ParticipantId("carole")

    def ts(epochSeconds: Int): CantonTimestamp =
      CantonTimestamp.Epoch.plusSeconds(epochSeconds.toLong)
    val ts1 = ts(1)
    val ts2 = ts(2)
    val ts3 = ts(3)

    val payloadBytes1 = ByteString.copyFromUtf8("1")
    val payloadBytes2 = ByteString.copyFromUtf8("1")
    val payload1 = Payload(PayloadId(ts1), payloadBytes1)
    val payload2 = Payload(PayloadId(ts2), payloadBytes2)
    val payload3 = Payload(PayloadId(ts3), payloadBytes2)
    val messageId1 = MessageId.tryCreate("1")
    val messageId2 = MessageId.tryCreate("2")
    val messageId3 = MessageId.tryCreate("3")

    val instanceDiscriminator1 = UUID.randomUUID()
    val instanceDiscriminator2 = UUID.randomUUID()

    final case class Env(store: SequencerStore = mk()) {

      def deliverEventWithDefaults(
          ts: CantonTimestamp,
          sender: SequencerMemberId = SequencerMemberId(0),
      )(
          recipients: NonEmpty[SortedSet[SequencerMemberId]] = NonEmpty(SortedSet, sender)
      ): Sequenced[PayloadId] =
        Sequenced(
          ts,
          mockDeliverStoreEvent(
            sender = sender,
            payloadId = PayloadId(ts),
            traceContext = traceContext,
          )(
            recipients
          ),
        )

      def deliverEvent(
          ts: CantonTimestamp,
          sender: Member,
          messageId: MessageId,
          payloadId: PayloadId,
          recipients: Set[Member] = Set.empty,
      ): Future[Sequenced[PayloadId]] =
        for {
          senderId <- store.registerMember(sender, ts)
          recipientIds <- recipients.toList.parTraverse(store.registerMember(_, ts)).map(_.toSet)
        } yield Sequenced(
          ts,
          DeliverStoreEvent(
            senderId,
            messageId,
            NonEmpty(SortedSet, senderId, recipientIds.toSeq: _*),
            payloadId,
            None,
            traceContext,
          ),
        )

      def lookupRegisteredMember(member: Member): Future[SequencerMemberId] =
        for {
          registeredMemberO <- store.lookupMember(member)
          memberId = registeredMemberO.map(_.memberId).getOrElse(fail(s"$member is not registered"))
        } yield memberId

      def readEvents(
          member: Member,
          fromTimestampO: Option[CantonTimestamp] = None,
          limit: Int = 1000,
      ): Future[Seq[Sequenced[Payload]]] =
        for {
          memberId <- lookupRegisteredMember(member)
          events <- store.readEvents(memberId, fromTimestampO, limit)
        } yield events.payloads

      def assertDeliverEvent(
          event: Sequenced[Payload],
          expectedTimestamp: CantonTimestamp,
          expectedSender: Member,
          expectedMessageId: MessageId,
          expectedRecipients: Set[Member],
          expectedPayload: Payload,
          expectedSigningTimestamp: Option[CantonTimestamp] = None,
      ): Future[Assertion] = {
        for {
          senderId <- lookupRegisteredMember(expectedSender)
          recipientIds <- expectedRecipients.toList.parTraverse(lookupRegisteredMember).map(_.toSet)
        } yield {
          event.timestamp shouldBe expectedTimestamp
          event.event match {
            case DeliverStoreEvent(
                  sender,
                  messageId,
                  recipients,
                  payload,
                  signingTimestampO,
                  traceContext,
                ) =>
              sender shouldBe senderId
              messageId shouldBe expectedMessageId
              recipients.forgetNE should contain.only(recipientIds.toSeq: _*)
              payload shouldBe expectedPayload
              signingTimestampO shouldBe expectedSigningTimestamp
            case other =>
              fail(s"Expected deliver event but got $other")
          }
        }
      }

      /** Save payloads using the default `instanceDiscriminator1` and expecting it to succeed */
      def savePayloads(payloads: NonEmpty[Seq[Payload]]): Future[Unit] =
        valueOrFail(store.savePayloads(payloads, instanceDiscriminator1))("savePayloads")

      def saveWatermark(ts: CantonTimestamp): EitherT[Future, SaveWatermarkError, Unit] =
        store.saveWatermark(instanceIndex, ts)
    }

    def checkpoint(
        counter: SequencerCounter,
        ts: CantonTimestamp,
        latestTopologyClientTs: Option[CantonTimestamp] = None,
    ): CounterCheckpoint =
      CounterCheckpoint(counter, ts, latestTopologyClientTs)

    "DeliverErrorStoreEvent" should {
      "be able to serialize to and deserialize the error from protobuf" onlyRunWithOrGreaterThan ProtocolVersion.CNTestNet in {
        val error = SequencerErrors.SigningTimestampTooEarly("too early!")
        val errorStatus = error.rpcStatusWithoutLoggingContext()
        val (message, serialized) =
          DeliverErrorStoreEvent.serializeError(error, testedProtocolVersion)
        val deserialized =
          DeliverErrorStoreEvent.deserializeError(message, serialized, testedProtocolVersion)
        deserialized shouldBe Right(errorStatus)
      }
    }

    "member registration" should {
      "be able to register a new member" in {
        val store = mk()
        for {
          id <- store.registerMember(alice, ts1)
          fetchedId <- store.lookupMember(alice)
        } yield fetchedId.value shouldBe RegisteredMember(id, ts1)
      }

      "lookup should return none if member is not registered" in {
        val store = mk()
        for {
          fetched <- store.lookupMember(alice)
        } yield fetched shouldBe None
      }

      "registering a member twice should just return the same id but the first timestamp should be kept" in {
        val store = mk()

        for {
          id1 <- store.registerMember(alice, ts1)
          id2 <- store.registerMember(alice, ts2)
          registeredMember <- store.lookupMember(alice)
        } yield {
          id2 shouldEqual id1
          registeredMember.value shouldBe RegisteredMember(id1, ts1)
        }
      }
    }

    "reading and writing" should {
      "deliver events should include associated payloads when read" in {
        val env = Env()

        for {
          _ <- env.savePayloads(NonEmpty(Seq, payload1, payload2))
          deliverEvent1 <- env.deliverEvent(ts1, alice, messageId1, payload1.id)
          deliverEvent2 <- env.deliverEvent(ts2, alice, messageId2, payload2.id)
          _ <- env.store.saveEvents(instanceIndex, NonEmpty(Seq, deliverEvent1, deliverEvent2))
          _ <- env.saveWatermark(deliverEvent2.timestamp).valueOrFail("saveWatermark")
          events <- env.readEvents(alice)
          _ = events should have size 2
          Seq(event1, event2) = events
          _ <- env.assertDeliverEvent(event1, ts1, alice, messageId1, Set(alice), payload1)
          _ <- env.assertDeliverEvent(event2, ts2, alice, messageId2, Set(alice), payload2)
        } yield succeed
      }

      "filter correctly by recipient" in {
        val env = Env()

        for {
          _ <- env.savePayloads(NonEmpty(Seq, payload1, payload2))
          // the first event is for alice, and the second for bob
          deliverEvent1 <- env.deliverEvent(ts1, alice, messageId1, payload1.id)
          deliverEvent2 <- env.deliverEvent(ts2, bob, messageId2, payload2.id)
          _ <- env.store.saveEvents(instanceIndex, NonEmpty(Seq, deliverEvent1, deliverEvent2))
          _ <- env.saveWatermark(deliverEvent2.timestamp).valueOrFail("saveWatermark")
          aliceEvents <- env.readEvents(alice)
          bobEvents <- env.readEvents(bob)
          _ = aliceEvents should have size 1
          _ = bobEvents should have size 1
          _ <- env.assertDeliverEvent(
            aliceEvents.headOption.value,
            ts1,
            alice,
            messageId1,
            Set(alice),
            payload1,
          )
          _ <- env.assertDeliverEvent(
            bobEvents.headOption.value,
            ts2,
            bob,
            messageId2,
            Set(bob),
            payload2,
          )
        } yield succeed
      }

      "events are delivered correctly to single and multiple recipients" in {
        val env = Env()

        for {
          _ <- env.savePayloads(NonEmpty(Seq, payload1, payload2, payload3))
          // the first event is for alice, and the second for bob
          deliverEventAlice <- env.deliverEvent(ts1, alice, messageId1, payload1.id)
          deliverEventAll <- env.deliverEvent(
            ts2,
            alice,
            messageId2,
            payload2.id,
            recipients = Set(alice, bob),
          )
          deliverEventBob <- env.deliverEvent(ts3, bob, messageId3, payload3.id)
          _ <- env.store.saveEvents(
            instanceIndex,
            NonEmpty(Seq, deliverEventAlice, deliverEventAll, deliverEventBob),
          )
          _ <- env.saveWatermark(deliverEventBob.timestamp).valueOrFail("saveWatermark")
          aliceEvents <- env.readEvents(alice)
          bobEvents <- env.readEvents(bob)
          _ = aliceEvents should have size (2)
          _ = bobEvents should have size (2)
          _ <- env.assertDeliverEvent(aliceEvents(0), ts1, alice, messageId1, Set(alice), payload1)
          _ <- env.assertDeliverEvent(
            aliceEvents(1),
            ts2,
            alice,
            messageId2,
            Set(alice, bob),
            payload2,
          )
          _ <- env.assertDeliverEvent(
            bobEvents(0),
            ts2,
            alice,
            messageId2,
            Set(alice, bob),
            payload2,
          )
          _ <- env.assertDeliverEvent(bobEvents(1), ts3, bob, messageId3, Set(bob), payload3)
        } yield succeed
      }

      "errors are delivered correctly" in {
        val env = Env()

        for {
          aliceId <- env.store.registerMember(alice, ts1)
          _bobId <- env.store.registerMember(bob, ts1)
          error: DeliverErrorStoreEvent = DeliverErrorStoreEvent
            .create(
              aliceId,
              messageId1,
              String256M.tryCreate("Something went wrong".repeat(22000)),
              None,
              traceContext,
            )
            .value
          timestampedError: Sequenced[Nothing] = Sequenced(ts1, error)
          _ <- env.store.saveEvents(instanceIndex, NonEmpty(Seq, timestampedError))
          _ <- env.saveWatermark(timestampedError.timestamp).valueOrFail("saveWatermark")
          aliceEvents <- env.readEvents(alice)
          bobEvents <- env.readEvents(bob)
        } yield {
          aliceEvents.headOption.value shouldBe timestampedError
          bobEvents should have size 0
        }
      }

      "support paging results" in {
        val env = Env()

        for {
          aliceId <- env.store.registerMember(alice, ts1)
          // lets write 20 deliver events - offsetting the second timestamp that is at epoch second 1
          events = NonEmptyUtil.fromUnsafe(
            (0L until 20L)
              .map(n => {
                env.deliverEventWithDefaults(ts1.plusSeconds(n), sender = aliceId)()
              })
              .toSeq
          )
          payloads = DomainSequencingTestUtils.payloadsForEvents(events)
          _ <- env.store
            .savePayloads(NonEmptyUtil.fromUnsafe(payloads), instanceDiscriminator1)
            .valueOrFail(s"Save payloads")
          _ <- env.store.saveEvents(instanceIndex, events)
          _ <- env.saveWatermark(events.last1.timestamp).valueOrFail("saveWatermark")
          // read from the beginning (None)
          firstPage <- env.readEvents(alice, None, 10)
          // read from the ts of the last event of the prior page (read should be non-inclusive)
          secondPage <- env.readEvents(alice, firstPage.lastOption.value.timestamp.some, 10)
          // ask for 10 results from a position where we know there are less
          partialPage <- env.readEvents(alice, ts1.plusSeconds(14).some, 10)
        } yield {
          def seconds(page: Seq[Sequenced[_]]) = page.map(_.timestamp.getEpochSecond).toList

          seconds(firstPage) shouldBe (1L to 10L).toList
          seconds(secondPage) shouldBe (11L to 20L).toList
          seconds(partialPage) shouldBe (16L to 20L).toList
        }
      }

      "only return events that are below the minimum watermark" in {
        val env = Env()

        for {
          aliceId <- env.store.registerMember(alice, ts1)
          // lets write 20 events - offsetting the second timestamp that is at epoch second 1
          events = (0L until 20L).map(n => {
            env.deliverEventWithDefaults(ts2.plusSeconds(n), sender = aliceId)()
          })
          payloads = DomainSequencingTestUtils.payloadsForEvents(events)
          _ <- env.store
            .savePayloads(NonEmptyUtil.fromUnsafe(payloads), instanceDiscriminator1)
            .valueOrFail(s"Save payloads")
          _ <- env.store.saveEvents(instanceIndex, NonEmptyUtil.fromUnsafe(events))
          // put a watermark only a bit into our events
          _ <- env.saveWatermark(ts2.plusSeconds(5)).valueOrFail("saveWatermark")
          firstPage <- env.readEvents(alice, None, 10)
        } yield {
          // should only contain events up until and including the watermark timestamp
          firstPage should have size 6
        }
      }

      "fetch watermark" in {
        val env = Env()
        val ts = CantonTimestamp.now()

        for {
          initialWatermarkO <- env.store.fetchWatermark(0)
          _ = initialWatermarkO shouldBe None
          _ <- env.store.saveWatermark(0, ts).valueOrFail("saveWatermark")
          updatedWatermarkO <- env.store.fetchWatermark(0)
        } yield updatedWatermarkO.value.timestamp shouldBe ts
      }
    }

    "save payloads" should {
      "return an error if there is a conflicting id" in {
        val env = Env()
        val Seq(p1, p2, p3) =
          0.until(3).map(n => Payload(PayloadId(ts(n)), ByteString.copyFromUtf8(n.toString)))

        // we'll first write p1 and p2 that should work
        // then write p2 and p3 with a separate instance discriminator which should fail due to a conflicting id
        for {
          _ <- valueOrFail(env.store.savePayloads(NonEmpty(Seq, p1, p2), instanceDiscriminator1))(
            "savePayloads1"
          )
          error <- leftOrFail(
            env.store.savePayloads(NonEmpty(Seq, p2, p3), instanceDiscriminator2)
          )("savePayloads2")
        } yield error shouldBe SavePayloadsError.ConflictingPayloadId(p2.id, instanceDiscriminator1)
      }
    }

    "counter checkpoints" should {
      "return none if none are available" in {
        val env = Env()

        for {
          aliceId <- env.store.registerMember(alice, ts1)
          checkpointO <- env.store.fetchClosestCheckpointBefore(aliceId, SequencerCounter(0))
        } yield checkpointO shouldBe None
      }

      "return the counter at the point queried" in {
        val env = Env()

        val checkpoint1 = checkpoint(SequencerCounter(0), ts2)
        val checkpoint2 = checkpoint(SequencerCounter(1), ts3, Some(ts1))
        for {
          aliceId <- env.store.registerMember(alice, ts1)
          _ <- valueOrFail(env.store.saveCounterCheckpoint(aliceId, checkpoint1))(
            "save first checkpoint"
          )
          _ <- valueOrFail(env.store.saveCounterCheckpoint(aliceId, checkpoint2))(
            "save second checkpoint"
          )
          firstCheckpoint <- env.store.fetchClosestCheckpointBefore(
            aliceId,
            SequencerCounter(0L + 1),
          )
          secondCheckpoint <- env.store.fetchClosestCheckpointBefore(
            aliceId,
            SequencerCounter(1L + 1),
          )
        } yield {
          firstCheckpoint.value shouldBe checkpoint1
          secondCheckpoint.value shouldBe checkpoint2
        }
      }

      "return the nearest value under the value queried" in {
        val env = Env()

        val futureTs = ts1.plusSeconds(50)
        val checkpoint1 = checkpoint(SequencerCounter(10), ts2, Some(ts1))
        val checkpoint2 = checkpoint(SequencerCounter(42), futureTs, Some(ts2))

        for {
          aliceId <- env.store.registerMember(alice, ts1)
          _ <- valueOrFail(env.store.saveCounterCheckpoint(aliceId, checkpoint1))(
            "save first checkpoint"
          )
          _ <- valueOrFail(env.store.saveCounterCheckpoint(aliceId, checkpoint2))(
            "save second checkpoint"
          )
          checkpointForCounterAfterFirst <- env.store.fetchClosestCheckpointBefore(
            aliceId,
            SequencerCounter(20),
          )
          checkpointForCounterAfterSecond <- env.store.fetchClosestCheckpointBefore(
            aliceId,
            SequencerCounter(50),
          )
        } yield {
          checkpointForCounterAfterFirst.value shouldBe checkpoint1
          checkpointForCounterAfterSecond.value shouldBe checkpoint2
        }
      }

      "ignore saving existing checkpoint if timestamps are the same" in {
        val env = Env()

        val checkpoint1 = checkpoint(SequencerCounter(10), ts1)
        val checkpoint2 = checkpoint(SequencerCounter(20), ts2, Some(ts1))
        for {
          aliceId <- env.store.registerMember(alice, ts1)
          _ <- valueOrFail(env.store.saveCounterCheckpoint(aliceId, checkpoint1))(
            "save first checkpoint"
          )
          withoutTopologyTimestamp <- env.store.saveCounterCheckpoint(aliceId, checkpoint1).value

          _ <- valueOrFail(env.store.saveCounterCheckpoint(aliceId, checkpoint2))(
            "save second checkpoint"
          )
          withTopologyTimestamp <- env.store.saveCounterCheckpoint(aliceId, checkpoint2).value
        } yield {
          withoutTopologyTimestamp shouldBe Right(())
          withTopologyTimestamp shouldBe Right(())
        }
      }

      "should return error if there is an existing checkpoint with different timestamp" in {
        val env = Env()

        val checkpoint1 = checkpoint(SequencerCounter(10), ts1)
        for {
          aliceId <- env.store.registerMember(alice, ts1)
          _ <- valueOrFail(env.store.saveCounterCheckpoint(aliceId, checkpoint1))(
            "save first checkpoint"
          )
          wrongTimestamp <- env.store
            .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(10), ts2))
            .value // note different timestamp value
          wrongTimestampAndTopologyTimestamp <- env.store
            .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(10), ts2, Some(ts2)))
            .value // note different timestamp value
          allowedDuplicateInsert <- env.store
            .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(10), ts1, Some(ts2)))
            .value // note different topology client timestamp value
          wrongTimestamp2 <- env.store
            .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(10), ts2))
            .value // note different topology client timestamp value
        } yield {
          wrongTimestamp shouldBe Left(
            SaveCounterCheckpointError.CounterCheckpointInconsistent(ts1, None)
          )
          wrongTimestampAndTopologyTimestamp shouldBe Left(
            SaveCounterCheckpointError.CounterCheckpointInconsistent(ts1, None)
          )
          // if we previously didn't have a latest topology timestamp, we allow a new 'insert'
          allowedDuplicateInsert shouldBe Right(())
          // but we won't actually update the stored latest topology timestamp, as this only happens immediately
          // after a migration without any activity on the domain
          wrongTimestamp2 shouldBe Left(
            SaveCounterCheckpointError.CounterCheckpointInconsistent(ts1, None)
          )
        }
      }
    }

    "acknowledgements" should {

      def acknowledgements(
          status: SequencerPruningStatus
      ): Map[Member, Option[CantonTimestamp]] =
        status.members.map { case SequencerMemberStatus(member, _, lastAcknowledged, _) =>
          member -> lastAcknowledged
        }.toMap

      "latestAcknowledgements should return acknowledgements" in {
        val env = Env()

        for {
          aliceId <- env.store.registerMember(alice, ts1)
          _ <- env.store.registerMember(bob, ts2)
          _ <- env.store.acknowledge(aliceId, ts3)
          latestAcknowledgements <- env.store.latestAcknowledgements()
        } yield {
          latestAcknowledgements shouldBe Map(
            aliceId -> ts3
          )
        }
      }

      "acknowledge should ignore earlier timestamps" in {
        val env = Env()

        for {
          aliceId <- env.store.registerMember(alice, ts1)
          _ <- env.store.acknowledge(aliceId, ts3)
          _ <- env.store.acknowledge(aliceId, ts2)
          acknowledgements <- env.store.status(ts(10)).map(acknowledgements)
        } yield acknowledgements shouldBe Map(
          alice -> ts3.some
        )
      }
    }

    "acknowledge should keep track of different clients ack'ing the same member" in {
      val env = Env()

      for {
        aliceId <- env.store.registerMember(alice, ts1)
        _ <- env.store.acknowledge(aliceId, ts2)
        _ <- env.store.acknowledge(aliceId, ts3)
        acknowledgements <- env.store.latestAcknowledgements()
      } yield acknowledgements shouldBe Map(
        aliceId -> ts2,
        aliceId -> ts3,
      )
    }

    "lower bound" should {
      "initially be empty" in {
        val env = Env()

        for {
          boundO <- env.store.fetchLowerBound()
        } yield boundO shouldBe empty
      }

      "return value once saved" in {
        val env = Env()
        val bound = CantonTimestamp.now()

        for {
          _ <- env.store.saveLowerBound(bound).valueOrFail("saveLowerBound")
          fetchedBoundO <- env.store.fetchLowerBound()
        } yield fetchedBoundO.value shouldBe bound
      }

      "error if set bound is lower than previous bound" in {
        val env = Env()
        val bound1 = CantonTimestamp.Epoch.plusSeconds(10)
        val bound2 = bound1.plusMillis(-1) // before prior bound

        for {
          _ <- env.store.saveLowerBound(bound1).valueOrFail("saveLowerBound1")
          error <- leftOrFail(env.store.saveLowerBound(bound2))("saveLowerBound2")
        } yield {
          error shouldBe BoundLowerThanExisting(bound1, bound2)
        }
      }
    }

    "pruning" should {
      "if data has been acknowledged and watermarked remove some now unnecessary data" in {
        val env = Env()
        import env.*

        for {
          aliceId <- store.registerMember(alice, ts1)
          _ <- store.saveEvents(0, NonEmpty(Seq, deliverEventWithDefaults(ts2)()))
          bobId <- store.registerMember(bob, ts3)
          _ <- env.savePayloads(NonEmpty(Seq, payload1))
          // store a deliver event at ts4, ts5, and ts6
          // (hopefully resulting in the earlier two deliver events being pruned)
          _ <- store.saveEvents(
            instanceIndex,
            NonEmpty(
              Seq,
              Sequenced(
                ts(4),
                DeliverStoreEvent(
                  aliceId,
                  messageId1,
                  NonEmpty(SortedSet, aliceId, bobId),
                  payload1.id,
                  None,
                  traceContext,
                ),
              ),
              deliverEventWithDefaults(ts(5))(recipients = NonEmpty(SortedSet, aliceId, bobId)),
              deliverEventWithDefaults(ts(6))(recipients = NonEmpty(SortedSet, aliceId, bobId)),
            ),
          )
          // save an earlier counter checkpoint that should be removed
          _ <- store
            .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(1), ts(2)))
            .valueOrFail("alice counter checkpoint")
          _ <- store
            .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(2), ts(5)))
            .valueOrFail("alice counter checkpoint")
          _ <- store
            .saveCounterCheckpoint(bobId, checkpoint(SequencerCounter(1), ts(5)))
            .valueOrFail("bob counter checkpoint")
          _ <- store
            .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(3), ts(6)))
            .valueOrFail("alice counter checkpoint")
          _ <- store
            .saveCounterCheckpoint(bobId, checkpoint(SequencerCounter(2), ts(6)))
            .valueOrFail("bob counter checkpoint")
          _ <- store.acknowledge(aliceId, ts(6))
          _ <- store.acknowledge(bobId, ts(6))
          statusBefore <- store.status(ts(10))
          recordCountsBefore <- store.countRecords
          pruningTimestamp = statusBefore.safePruningTimestamp
          _tsAndReport <- {
            logger.debug(s"Pruning sequencer store from $pruningTimestamp")
            store
              .prune(pruningTimestamp, statusBefore, NonNegativeFiniteDuration.tryOfSeconds(1))
              .valueOrFail("prune")
          }
          statusAfter <- store.status(ts(10))
          recordCountsAfter <- store.countRecords
          lowerBound <- store.fetchLowerBound()
        } yield {
          val removedCounts = recordCountsBefore - recordCountsAfter
          removedCounts.counterCheckpoints shouldBe 1 // for alice's first checkpoint
          removedCounts.events shouldBe 2 // the earlier deliver events
          removedCounts.payloads shouldBe 1 // for the deliver event
          statusBefore.lowerBound shouldBe <(statusAfter.lowerBound)
          lowerBound.value shouldBe ts(5) // to prevent reads from before this point
        }
      }

      "not prune more than requested" in {
        val env = Env()
        import env.*

        for {
          isStoreInitiallyEmpty <- store
            .locatePruningTimestamp(NonNegativeInt.tryCreate(0))
            .map(_.isEmpty)
          aliceId <- store.registerMember(alice, ts1)
          _ <- store.saveEvents(0, NonEmpty(Seq, deliverEventWithDefaults(ts2)()))
          bobId <- store.registerMember(bob, ts3)
          _ <- env.savePayloads(NonEmpty(Seq, payload1))
          // store a deliver event at ts4, ts5, ts6, and ts7
          // resulting in only the first deliver event being pruned honoring the pruning timestamp of earlier than ts5
          _ <- store.saveEvents(
            instanceIndex,
            NonEmpty(
              Seq,
              Sequenced(
                ts(4),
                DeliverStoreEvent(
                  aliceId,
                  messageId1,
                  NonEmpty(SortedSet, aliceId, bobId),
                  payload1.id,
                  None,
                  traceContext,
                ),
              ),
              deliverEventWithDefaults(ts(5))(recipients = NonEmpty(SortedSet, aliceId, bobId)),
              deliverEventWithDefaults(ts(6))(recipients = NonEmpty(SortedSet, aliceId, bobId)),
              deliverEventWithDefaults(ts(7))(recipients = NonEmpty(SortedSet, aliceId, bobId)),
            ),
          )
          // save an earlier counter checkpoint that should be removed
          _ <- store
            .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(1), ts(4)))
            .valueOrFail("alice counter checkpoint")
          _ <- store
            .saveCounterCheckpoint(bobId, checkpoint(SequencerCounter(1), ts(4)))
            .valueOrFail("bob counter checkpoint")
          _ <- store
            .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(2), ts(6)))
            .valueOrFail("alice counter checkpoint")
          _ <- store
            .saveCounterCheckpoint(bobId, checkpoint(SequencerCounter(2), ts(6)))
            .valueOrFail("bob counter checkpoint")
          _ <- store.acknowledge(aliceId, ts(7))
          _ <- store.acknowledge(bobId, ts(7))
          statusBefore <- store.status(ts(10))
          recordCountsBefore <- store.countRecords
          pruningTimestamp = ts(5)
          _tsAndReport <- {
            logger.debug(s"Pruning sequencer store from $pruningTimestamp")
            store
              .prune(pruningTimestamp, statusBefore, NonNegativeFiniteDuration.tryOfSeconds(1))
              .valueOrFail("prune")
          }
          recordCountsAfter <- store.countRecords
          oldestTimestamp <- store.locatePruningTimestamp(NonNegativeInt.tryCreate(0))
        } yield {
          isStoreInitiallyEmpty shouldBe true
          // as pruning is "exclusive", should see the requested pruning time of ts5, and not
          // ts6, the timestamp just before safePruningTimestamp (ts7)
          oldestTimestamp shouldBe Some(ts(5))
          statusBefore.safePruningTimestamp shouldBe ts(7)
          val removedCounts = recordCountsBefore - recordCountsAfter
          removedCounts.counterCheckpoints shouldBe 2 // Alice's and Bob's checkpoints from ts4
          removedCounts.events shouldBe 2 // the two deliver event earlier than ts5 from ts2 and ts4
          removedCounts.payloads shouldBe 1 // for payload1 from ts1
        }
      }

      "when adjusting the safe pruning timestamp" should {
        "set it back to ensure we don't delete events that we'll still need to read to re-establish counters" in {
          val env = Env()
          import env.*

          for {
            aliceId <- store.registerMember(alice, ts(1))
            _ <- store
              .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(3), ts(3)))
              .valueOrFail("saveCounterCheckpoint")
            _ <- store.acknowledge(aliceId, ts(5))
            status <- store.status(CantonTimestamp.Epoch)
            safeTimestamp = status.safePruningTimestamp
            _ = safeTimestamp shouldBe ts(5) // as alice has acknowledged the event
            adjustedTimestampO <- store.adjustPruningTimestampForCounterCheckpoints(
              safeTimestamp,
              Seq.empty,
            )
          } yield {
            adjustedTimestampO.value shouldBe ts(
              3
            ) // but we need to retain earlier events to not blow up the counter checkpoints
          }
        }

        "correctly consider clients when there are many" in {
          val env = Env()
          import env.*

          for {
            aliceId <- store.registerMember(alice, ts(1))
            _ <- store
              .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(3), ts(3)))
              .valueOrFail("saveCounterCheckpoint1")
            _ <- store
              .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(5), ts(5)))
              .valueOrFail("saveCounterCheckpoint2")
            // clients have acknowledgements at different points
            _ <- store.acknowledge(aliceId, ts(4))
            status <- store.status(CantonTimestamp.Epoch)
            safeTimestamp = status.safePruningTimestamp
            _ = safeTimestamp shouldBe ts(4) // due to the earlier client ack
            adjustedTimestampO <- store.adjustPruningTimestampForCounterCheckpoints(
              safeTimestamp,
              Seq.empty,
            )
          } yield adjustedTimestampO.value shouldBe ts(
            3
          ) // as we need to keep the counter checkpoint immediately below the client we still need to serve
        }

        "not worry about ignored members" in {
          val env = Env()
          import env.*

          for {
            aliceId <- store.registerMember(alice, ts(1))
            bobId <- store.registerMember(bob, ts(2))
            _ <- store
              .saveCounterCheckpoint(aliceId, checkpoint(SequencerCounter(3), ts(3)))
              .valueOrFail("saveCounterCheckpoint1")
            _ <- store
              .saveCounterCheckpoint(bobId, checkpoint(SequencerCounter(5), ts(5)))
              .valueOrFail("saveCounterCheckpoint2")
            // clients have acknowledgements at different points
            _ <- store.acknowledge(aliceId, ts(4))
            _ <- store.acknowledge(bobId, ts(6))
            _ <- store.disableMember(aliceId)
            status <- store.status(CantonTimestamp.Epoch)
            safeTimestamp = status.safePruningTimestamp
            _ = safeTimestamp shouldBe ts(6) // as alice is ignored
            adjustedTimestampO <- store.adjustPruningTimestampForCounterCheckpoints(
              safeTimestamp,
              Seq(aliceId),
            )
          } yield {
            adjustedTimestampO.value shouldBe ts(
              5
            ) // the later counter checkpoint is sufficient to keep the non-ignored client working
          }
        }

        "throw if we try to prune all data" in {
          val env = Env()
          import env.*

          for {
            aliceId <- store.registerMember(alice, ts(1))
            bobId <- store.registerMember(bob, ts(2))
            _ <- store.disableMember(aliceId)
            _ <- store.disableMember(bobId)
            status <- store.status(ts(3))
            exception <- loggerFactory.assertLogs(
              store
                .prune(
                  status.safePruningTimestamp,
                  status,
                  NonNegativeFiniteDuration.tryOfSeconds(1),
                )
                .valueOrFail("prune")
                .failed,
              _.errorMessage should include(
                "Preventing pruning as it would remove all data from the Sequencer"
              ),
            )
          } yield {
            exception shouldBe an[RuntimeException]
            exception.getMessage should include("Preventing pruning")
          }
        }
      }
    }

    "disabling clients" in {
      val env = Env()
      import env.*

      for {
        List(aliceId, bobId, caroleId) <- List(alice, bob, carole).parTraverse(
          store.registerMember(_, ts(0))
        )
        disabledClientsBefore <- store.status(ts(0)).map(_.disabledClients)
        _ = {
          disabledClientsBefore.members shouldBe empty
        }
        _ <- store.disableMember(aliceId)
        _ <- store.disableMember(bobId)
        disabledClientsAfter <- store.status(ts(0)).map(_.disabledClients)
        _ = disabledClientsAfter.members should contain.only(alice, bob)
        // alice instances should be entirely disabled
        aliceEnabled <- leftOrFail(store.isEnabled(aliceId))("alice1Enabled")
        _ = aliceEnabled shouldBe MemberDisabledError
        // should also be idempotent
        _ <- store.disableMember(aliceId)
        _ <- store.disableMember(bobId)
        _ <- store.disableMember(caroleId)
      } yield succeed
    }

    "unregister unauthenticated members" in {
      val env = Env()
      import env.*

      val unauthenticatedAlice: UnauthenticatedMemberId =
        UnauthenticatedMemberId(UniqueIdentifier.tryCreate("alice_unauthenticated", "fingerprint"))

      for {
        id <- store.registerMember(unauthenticatedAlice, ts1)
        aliceLookup1 <- store.lookupMember(unauthenticatedAlice)
        _ = aliceLookup1 shouldBe Some(RegisteredMember(id, ts1))
        _ <- store.unregisterUnauthenticatedMember(unauthenticatedAlice)
        aliceLookup2 <- store.lookupMember(unauthenticatedAlice)
        _ = aliceLookup2 shouldBe empty
        // should also be idempotent
        _ <- store.unregisterUnauthenticatedMember(unauthenticatedAlice)
      } yield succeed
    }

    "validating commit mode" should {
      "be successful during tests" in {
        val store = mk()

        for {
          _ <- valueOrFail(store.validateCommitMode(CommitMode.Synchronous))("validate commit mode")
        } yield succeed
      }
    }
  }

}
