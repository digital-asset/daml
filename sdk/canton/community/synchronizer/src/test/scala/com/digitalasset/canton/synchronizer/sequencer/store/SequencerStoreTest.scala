// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  MessageId,
  Recipients,
  SequencerErrors,
}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.db.DbTest
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.synchronizer.sequencer.SynchronizerSequencingTestUtils.deliverStoreEventWithPayloadWithDefaults
import com.digitalasset.canton.synchronizer.sequencer.store.SaveLowerBoundError.BoundLowerThanExisting
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown, ProtocolVersionChecksAsyncWordSpec}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.annotation.nowarn
import scala.collection.immutable.SortedSet

@nowarn("msg=match may not be exhaustive")
trait SequencerStoreTest
    extends AsyncWordSpec
    with BaseTest
    with HasCloseContext
    with FlagCloseable
    with ProtocolVersionChecksAsyncWordSpec
    with FailOnShutdown {

  lazy val sequencerMember: Member = DefaultTestIdentities.sequencerId

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
    val ts4 = ts(4)

    val batch = Batch(
      List(
        ClosedEnvelope.create(
          ByteString.copyFromUtf8("1"),
          Recipients.cc(alice, bob, carole),
          Seq.empty,
          testedProtocolVersion,
        )
      ),
      testedProtocolVersion,
    )
    val payloadBytes1 = batch.toByteString
    val payloadBytes2 = batch.toByteString
    val payload1 = BytesPayload(PayloadId(ts1), payloadBytes1)
    val payload2 = BytesPayload(PayloadId(ts2), payloadBytes2)
    val payload3 = BytesPayload(PayloadId(ts3), payloadBytes2)
    val messageId1 = MessageId.tryCreate("1")
    val messageId2 = MessageId.tryCreate("2")
    val messageId3 = MessageId.tryCreate("3")
    val messageId4 = MessageId.tryCreate("4")

    val instanceDiscriminator1 = UUID.randomUUID()
    val instanceDiscriminator2 = UUID.randomUUID()

    final case class Env(store: SequencerStore = mk()) {
      def deliverEventWithDefaults(
          ts: CantonTimestamp,
          sender: SequencerMemberId = SequencerMemberId(0),
      )(
          recipients: NonEmpty[SortedSet[SequencerMemberId]] = NonEmpty(SortedSet, sender)
      ): Sequenced[BytesPayload] =
        Sequenced(
          ts,
          deliverStoreEventWithPayloadWithDefaults(
            sender = sender,
            payload = BytesPayload(PayloadId(ts), Batch.empty(testedProtocolVersion).toByteString),
            traceContext = traceContext,
          )(
            recipients
          ),
        )

      def deliverEvent[P](
          ts: CantonTimestamp,
          sender: Member,
          messageId: MessageId,
          payload: P,
          recipients: Set[Member] = Set.empty,
          trafficReceiptO: Option[TrafficReceipt] = None,
      ): FutureUnlessShutdown[Sequenced[P]] =
        for {
          senderId <- store.registerMember(sender, ts)
          recipientIds <- recipients.toList.parTraverse(store.registerMember(_, ts)).map(_.toSet)
        } yield Sequenced(
          ts,
          DeliverStoreEvent(
            senderId,
            messageId,
            NonEmpty(SortedSet, senderId, recipientIds.toSeq*),
            payload,
            None,
            traceContext,
            trafficReceiptO,
          ),
        )

      def deliverReceipt(
          ts: CantonTimestamp,
          sender: Member,
          messageId: MessageId,
          topologyTimestamp: CantonTimestamp,
          trafficReceiptO: Option[TrafficReceipt] = None,
      ): FutureUnlessShutdown[Sequenced[BytesPayload]] =
        for {
          senderId <- store.registerMember(sender, ts)
        } yield Sequenced(
          ts,
          ReceiptStoreEvent(
            senderId,
            messageId,
            topologyTimestampO = Some(topologyTimestamp),
            traceContext,
            trafficReceiptO,
          ),
        )

      def lookupRegisteredMember(member: Member): FutureUnlessShutdown[SequencerMemberId] =
        for {
          registeredMemberO <- store.lookupMember(member)
          memberId = registeredMemberO.map(_.memberId).getOrElse(fail(s"$member is not registered"))
        } yield memberId

      def saveEventsAndBuffer(instanceIndex: Int, events: NonEmpty[Seq[Sequenced[BytesPayload]]])(
          implicit traceContext: TraceContext
      ): FutureUnlessShutdown[Unit] = {
        val savePayloadsF = NonEmpty.from(events.forgetNE.flatMap(_.event.payloadO.toList)) match {
          case Some(payloads) => savePayloads(payloads)
          case _ => FutureUnlessShutdown.unit
        }
        savePayloadsF.flatMap(_ =>
          store
            .saveEvents(instanceIndex, events.map(_.map(_.id)))
            .map(_ => store.bufferEvents(events))
        )
      }

      def readEvents(
          member: Member,
          fromTimestampO: Option[CantonTimestamp] = Some(CantonTimestamp.Epoch),
          limit: Int = 1000,
      ): FutureUnlessShutdown[Seq[Sequenced[BytesPayload]]] =
        for {
          memberId <- lookupRegisteredMember(member)
          events <- store.readEvents(memberId, member, fromTimestampO, limit)
          payloads <- store.readPayloads(events.events.flatMap(_.event.payloadO).toList, member)
        } yield events.events.map {
          _.map {
            case id: PayloadId => BytesPayload(id, payloads(id).toByteString)
            case payload: BytesPayload => payload
          }
        }

      def assertDeliverEvent(
          event: Sequenced[BytesPayload],
          expectedTimestamp: CantonTimestamp,
          expectedSender: Member,
          expectedMessageId: MessageId,
          expectedRecipients: Set[Member],
          expectedPayload: BytesPayload,
          expectedTopologyTimestamp: Option[CantonTimestamp] = None,
      ): FutureUnlessShutdown[Unit] =
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
                  topologyTimestampO,
                  traceContext,
                  _trafficReceiptO,
                ) =>
              sender shouldBe senderId
              messageId shouldBe expectedMessageId
              recipients.forgetNE should contain.only(recipientIds.toSeq*)
              payload shouldBe expectedPayload
              topologyTimestampO shouldBe expectedTopologyTimestamp
            case other =>
              fail(s"Expected deliver event but got $other")
          }
          ()
        }

      def assertReceiptEvent(
          event: Sequenced[BytesPayload],
          expectedTimestamp: CantonTimestamp,
          expectedSender: Member,
          expectedMessageId: MessageId,
          expectedTopologyTimestamp: Option[CantonTimestamp],
      ): FutureUnlessShutdown[Unit] =
        for {
          senderId <- lookupRegisteredMember(expectedSender)
        } yield {
          event.timestamp shouldBe expectedTimestamp
          event.event match {
            case ReceiptStoreEvent(
                  sender,
                  messageId,
                  topologyTimestampO,
                  _traceContext,
                  _trafficReceiptO,
                ) =>
              sender shouldBe senderId
              messageId shouldBe expectedMessageId
              event.event.members shouldBe Set(senderId)
              event.event.payloadO shouldBe None
              topologyTimestampO shouldBe expectedTopologyTimestamp
            case other =>
              fail(s"Expected deliver receipt but got $other")
          }
          ()
        }

      /** Save payloads using the default `instanceDiscriminator1` and expecting it to succeed */
      def savePayloads(payloads: NonEmpty[Seq[BytesPayload]]): FutureUnlessShutdown[Unit] =
        store.savePayloads(payloads, instanceDiscriminator1).valueOrFail("savePayloads")

      def saveWatermark(
          ts: CantonTimestamp
      ): EitherT[FutureUnlessShutdown, SaveWatermarkError, Unit] =
        store.saveWatermark(instanceIndex, ts)

      def resetWatermark(
          ts: CantonTimestamp
      ): EitherT[FutureUnlessShutdown, SaveWatermarkError, Unit] =
        store.resetWatermark(instanceIndex, ts)
    }

    "DeliverErrorStoreEvent" should {
      "be able to serialize to and deserialize the error from protobuf" in {
        val error = SequencerErrors.TopologyTimestampTooEarly("too early!")
        val errorStatus = error.rpcStatusWithoutLoggingContext()
        val serialized = DeliverErrorStoreEvent.serializeError(errorStatus, testedProtocolVersion)
        val deserialized =
          DeliverErrorStoreEvent.fromByteString(Some(serialized), testedProtocolVersion)
        deserialized shouldBe Right(errorStatus)
      }
    }

    "member registration" should {
      "be able to register a new member" in {
        val store = mk()
        for {
          id <- store.registerMember(alice, ts1)
          fetchedId <- store.lookupMember(alice)
        } yield fetchedId.value shouldBe RegisteredMember(id, ts1, enabled = true)
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
          registeredMember.value shouldBe RegisteredMember(id1, ts1, enabled = true)
        }
      }
    }

    "reading and writing" should {
      "deliver events should include associated payloads when read" in {
        val env = Env()

        for {
          deliverEvent1 <- env.deliverEvent(ts1, alice, messageId1, payload1)
          deliverEvent2 <- env.deliverEvent(ts2, alice, messageId2, payload2)
          _ <- env
            .saveEventsAndBuffer(instanceIndex, NonEmpty(Seq, deliverEvent1, deliverEvent2))
          _ <- env
            .saveWatermark(deliverEvent2.timestamp)
            .valueOrFail("saveWatermark")
          events <- env.readEvents(alice)
          _ = events should have size 2
          Seq(event1, event2) = events
          _ <- env
            .assertDeliverEvent(event1, ts1, alice, messageId1, Set(alice), payload1)
          _ <- env
            .assertDeliverEvent(event2, ts2, alice, messageId2, Set(alice), payload2)
        } yield succeed
      }

      "filter correctly by recipient" in {
        val env = Env()

        for {
          // the first event is for alice, and the second for bob
          deliverEvent1 <- env.deliverEvent(ts1, alice, messageId1, payload1)
          deliverEvent2 <- env.deliverEvent(ts2, bob, messageId2, payload2)
          _ <- env
            .saveEventsAndBuffer(instanceIndex, NonEmpty(Seq, deliverEvent1, deliverEvent2))
          _ <- env
            .saveWatermark(deliverEvent2.timestamp)
            .valueOrFail("saveWatermark")
          aliceEvents <- env.readEvents(alice)
          bobEvents <- env.readEvents(bob)
          _ = aliceEvents should have size 1
          _ = bobEvents should have size 1
          _ <- env
            .assertDeliverEvent(
              aliceEvents.headOption.value,
              ts1,
              alice,
              messageId1,
              Set(alice),
              payload1,
            )
          _ <- env
            .assertDeliverEvent(
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
          // the first event is for alice, and the second for bob
          deliverEventAlice <- env.deliverEvent(ts1, alice, messageId1, payload1)
          deliverEventAll <- env
            .deliverEvent(
              ts2,
              alice,
              messageId2,
              payload2,
              recipients = Set(alice, bob),
            )
          receiptAlice <- env.deliverReceipt(ts4, alice, messageId4, ts3)
          deliverEventBob <- env.deliverEvent(ts3, bob, messageId3, payload3)
          _ <- env
            .saveEventsAndBuffer(
              instanceIndex,
              NonEmpty(Seq, deliverEventAlice, deliverEventAll, deliverEventBob, receiptAlice),
            )
          _ <- env.saveWatermark(receiptAlice.timestamp).valueOrFail("saveWatermark")
          aliceEvents <- env.readEvents(alice)
          bobEvents <- env.readEvents(bob)
          _ = aliceEvents should have size (3)
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
          _ <- env.assertReceiptEvent(
            aliceEvents(2),
            ts4,
            alice,
            messageId4,
            ts3.some,
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
          error = DeliverErrorStoreEvent(
            aliceId,
            messageId1,
            None,
            traceContext,
            None,
          )
          timestampedError: Sequenced[Nothing] = Sequenced(ts1, error)
          _ <- env.saveEventsAndBuffer(instanceIndex, NonEmpty(Seq, timestampedError))
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
            (0L until 20L).map { n =>
              env.deliverEventWithDefaults(ts1.plusSeconds(n), sender = aliceId)()
            }.toSeq
          )
          _ <- env.saveEventsAndBuffer(instanceIndex, events)
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
          events = (0L until 20L).map { n =>
            env.deliverEventWithDefaults(ts2.plusSeconds(n), sender = aliceId)()
          }
          _ <- env.saveEventsAndBuffer(instanceIndex, NonEmptyUtil.fromUnsafe(events))
          // put a watermark only a bit into our events
          _ <- env.saveWatermark(ts2.plusSeconds(5)).valueOrFail("saveWatermark")
          firstPage <- env.readEvents(alice, None, 10)
          state <- env.store.readStateAtTimestamp(ts2.plusSeconds(5))
        } yield {
          val numberOfEvents = 6L
          // should only contain events up until and including the watermark timestamp
          firstPage should have size numberOfEvents
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

      "read from fan-out buffer if enabled" in {
        val env = Env()
        for {
          deliverEvent1 <- env.deliverEvent(ts1, alice, messageId1, payload1).failOnShutdown
          deliverEvent2 <- env.deliverEvent(ts2, alice, messageId2, payload2).failOnShutdown
          deliverEvent3 <- env.deliverEvent(ts3, alice, messageId3, payload3).failOnShutdown
          _ <- env
            .saveEventsAndBuffer(
              instanceIndex,
              NonEmpty(Seq, deliverEvent1, deliverEvent2, deliverEvent3),
            )
            .failOnShutdown
          _ <- env.saveWatermark(ts3).valueOrFail("saveWatermark").failOnShutdown
          events <- {
            loggerFactory.assertLogsSeq(SuppressionRule.FullSuppression)(
              // Note that this timestamp ts1 is exclusive so we WILL miss the first event
              env.readEvents(alice, ts(1).some),
              logs => {
                val readFromTheBuffer =
                  logs.exists(_.message.contains("Serving 2 events from the buffer"))
                val bufferDisabled = !env.store.eventsBufferEnabled
                (readFromTheBuffer || bufferDisabled) shouldBe true
              },
            )
          }.failOnShutdown
          _ = events should have size 2
          Seq(event2, event3) = events
          _ <- env.assertDeliverEvent(event2, ts2, alice, messageId2, Set(alice), payload2)
          _ <- env.assertDeliverEvent(event3, ts3, alice, messageId3, Set(alice), payload3)
        } yield succeed
      }
    }

    "save payloads" should {
      // TODO(#16087) enable test for database sequencer with blockSequencerMode=false
      "return an error if there is a conflicting id for database sequencer" ignore {
        val env = Env()

        val Seq(p1, p2, p3) =
          0.until(3).map(n => BytesPayload(PayloadId(ts(n)), ByteString.copyFromUtf8(n.toString)))

        // we'll first write p1 and p2 that should work
        // then write p2 and p3 with a separate instance discriminator which should fail due to a conflicting id
        for {
          _ <- env.store
            .savePayloads(NonEmpty(Seq, p1, p2), instanceDiscriminator1)
            .valueOrFail("savePayloads1")
          error <- env.store
            .savePayloads(NonEmpty(Seq, p2, p3), instanceDiscriminator2)
            .leftOrFail("savePayloads2")
        } yield error shouldBe SavePayloadsError.ConflictingPayloadId(p2.id, instanceDiscriminator1)
      }

      // TODO(#16087) when bringing back database sequencer, only run this test if blockSequencerMode=true
      "succeed on a conflicting payload id for unified sequencer" in {
        val env = Env()

        val Seq(p1, p2, p3) =
          0.until(3).map(n => BytesPayload(PayloadId(ts(n)), ByteString.copyFromUtf8(n.toString)))

        // we'll first write p1 and p2 that should work
        // then write p2 and p3 with a separate instance discriminator which should fail due to a conflicting id
        for {
          _ <- env.store
            .savePayloads(NonEmpty(Seq, p1, p2), instanceDiscriminator1)
            .valueOrFail("savePayloads1")
          _ <- env.store
            .savePayloads(NonEmpty(Seq, p2, p3), instanceDiscriminator2)
            .valueOrFail("savePayloads2")
        } yield succeed
      }
    }

    "previous timestamps" should {
      "return none if no events are available" in {
        val env = Env()
        for {
          aliceId <- env.store.registerMember(alice, ts1)
          previousTimestampO <- env.store.fetchPreviousEventTimestamp(aliceId, ts1)
        } yield {
          // registration sets the pruned_previous_event_timestamp to None
          previousTimestampO shouldBe None
        }
      }

      "return pruned/onboarding timestamp for the member if no events are available" in {
        val env = Env()
        for {
          aliceId <- env.store.registerMember(alice, ts1)
          _ <- env.store.updatePrunedPreviousEventTimestamps(Map(alice -> Some(ts2)))
          previousTimestampO <- env.store.fetchPreviousEventTimestamp(aliceId, ts3)
        } yield {
          // we expect the value from the members table that we updated above
          previousTimestampO shouldBe Some(ts2)
        }
      }

      "return the correct previous events timestamp for the member" in {
        val env = Env()
        for {
          aliceId <- env.store.registerMember(alice, ts1)
          _ <- env.store.updatePrunedPreviousEventTimestamps(Map(alice -> Some(ts2)))
          event1 <- env.deliverEvent(ts4, alice, messageId1, payload1)
          event2 <- env.deliverEvent(ts(6), alice, messageId2, payload2)
          _ <- env.saveEventsAndBuffer(instanceIndex, NonEmpty(Seq, event1, event2))
          _ <- env.saveWatermark(ts(6)).valueOrFail("saveWatermark")
          previousTimestamp1 <- env.store.fetchPreviousEventTimestamp(aliceId, ts3)
          previousTimestamp2 <- env.store.fetchPreviousEventTimestamp(aliceId, ts4)
          previousTimestamp3 <- env.store.fetchPreviousEventTimestamp(aliceId, ts(5))
          previousTimestamp4 <- env.store.fetchPreviousEventTimestamp(aliceId, ts(6))
        } yield {
          previousTimestamp1 shouldBe Some(ts2) // from members table
          previousTimestamp2 shouldBe Some(ts4) // the first event
          previousTimestamp3 shouldBe Some(ts4) // still the first event
          previousTimestamp4 shouldBe Some(ts(6)) // and finally the second event
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
        val boundTopology = CantonTimestamp.now().minusMillis(1L).some

        for {
          _ <- env.store.saveLowerBound(bound, boundTopology).valueOrFail("saveLowerBound")
          fetchedBoundO <- env.store.fetchLowerBound()
        } yield fetchedBoundO.value shouldBe (bound, boundTopology)
      }

      "error if set bound is lower than previous bound" in {
        val env = Env()
        val bound1 = CantonTimestamp.Epoch.plusSeconds(10)
        val bound1Topology = CantonTimestamp.Epoch.plusSeconds(9).some
        val bound2 = bound1.plusMillis(-1) // before prior bound
        val bound2Topology = bound1Topology.map(_.plusMillis(-1))

        for {
          _ <- env.store.saveLowerBound(bound1, bound1Topology).valueOrFail("saveLowerBound1")
          error <- env.store.saveLowerBound(bound2, bound2Topology).leftOrFail("saveLowerBound2")
        } yield {
          error shouldBe BoundLowerThanExisting((bound1, bound1Topology), (bound2, bound2Topology))
        }
      }
    }

    "pruning" should {
      "if data has been acknowledged and watermarked remove some now unnecessary data" in {
        val env = Env()
        import env.*

        for {
          sequencerId <- store.registerMember(sequencerMember, ts1)
          aliceId <- store.registerMember(alice, ts1)
          _ <- env.saveEventsAndBuffer(
            instanceIndex,
            NonEmpty(
              Seq,
              deliverEventWithDefaults(ts2)(recipients = NonEmpty(SortedSet, aliceId, sequencerId)),
            ),
          )
          bobId <- store.registerMember(bob, ts3)
          // store a deliver event at ts4, ts5, and ts6
          // (hopefully resulting in the earlier two deliver events being pruned)
          _ <- env.saveEventsAndBuffer(
            instanceIndex,
            NonEmpty(
              Seq,
              Sequenced(
                ts(4),
                DeliverStoreEvent(
                  aliceId,
                  messageId1,
                  NonEmpty(SortedSet, aliceId, bobId),
                  payload1,
                  None,
                  traceContext,
                  None,
                ),
              ),
              deliverEventWithDefaults(ts(5))(recipients = NonEmpty(SortedSet, aliceId, bobId)),
              deliverEventWithDefaults(ts(6))(recipients = NonEmpty(SortedSet, aliceId, bobId)),
            ),
          )
          _ <- env.saveWatermark(ts(6)).valueOrFail("saveWatermark")
          _ <- store.acknowledge(aliceId, ts(6))
          _ <- store.acknowledge(bobId, ts(6))
          _ <- store.acknowledge(sequencerId, ts(6))
          statusBefore <- store.status(ts(10))
          stateBeforePruning <- store.readStateAtTimestamp(ts(10))
          recordCountsBefore <- store.countRecords
          pruningTimestamp = statusBefore.safePruningTimestamp
          _tsAndReport <- {
            logger.debug(s"Pruning sequencer store up to $pruningTimestamp")
            store
              .prune(pruningTimestamp, statusBefore, NonNegativeFiniteDuration.tryOfSeconds(1))
              .valueOrFail("prune")
          }
          statusAfter <- store.status(ts(10))
          stateAfterPruning <- store.readStateAtTimestamp(ts(10))
          recordCountsAfter <- store.countRecords
          lowerBound <- store.fetchLowerBound()
        } yield {
          val removedCounts = recordCountsBefore - recordCountsAfter
          removedCounts.events shouldBe 3 // the earlier deliver events
          removedCounts.payloads shouldBe 2 // for payload1 from ts1 + payload from deliverEventWithDefaults(ts2)
          statusBefore.lowerBound shouldBe <(statusAfter.lowerBound)
          val expectedPreviousTimestamps = Map(
            alice -> ts(6).some,
            bob -> ts(6).some,
            sequencerMember -> ts(2).some,
          )
          stateBeforePruning.previousTimestamps shouldBe expectedPreviousTimestamps
          // below the event at ts(2) is gone, so ts(2) should come from
          // the sequencer_members.pruned_previous_event_timestamp
          stateAfterPruning.previousTimestamps shouldBe expectedPreviousTimestamps
          // pruning should update:
          // - lower bound to the last acknowledged timestamp: ts(6),
          // - latest topology client timestamp at lower bound should be set
          //   to latest event addressed to sequencer: ts(2)
          lowerBound.value shouldBe ((ts(6), ts(2).some))
        }
      }

      "not prune more than requested" in {
        val env = Env()
        import env.*

        for {
          isStoreInitiallyEmpty <- store
            .locatePruningTimestamp(NonNegativeInt.tryCreate(0))
            .map(_.isEmpty)
          sequencerId <- store.registerMember(sequencerMember, ts1)
          aliceId <- store.registerMember(alice, ts1)
          _ <- env.saveEventsAndBuffer(0, NonEmpty(Seq, deliverEventWithDefaults(ts2)()))
          bobId <- store.registerMember(bob, ts3)
          // store a deliver event at ts4, ts5, ts6, and ts7
          // resulting in only the first deliver event being pruned honoring the pruning timestamp of earlier than ts5
          _ <- env.saveEventsAndBuffer(
            instanceIndex,
            NonEmpty(
              Seq,
              Sequenced(
                ts(4),
                DeliverStoreEvent(
                  aliceId,
                  messageId1,
                  NonEmpty(SortedSet, aliceId, bobId),
                  payload1,
                  None,
                  traceContext,
                  None,
                ),
              ),
              deliverEventWithDefaults(ts(5))(recipients = NonEmpty(SortedSet, aliceId, bobId)),
              deliverEventWithDefaults(ts(6))(recipients = NonEmpty(SortedSet, aliceId, bobId)),
              deliverEventWithDefaults(ts(7))(recipients = NonEmpty(SortedSet, aliceId, bobId)),
            ),
          )
          _ <- env.saveWatermark(ts(7)).valueOrFail("saveWatermark")
          _ <- store.acknowledge(aliceId, ts(7))
          _ <- store.acknowledge(bobId, ts(7))
          _ <- store.acknowledge(sequencerId, ts(7))
          statusBefore <- store.status(ts(10))
          recordCountsBefore <- store.countRecords
          pruningTimestamp = ts(5)
          _tsAndReport <- {
            logger.debug(s"Pruning sequencer store up to $pruningTimestamp")
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
        }
      }

      "when adjusting the safe pruning timestamp" should {
        "correctly consider clients when there are many" in {
          val env = Env()
          import env.*

          for {
            aliceId <- store.registerMember(alice, ts(1))
            // clients have acknowledgements at different points
            _ <- store.acknowledge(aliceId, ts(4))
            status <- store.status(ts(5))
            safeTimestamp = status.safePruningTimestamp
          } yield {
            safeTimestamp shouldBe ts(4) // due to the earlier client ack
          }
        }

        "not worry about ignored members" in {
          val env = Env()
          import env.*

          for {
            aliceId <- store.registerMember(alice, ts(1))
            bobId <- store.registerMember(bob, ts(2))
            // clients have acknowledgements at different points
            _ <- store.acknowledge(aliceId, ts(4))
            _ <- store.acknowledge(bobId, ts(6))
            _ <- store.disableMember(alice)
            status <- store.status(ts(6))
            safeTimestamp = status.safePruningTimestamp
          } yield {
            safeTimestamp shouldBe ts(6) // as alice is ignored
          }
        }
      }
    }

    "disabling clients" in {
      val env = Env()
      import env.*

      for {
        _ <- List(alice, bob, carole).parTraverse(
          store.registerMember(_, ts(0))
        )
        disabledClientsBefore <- store.status(ts(0)).map(_.disabledClients)
        _ = {
          disabledClientsBefore.members shouldBe empty
        }
        _ <- store.lookupMember(alice) // this line populates the cache
        _ <- store.disableMember(alice)
        _ <- store.disableMember(bob)
        disabledClientsAfter <- store.status(ts(0)).map(_.disabledClients)
        _ = disabledClientsAfter.members should contain.only(alice, bob)
        // alice instances should be entirely disabled
        aliceRegisteredMember <- clue("lookupMember alice") {
          store.lookupMember(alice).map(_.getOrElse(fail("lookupMember alice")))
        }
        _ = clue("alice was not disabled, maybe due to caching?") {
          aliceRegisteredMember.enabled shouldBe false
        }
        // should also be idempotent
        _ <- store.disableMember(alice)
        _ <- store.disableMember(bob)
        _ <- store.disableMember(carole)
      } yield succeed
    }

    "validating commit mode" should {
      "be successful during tests" in {
        val store = mk()

        for {
          _ <- store.validateCommitMode(CommitMode.Synchronous).valueOrFail("validate commit mode")
        } yield succeed
      }
    }

    "snapshotting" should {
      "be able to initialize a separate store with a snapshot from the first one" in {
        def createSnapshots() = {
          val env = Env()
          import env.*
          for {
            _ <- store.registerMember(carole, ts3)
            aliceId <- store.registerMember(alice, ts1)
            sequencerId <- store.registerMember(sequencerMember, ts1)

            _ <- env.saveEventsAndBuffer(
              instanceIndex,
              NonEmpty(
                Seq,
                deliverEventWithDefaults(ts2)(recipients = NonEmpty(SortedSet, aliceId)),
              ),
            )
            bobId <- store.registerMember(bob, ts3)
            _ <- env.saveEventsAndBuffer(
              instanceIndex,
              NonEmpty(
                Seq,
                Sequenced(
                  ts(4),
                  DeliverStoreEvent(
                    aliceId,
                    messageId1,
                    NonEmpty(SortedSet, aliceId, bobId, sequencerId),
                    payload1,
                    None,
                    traceContext,
                    None,
                  ),
                ),
              ),
            )
            _ <- saveWatermark(ts(4)).valueOrFail("saveWatermark")
            snapshot <- store.readStateAtTimestamp(ts(4))

            value1 = NonEmpty(
              Seq,
              deliverEventWithDefaults(ts(5))(recipients = NonEmpty(SortedSet, aliceId, bobId)),
              deliverEventWithDefaults(ts(6))(recipients = NonEmpty(SortedSet, aliceId, bobId)),
            )
            _ <- env.saveEventsAndBuffer(
              instanceIndex,
              value1,
            )
            _ <- saveWatermark(ts(6)).valueOrFail("saveWatermark")

          } yield snapshot
        }

        def createFromSnapshot(snapshot: SequencerSnapshot) = {
          val env = Env()
          import env.*
          val initialState = SequencerInitialState(
            synchronizerId = DefaultTestIdentities.synchronizerId, // not used
            snapshot = snapshot,
            latestSequencerEventTimestamp = None,
            initialTopologyEffectiveTimestamp = None,
          )
          for {
            _ <- store.initializeFromSnapshot(initialState).value.map {
              case Left(error) =>
                fail(s"Failed to initialize from snapshot $error")
              case _ => ()
            }

            stateFromNewStore <- store.readStateAtTimestamp(ts(4))

            newAliceId <- store.lookupMember(alice).map(_.getOrElse(fail()).memberId)
            newSequencerId <- store.lookupMember(sequencerMember).map(_.getOrElse(fail()).memberId)
            newBobId <- store.lookupMember(bob).map(_.getOrElse(fail()).memberId)

            _ <- env.saveEventsAndBuffer(
              instanceIndex,
              NonEmpty(
                Seq,
                deliverEventWithDefaults(ts(5))(recipients =
                  NonEmpty(SortedSet, newAliceId, newSequencerId)
                ),
                deliverEventWithDefaults(ts(6))(recipients =
                  NonEmpty(SortedSet, newAliceId, newBobId)
                ),
              ),
            )
            _ <- saveWatermark(ts(6)).valueOrFail("saveWatermark")

            snapshotFromNewStoreAfterNewEvents <- store.readStateAtTimestamp(ts(6))
          } yield (
            stateFromNewStore,
            snapshotFromNewStoreAfterNewEvents,
          )
        }

        for {
          snapshot <- createSnapshots()

          // resetting the db tables
          _ = this match {
            case dbTest: DbTest => dbTest.beforeEach()
            case _ => ()
          }

          newSnapshots <- createFromSnapshot(snapshot)
          (
            snapshotFromNewStore,
            snapshotFromNewStoreAfterNewEvents,
          ) = newSnapshots
        } yield {

          val expectedMemberPreviousTimestamps = Map(
            alice -> Some(ts(4)),
            bob -> Some(ts(4)),
            carole -> None,
            sequencerMember -> Some(ts(4)),
          )
          snapshot.previousTimestamps shouldBe expectedMemberPreviousTimestamps

          val expectedMemberPreviousTimestampsAfter = Map(
            alice -> Some(ts(6)),
            bob -> Some(ts(6)),
            carole -> None,
            sequencerMember -> Some(ts(5)),
          )
          snapshotFromNewStoreAfterNewEvents.previousTimestamps shouldBe expectedMemberPreviousTimestampsAfter
        }
      }
    }

    "resetting watermark" should {
      "reset watermark if the ts if before the current watermark" in {
        val env = Env()
        import env.*
        for {
          _ <- resetWatermark(ts(3)).valueOrFail("resetWatermark=3 failed")
          watermark0 <- store.fetchWatermark(0)
          _ <- saveWatermark(ts(5)).valueOrFail("saveWatermark=5 failed")
          _ <- resetWatermark(ts(7)).valueOrFail("resetWatermark=7 failed")
          watermark1 <- store.fetchWatermark(0)
          _ <- resetWatermark(ts(4)).valueOrFail("resetWatermark=4 failed")
          watermark2 <- store.fetchWatermark(0)
        } yield {
          watermark0 shouldBe None // unchanged
          watermark1 shouldBe Some(
            Watermark(ts(5), online = true)
          ) // ts(5) was not touched and still online
          watermark2 shouldBe Some(Watermark(ts(4), online = false)) // ts(5) was reset and offline
        }
      }
    }

    "deleteEventsPastWatermark" should {
      "return the watermark used for the deletion" in {
        val testWatermark = CantonTimestamp.assertFromLong(1719841168208718L)
        val env = Env()
        import env.*

        for {
          _ <- saveWatermark(testWatermark).valueOrFail("saveWatermark")
          watermark <- store.deleteEventsPastWatermark(0)
        } yield {
          watermark shouldBe Some(testWatermark)
        }
      }
    }
  }
}
