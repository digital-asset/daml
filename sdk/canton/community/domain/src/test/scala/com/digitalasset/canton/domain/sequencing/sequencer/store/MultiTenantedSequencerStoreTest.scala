// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import cats.syntax.option.*
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.DomainSequencingTestUtils
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.topology.{Member, ParticipantId}
import org.scalatest.compatible.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID
import scala.concurrent.Future

trait MultiTenantedSequencerStoreTest extends FlagCloseable with HasCloseContext {
  this: AsyncWordSpec with BaseTest =>

  def multiTenantedSequencerStore(mk: () => SequencerStore): Unit = {
    val alice: Member = ParticipantId("alice")
    val bob: Member = ParticipantId("bob")
    val instanceDiscriminator = UUID.randomUUID()
    def mkInstanceStore(instanceIndex: Int, store: SequencerStore) =
      new SimpleSequencerWriterStore(instanceIndex, store)

    def ts(epochSeconds: Int): CantonTimestamp =
      CantonTimestamp.Epoch.plusSeconds(epochSeconds.toLong)

    def deliver(ts: CantonTimestamp, sender: SequencerMemberId): Sequenced[PayloadId] = {
      Sequenced(
        ts,
        DomainSequencingTestUtils.mockDeliverStoreEvent(
          sender = sender,
          payloadId = PayloadId(ts),
          traceContext = traceContext,
        )(),
      )
    }

    def writeDelivers(store: SequencerWriterStore, sender: SequencerMemberId)(
        epochSeconds: Int*
    ) = {
      val delivers: List[Sequenced[PayloadId]] =
        epochSeconds.map(ts).map(ts => deliver(ts, sender)).toList
      val payloads = DomainSequencingTestUtils.payloadsForEvents(delivers)
      for {
        _unit <- store
          .savePayloads(NonEmptyUtil.fromUnsafe(payloads), instanceDiscriminator)
          .valueOrFail(s"Save payloads")
        _unit <- store.saveEvents(NonEmptyUtil.fromUnsafe(delivers))
      } yield { () }

    }

    def assertTimestamps(events: ReadEvents)(epochSeconds: Int*): Assertion = {
      events.payloads.length shouldBe epochSeconds.length
      events.payloads.map(_.timestamp) should contain theSameElementsInOrderAs epochSeconds.map(ts)
    }

    "checking sequencer liveness" should {
      "knock stale sequencers offline" in {
        val store = mk()
        val sequencer0 = mkInstanceStore(0, store)
        val sequencer1 = mkInstanceStore(1, store)
        val sequencer2 = mkInstanceStore(2, store)
        for {
          _ <- sequencer0.saveWatermark(ts(5)).valueOrFail("saveWatermark0")
          _ <- sequencer1.saveWatermark(ts(10)).valueOrFail("saveWatermark1")
          _ <- sequencer2.saveWatermark(ts(15)).valueOrFail("saveWatermark2")
          _ <- store.markLaggingSequencersOffline(ts(10))
          s0Update <- sequencer0.saveWatermark(ts(25)).value
          s1Update <- sequencer1.saveWatermark(ts(25)).value
          s2Update <- sequencer2.saveWatermark(ts(25)).value
        } yield {
          // s0 & s1 should both be offline so the watermark update should have failed
          // s2 should be online so shouldn't have returned an error
          s0Update.left.value shouldBe SaveWatermarkError.WatermarkFlaggedOffline
          s1Update.left.value shouldBe SaveWatermarkError.WatermarkFlaggedOffline
          s2Update shouldBe Right(())
        }
      }
    }

    "reading events" should {
      "return safe watermark when no events are found for member" in {
        val store = mk()
        val sequencer1 = mkInstanceStore(1, store)
        val sequencer2 = mkInstanceStore(2, store)

        for {
          aliceId <- store.registerMember(alice, ts(0))
          bobId <- store.registerMember(bob, ts(0))
          _ <- writeDelivers(sequencer1, bobId)(1)
          _ <- writeDelivers(sequencer2, aliceId)(2, 3, 4, 5)
          _ <- sequencer1.saveWatermark(ts(4)).valueOrFail("saveWatermark1")
          _ <- sequencer2.saveWatermark(ts(6)).valueOrFail("saveWatermark2")
          aliceEvents <- store.readEvents(
            aliceId,
            fromTimestampO = ts(2).some,
            10,
          )
          bobEvents <- store.readEvents(
            bobId,
            fromTimestampO = ts(2).some,
            10,
          )
        } yield {
          bobEvents shouldBe SafeWatermark(Some(ts(4)))
          assertTimestamps(aliceEvents)(3, 4)
        }
      }

      "not include events from offline sequencers after they're knocked offline" in {
        val store = mk()
        val sequencer1 = mkInstanceStore(1, store)
        val sequencer2 = mkInstanceStore(2, store)
        val sequencer3 = mkInstanceStore(3, store)

        for {
          aliceId <- store.registerMember(alice, ts(0))
          _ <- writeDelivers(sequencer1, aliceId)(1, 4)
          _ <- writeDelivers(sequencer2, aliceId)(2, 5)
          _ <- writeDelivers(sequencer3, aliceId)(3, 6)
          _ <- sequencer1.saveWatermark(ts(3)).valueOrFail("saveWatermark1")
          _ <- sequencer2.saveWatermark(ts(6)).valueOrFail("saveWatermark2")
          _ <- sequencer3.saveWatermark(ts(6)).valueOrFail("saveWatermark3")
          eventsBefore <- store.readEvents(
            aliceId,
            fromTimestampO = ts(0).some,
            10,
          )
          // so despite a bunch of later events because s1 is last has a watermark at ts(3) we won't see any later events
          _ = assertTimestamps(eventsBefore)(1, 2, 3)
          // mark s1 as offline
          _ <- store.markLaggingSequencersOffline(ts(3))
          eventsAfter <- store
            .readEvents(aliceId, fromTimestampO = ts(0).some, 10)
        } yield {
          // should now read all events up until the min online sequencer which is ts(6)
          // however it should now include events from sequencers after they went offline (ts(4))
          assertTimestamps(eventsAfter)(1, 2, 3, 5, 6)
        }
      }

      /* Given the sequencer is using its own clock for setting watermark and lastActiveAt, I doubt there's any realistic
         way that a sequencer that's knocked offline could have written events and a lastActiveAt ahead of min(watermark) from
         all online sequencers. But this test verifies that we won't screw up the event order even if that happens.
       */
      "if offline sequencer is ahead of min(watermark) shouldn't mess up event order" in {
        val store = mk()
        val sequencer1 = mkInstanceStore(1, store)
        val sequencer2 = mkInstanceStore(2, store)
        val sequencer3 = mkInstanceStore(3, store)

        for {
          aliceId <- store.registerMember(alice, ts(0))
          // deliver event 7 is ahead of all other events, but won't be included in s1's watermark
          _ <- writeDelivers(sequencer1, aliceId)(1, 7, 8)
          _ <- writeDelivers(sequencer2, aliceId)(2, 5, 9)
          _ <- writeDelivers(sequencer3, aliceId)(3, 6)
          // sequencer1 is going to set its watermark to ts(7) but then go offline leaving the min-watermark at ts(4)
          _ <- sequencer1.saveWatermark(ts(7)).valueOrFail("saveWatermark1")
          _ <- sequencer2.saveWatermark(ts(4)).valueOrFail("saveWatermark2")
          _ <- sequencer3.saveWatermark(ts(4)).valueOrFail("saveWatermark3")
          // mark s1 as offline
          _ <- sequencer1.goOffline()
          events1 <- store.readEvents(aliceId, fromTimestampO = ts(0).some, 10)
          _ = assertTimestamps(events1)(1, 2, 3)
          // when s2&s3 progress past the online watermark of s1 we should see event at ts(7) that s1 wrote
          // but not ts(8) as that was not included by their watermark and will later removed upon recovery
          _ <- sequencer2.saveWatermark(ts(9)).valueOrFail("saveWatermark4")
          _ <- sequencer3.saveWatermark(ts(9)).valueOrFail("saveWatermark5")
          events2 <- store.readEvents(aliceId, fromTimestampO = ts(0).some, 10)
        } yield {
          assertTimestamps(events2)(1, 2, 3, 5, 6, 7, 9)
        }
      }

      "if sequencers go offline we only read events up until their watermark" in {
        val store = mk()
        val sequencer1 = mkInstanceStore(1, store)
        val sequencer2 = mkInstanceStore(2, store)
        val sequencer3 = mkInstanceStore(3, store)

        for {
          aliceId <- store.registerMember(alice, ts(0))
          _ <- writeDelivers(sequencer1, aliceId)(1, 4)
          _ <- writeDelivers(sequencer2, aliceId)(2, 5)
          _ <- writeDelivers(sequencer3, aliceId)(3, 6)
          // save watermarks so all will make up to ts(3) visible
          _ <- sequencer1.saveWatermark(ts(3)).valueOrFail("saveWatermark1")
          _ <- sequencer2.saveWatermark(ts(3)).valueOrFail("saveWatermark2")
          _ <- sequencer3.saveWatermark(ts(3)).valueOrFail("saveWatermark3")
          // mark all sequencers as offline
          _ <- store.markLaggingSequencersOffline(ts(3))
          readEvents <- store.readEvents(aliceId, fromTimestampO = ts(0).some, 10)
        } yield {
          assertTimestamps(readEvents)(1, 2, 3)
        }
      }
    }

    "going offline" in {
      val store = mk()
      val sequencer = mkInstanceStore(1, store)

      for {
        _ <- sequencer.saveWatermark(ts(10)).valueOrFail("saveWatermark")
        _ <- sequencer.goOffline()
        watermarkO <- sequencer.fetchWatermark()
      } yield watermarkO.value shouldBe Watermark(ts(10), online = false)
    }

    "going online" should {
      "if node has never been online create a new watermark" in {
        val store = mk()
        val sequencer = mkInstanceStore(1, store)
        val now = ts(10)

        for {
          goOnlineTs <- sequencer.goOnline(now)
          watermarkO <- sequencer.fetchWatermark()
        } yield {
          goOnlineTs shouldBe now
          watermarkO.value shouldBe Watermark(now, online = true)
        }
      }

      "uses now if we're ahead of the current max watermark" in {
        val store = mk()
        val sequencer1 = mkInstanceStore(1, store)
        val sequencer2 = mkInstanceStore(2, store)
        val sequencer3 = mkInstanceStore(3, store)

        for {
          _ <- sequencer1.saveWatermark(ts(5)).valueOrFail("saveWatermark1")
          _ <- sequencer2.saveWatermark(ts(6)).valueOrFail("saveWatermark2")
          goOnlineTimestamp <- sequencer3.goOnline(ts(7))
        } yield goOnlineTimestamp shouldBe ts(7)
      }

      "uses the max watermark from existing sequencers if we're behind" in {
        val store = mk()
        val sequencer1 = mkInstanceStore(1, store)
        val sequencer2 = mkInstanceStore(2, store)
        val sequencer3 = mkInstanceStore(3, store)

        for {
          _ <- sequencer1.saveWatermark(ts(5)).valueOrFail("saveWatermark1")
          _ <- sequencer2.saveWatermark(ts(6)).valueOrFail("saveWatermark2")
          goOnlineTimestamp <- sequencer3.goOnline(ts(4))
        } yield goOnlineTimestamp shouldBe ts(6)
      }
    }

    "deleting events past watermark" should {
      // accessor for method that is intentionally hidden only for tests
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      def countEvents(store: SequencerStore, instanceIndex: Int): Future[Int] =
        store.asInstanceOf[DbSequencerStore].countEventsForNode(instanceIndex)

      "remove all events if the sequencer didn't write a watermark" in {
        val store = mk()
        val sequencer1 = mkInstanceStore(1, store)
        val sequencer2 = mkInstanceStore(2, store)

        for {
          _ <- writeDelivers(sequencer1, SequencerMemberId(0))(1, 3, 5)
          _ <- writeDelivers(sequencer2, SequencerMemberId(1))(2, 4, 6)
          _ <- sequencer1.saveWatermark(ts(3)).valueOrFail("watermark1")
          _ <- sequencer2.deleteEventsPastWatermark()
          s1Count <- countEvents(store, 1)
          s2Count <- countEvents(store, 2)
        } yield {
          s1Count shouldBe 3
          s2Count shouldBe 0
        }
      }

      "remove all events past our watermark if it exists" in {
        val store = mk()
        val sequencer1 = mkInstanceStore(1, store)
        val sequencer2 = mkInstanceStore(2, store)

        for {
          _ <- writeDelivers(sequencer1, SequencerMemberId(2))(1, 3, 5)
          _ <- writeDelivers(sequencer2, SequencerMemberId(3))(2, 4, 6)
          _ <- sequencer1.saveWatermark(ts(3)).valueOrFail("watermark1")
          _ <- sequencer2.saveWatermark(ts(4)).valueOrFail("watermark2")
          _ <- sequencer2.deleteEventsPastWatermark()
          s1Count <- countEvents(store, 1)
          s2Count <- countEvents(store, 2)
        } yield {
          s1Count shouldBe 3
          s2Count shouldBe 2 // have removed ts(6)
        }
      }
    }
  }
}
