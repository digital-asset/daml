// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.option.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.LedgerSyncRecordTime
import com.digitalasset.canton.participant.store.EventLogId.ParticipantEventLogId
import com.digitalasset.canton.participant.store.db.DbEventLogTestResources
import com.digitalasset.canton.participant.sync.TimestampedEvent.TimelyRejectionEventId
import com.digitalasset.canton.topology.DomainId
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID

trait ParticipantEventLogTest extends AsyncWordSpec with BaseTest {

  lazy val id: ParticipantEventLogId =
    DbEventLogTestResources.dbParticipantEventLogTestParticipantEventLogId

  lazy val domainNamePrefix = "participant-event-log-test-domain"
  lazy val domainId1 = DomainId.tryFromString(s"$domainNamePrefix::1")
  lazy val domainId2 = DomainId.tryFromString(s"$domainNamePrefix::2")

  protected def newStore: ParticipantEventLog

  "ParticipantEventLog" can {
    // We do not run the tests from SingleDimensionEventLog
    // because those would interfere with the tests of the MultiDomainEventLog

    "return monotonically increasing local offsets" in {
      val store = newStore

      for {
        off1 <- store.nextLocalOffset()
        off2 <- store.nextLocalOffset()
        off3 <- store.nextLocalOffset()
      } yield {
        off1 should be < off2
        off2 should be < off3
      }
    }

    "return many new offsets" in {
      val store = newStore

      val size = 10000

      for {
        many <- store.nextLocalOffsets(NonNegativeInt.tryCreate(size))
        anotherOne <- store.nextLocalOffset()
      } yield {
        many.size shouldBe size
        many.sorted shouldBe many // new offsets are ascending
        many.distinct shouldBe many // new offsets are distinct
        forEvery(many) { off =>
          off should be < anotherOne
        }
      }
    }

    "return 0 new offsets" in {
      val store = newStore

      for {
        none <- store.nextLocalOffsets(NonNegativeInt.tryCreate(0))
      } yield none shouldBe Seq.empty
    }

    "first event with associated domain" in {
      val store = newStore

      for {
        offsets <- store.nextLocalOffsets(NonNegativeInt.tryCreate(4))
        event1 = SingleDimensionEventLogTest
          .generateEvent(
            LedgerSyncRecordTime.Epoch.addMicros(2000),
            offsets(0),
          ) // Reversed timestamp/offset order
          .copy(eventId = TimelyRejectionEventId(domainId1, UUID.randomUUID()).some)
        event2 = SingleDimensionEventLogTest
          .generateEvent(LedgerSyncRecordTime.Epoch.addMicros(1000), offsets(1))
          .copy(eventId = TimelyRejectionEventId(domainId1, UUID.randomUUID()).some)
        event3 = SingleDimensionEventLogTest
          .generateEvent(LedgerSyncRecordTime.Epoch.addMicros(500), offsets(2))
          .copy(eventId = TimelyRejectionEventId(domainId2, UUID.randomUUID()).some)
        event4 = SingleDimensionEventLogTest
          .generateEvent(LedgerSyncRecordTime.Epoch.addMicros(3000), offsets(3))
          .copy(eventId = TimelyRejectionEventId(domainId1, UUID.randomUUID()).some)
        inserts <- store.insertsUnlessEventIdClash(Seq(event1, event2, event3, event4))
        first1 <- store.firstEventWithAssociatedDomainAtOrAfter(domainId1, CantonTimestamp.Epoch)
        first2 <- store.firstEventWithAssociatedDomainAtOrAfter(domainId2, CantonTimestamp.Epoch)
        first1a <- store.firstEventWithAssociatedDomainAtOrAfter(
          domainId1,
          CantonTimestamp.ofEpochMilli(2),
        )
        first1b <- store.firstEventWithAssociatedDomainAtOrAfter(
          domainId1,
          CantonTimestamp.ofEpochMilli(3),
        )
        first1c <- store.firstEventWithAssociatedDomainAtOrAfter(
          domainId1,
          CantonTimestamp.MaxValue,
        )
        first2a <- store.firstEventWithAssociatedDomainAtOrAfter(
          domainId2,
          CantonTimestamp.ofEpochMilli(1),
        )
      } yield {
        inserts shouldBe Seq(Right(()), Right(()), Right(()), Right(()))
        first1 shouldBe Some(event1) // Comparison is based on timestamp, ordering based on offset
        first2 shouldBe Some(event3) // Different domain
        first1a shouldBe Some(event1) // Exact timestamp
        first1b shouldBe Some(event4) // Exact timestamp
        first1c shouldBe None
        first2a shouldBe None // No later event on the given domain, but on other domains
      }
    }
  }
}
