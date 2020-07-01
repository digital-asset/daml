// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlPackageUploadEntry
}
import org.scalatest.{Matchers, WordSpec}
import KeyValueConsumption.logEntryToUpdate
import com.daml.lf.data.Time.Timestamp

class KeyValueConsumptionSpec extends WordSpec with Matchers {
  private val aLogEntryId = DamlLogEntryId.getDefaultInstance
  private val aLogEntryWithoutRecordTime = DamlLogEntry.newBuilder
    .setPackageUploadEntry(DamlPackageUploadEntry.getDefaultInstance)
    .build
  private val aRecordTimeFromUpdate = Timestamp(123456789)
  private val aLogEntryWithRecordTime = DamlLogEntry.newBuilder
    .setRecordTime(com.google.protobuf.Timestamp.newBuilder.setSeconds(100))
    .setPackageUploadEntry(DamlPackageUploadEntry.getDefaultInstance)
    .build

  "logEntryToUpdate" should {
    "throw in case no record time is available from the log entry or input argument" in {
      assertThrows[Err](
        logEntryToUpdate(aLogEntryId, aLogEntryWithoutRecordTime, recordTimeForUpdate = None))
    }

    "use record time provided as input instead of log entry's" in {
      val actual :: Nil = logEntryToUpdate(
        aLogEntryId,
        aLogEntryWithRecordTime,
        recordTimeForUpdate = Some(aRecordTimeFromUpdate))

      actual.recordTime shouldBe aRecordTimeFromUpdate
    }

    "use record time from log entry if not provided as input" in {
      val actual :: Nil =
        logEntryToUpdate(aLogEntryId, aLogEntryWithRecordTime, recordTimeForUpdate = None)

      actual.recordTime shouldBe Timestamp.assertFromInstant(Instant.ofEpochSecond(100))
    }
  }
}
