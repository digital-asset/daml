// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.IdRange
import com.digitalasset.canton.platform.store.backend.common.EventPayloadSourceForUpdatesLedgerEffects
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Connection
import java.time.Instant
import java.util.TimeZone

private[backend] trait StorageBackendTestsTimestamps extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (timestamps)"

  import StorageBackendTestValues.*

  it should "correctly read ledger effective time using rawEvents" in {
    val let = timestampFromInstant(Instant.now)
    val consuming = dtosConsumingExercise(
      event_offset = 1L,
      event_sequential_id = 1L,
      ledger_effective_time = let.micros,
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))

    executeSql(ingest(consuming.toVector, _))
    executeSql(updateLedgerEnd(offset(1), 1L))

    val events = backend.event.fetchEventPayloadsLedgerEffects(
      EventPayloadSourceForUpdatesLedgerEffects.Deactivate
    )(
      eventSequentialIds = IdRange(1L, 10L),
      requestingPartiesForTx = Some(Set.empty),
      requestingPartiesForReassignment = Some(Set.empty),
    )
    val events1 = executeSql(events)
    val events2 = executeSql(withDefaultTimeZone("GMT-1")(events))
    val events3 = executeSql(withDefaultTimeZone("GMT+1")(events))

    withClue("UTC")(
      events1
        .collect { case ex: EventStorageBackend.RawExercisedEvent => ex }
        .head
        .ledgerEffectiveTime shouldBe let
    )
    withClue("GMT-1")(
      events2
        .collect { case ex: EventStorageBackend.RawExercisedEvent => ex }
        .head
        .ledgerEffectiveTime shouldBe let
    )
    withClue("GMT+1")(
      events3
        .collect { case ex: EventStorageBackend.RawExercisedEvent => ex }
        .head
        .ledgerEffectiveTime shouldBe let
    )
  }

  // Some JDBC operations depend on the JVM default time zone.
  // In particular, TIMESTAMP WITHOUT TIME ZONE columns are interpreted in the local time zone of the client.
  private def withDefaultTimeZone[T](tz: String)(f: Connection => T)(connection: Connection): T = {
    val previousDefaultTimeZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone(tz))
    try {
      f(connection)
    } finally {
      TimeZone.setDefault(previousDefaultTimeZone)
    }
  }
}
