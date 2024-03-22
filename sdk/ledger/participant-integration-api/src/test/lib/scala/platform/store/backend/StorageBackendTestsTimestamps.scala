// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection
import java.time.Instant
import java.util.TimeZone

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsTimestamps extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (timestamps)"

  import StorageBackendTestValues._

  it should "correctly read ledger effective time using rawEvents" in {
    val let = timestampFromInstant(Instant.now)
    val cid = hashCid("#1")
    val create = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = cid,
      ledgerEffectiveTime = Some(let),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams))

    executeSql(ingest(Vector(create), _))
    executeSql(updateLedgerEnd(offset(1), 1L))

    val events1 = executeSql(backend.event.rawEvents(0L, 1L))
    val events2 = executeSql(withDefaultTimeZone("GMT-1")(backend.event.rawEvents(0L, 1L)))
    val events3 = executeSql(withDefaultTimeZone("GMT+1")(backend.event.rawEvents(0L, 1L)))

    withClue("UTC") { events1.head.ledgerEffectiveTime shouldBe Some(let) }
    withClue("GMT-1") { events2.head.ledgerEffectiveTime shouldBe Some(let) }
    withClue("GMT+1") { events3.head.ledgerEffectiveTime shouldBe Some(let) }
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
