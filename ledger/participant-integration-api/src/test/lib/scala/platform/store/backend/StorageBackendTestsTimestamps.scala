// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection
import java.time.Instant
import java.util.TimeZone

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

private[backend] trait StorageBackendTestsTimestamps extends Matchers with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "StorageBackend (timestamps)"

  import StorageBackendTestValues._

  it should "correctly read ledger effective time using maximumLedgerTime" in {
    val let = Instant.now
    val cid = com.daml.lf.value.Value.ContractId.V0.assertFromString("#1")
    val create = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = cid.coid,
      ledgerEffectiveTime = Some(let),
    )
    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))

      _ <- executeSql(ingest(Vector(create), _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(1), 1L)))

      let1 <- executeSql(backend.maximumLedgerTime(Set(cid)))
      let2 <- executeSql(withDefaultTimeZone("GMT-1")(backend.maximumLedgerTime(Set(cid))))
      let3 <- executeSql(withDefaultTimeZone("GMT+1")(backend.maximumLedgerTime(Set(cid))))
    } yield {
      withClue("UTC") { let1 shouldBe Success(Some(let)) }
      withClue("GMT-1") { let2 shouldBe Success(Some(let)) }
      withClue("GMT+1") { let3 shouldBe Success(Some(let)) }
    }
  }

  it should "correctly read ledger effective time using rawEvents" in {
    val let = Instant.now
    val cid = com.daml.lf.value.Value.ContractId.V0.assertFromString("#1")
    val create = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = cid.coid,
      ledgerEffectiveTime = Some(let),
    )
    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))

      _ <- executeSql(ingest(Vector(create), _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(1), 1L)))

      events1 <- executeSql(backend.rawEvents(0L, 1L))
      events2 <- executeSql(withDefaultTimeZone("GMT-1")(backend.rawEvents(0L, 1L)))
      events3 <- executeSql(withDefaultTimeZone("GMT+1")(backend.rawEvents(0L, 1L)))
    } yield {
      withClue("UTC") { events1.head.ledgerEffectiveTime shouldBe Some(let) }
      withClue("GMT-1") { events2.head.ledgerEffectiveTime shouldBe Some(let) }
      withClue("GMT+1") { events3.head.ledgerEffectiveTime shouldBe Some(let) }
    }
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
