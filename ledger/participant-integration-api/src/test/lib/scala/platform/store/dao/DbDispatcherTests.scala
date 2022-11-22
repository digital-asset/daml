// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.codahale.metrics.MetricRegistry
import com.daml.lf.data.Ref
import com.daml.metrics.Metrics
import com.daml.platform.localstore.PersistentStoreSpecBase
import com.daml.platform.store.backend.StorageBackendProvider
import com.daml.platform.store.backend.localstore.PartyRecordStorageBackend.DbPartyRecordPayload
import com.daml.platform.store.backend.localstore.PartyRecordStorageBackendImpl
import com.daml.platform.store.dao.DbDispatcherTests.GeneratedId
import io.opentelemetry.api.GlobalOpenTelemetry
import org.scalatest.Inside.inside
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{EitherValues, OptionValues}

trait DbDispatcherTests
    extends PersistentStoreSpecBase
    with Matchers
    with OptionValues
    with EitherValues {
  self: AsyncFreeSpec with StorageBackendProvider =>

  private val metrics = new Metrics(new MetricRegistry, GlobalOpenTelemetry.getMeter("test"))
  private val dbMetric = metrics.daml.index.db.loadArchive

  private def party = Ref.Party.assertFromString("Alice")
  private def payload = DbPartyRecordPayload(party, 1, 1)

  "rollback transaction if Left is received in the execute" in {
    for {
      value1 <- dbSupport.dbDispatcher
        .executeSqlEither(dbMetric) { connection =>
          val generatedId = PartyRecordStorageBackendImpl.createPartyRecord(payload)(connection)
          Left(GeneratedId(generatedId))
        }
      loadedObject1 <- dbSupport.dbDispatcher
        .executeSqlEither(dbMetric) { connection =>
          val partyRecord = PartyRecordStorageBackendImpl.getPartyRecord(party)(connection)
          Right(partyRecord)
        }
      value2 <- dbSupport.dbDispatcher
        .executeSqlEither(dbMetric) { connection =>
          val generatedId = PartyRecordStorageBackendImpl.createPartyRecord(payload)(connection)
          Right(GeneratedId(generatedId))
        }
      loadedObject2 <- dbSupport.dbDispatcher
        .executeSqlEither(dbMetric) { connection =>
          val partyRecord = PartyRecordStorageBackendImpl.getPartyRecord(party)(connection)
          Right(partyRecord)
        }
    } yield {
      value1.isLeft shouldBe true
      loadedObject1 shouldBe Right(None) // actually an objection has not been stored
      value2.isRight shouldBe true
      inside(loadedObject2) { case Right(Some(record)) =>
        record.payload shouldBe payload // loading previously stored object
      }
    }
  }
}

object DbDispatcherTests {
  case class GeneratedId(createdId: Int)
}
