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
import com.daml.platform.store.dao.DbDispatcherTests.Result
import io.opentelemetry.api.GlobalOpenTelemetry
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
  private def dispatcher = dbSupport.dbDispatcher

  "rollback transaction if Left is received in the execute" in {
    for {
      value1 <- dispatcher.executeSqlEither(dbMetric) { connection =>
        Left(Result(PartyRecordStorageBackendImpl.createPartyRecord(payload)(connection) > 0))
      }
      loadedParty1 <- dispatcher.executeSqlEither(dbMetric) { connection =>
        Right(PartyRecordStorageBackendImpl.getPartyRecord(party)(connection))
      }
      value2 <- dispatcher.executeSqlEither(dbMetric) { connection =>
        Right(Result(PartyRecordStorageBackendImpl.createPartyRecord(payload)(connection) > 0))
      }
      loadedParty2 <- dispatcher.executeSqlEither(dbMetric) { connection =>
        Right(PartyRecordStorageBackendImpl.getPartyRecord(party)(connection))
      }
    } yield {
      value1 shouldBe Left(Result(true)) // simulating an error in Left channel
      loadedParty1.value shouldBe None // party record has not been stored, i.e. transaction has been rolled back
      value2 shouldBe Right(Result(true)) // simulating success in Right channel
      loadedParty2.value.value.payload shouldBe payload // loading previously stored object
    }
  }
}

object DbDispatcherTests {
  case class Result(valid: Boolean)
}
