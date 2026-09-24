// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.participant.store.ParticipantPruningStore
import com.digitalasset.canton.participant.store.ParticipantPruningStore.ParticipantPruningStatus
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}

trait DbParticipantPruningStoreCachedTest extends DbParticipantPruningStoreTest { this: DbTest =>

  override def mk(): ParticipantPruningStore =
    mk(initialStatus = ParticipantPruningStore.ParticipantPruningStatus(None, None))

  def mk(initialStatus: ParticipantPruningStatus): ParticipantPruningStore =
    new DbParticipantPruningStoreCached(
      underlying = new DbParticipantPruningStore(name, storage, timeouts, loggerFactory),
      initialStatus = initialStatus,
    )

  "initialize cache correctly" in {
    val initialStatus = ParticipantPruningStatus(Some(100L), Some(50L))
    val store = mk(initialStatus = initialStatus)
    for {
      status <- store.pruningStatus()
    } yield status shouldBe initialStatus
  }

  "match the states of the underlying db store in a single pruning cycle" in {
    val initialStatus = ParticipantPruningStatus(None, None)
    val underlying = new DbParticipantPruningStore(name, storage, timeouts, loggerFactory)
    val store =
      new DbParticipantPruningStoreCached(
        underlying = underlying,
        initialStatus = initialStatus,
      )
    for {
      statusCache0 <- store.pruningStatus()
      statusDb0 <- underlying.pruningStatus()
      _ <- store.markPruningStarted(42L)
      statusCache1 <- store.pruningStatus()
      statusDb1 <- underlying.pruningStatus()
      _ <- store.markPruningDone(42L)
      statusCache2 <- store.pruningStatus()
      statusDb2 <- underlying.pruningStatus()
    } yield {
      statusCache0 shouldBe ParticipantPruningStatus(None, None)
      statusCache0 shouldBe statusDb0

      statusCache1 shouldBe ParticipantPruningStatus(Some(42L), None)
      statusCache1 shouldBe statusDb1

      statusCache2 shouldBe ParticipantPruningStatus(Some(42L), Some(42L))
      statusCache2 shouldBe statusDb2
    }
  }

  "match the states of the underlying db store in two concurrent pruning cycles" in {
    val initialStatus = ParticipantPruningStatus(None, None)
    val underlying = new DbParticipantPruningStore(name, storage, timeouts, loggerFactory)
    val store =
      new DbParticipantPruningStoreCached(
        underlying = underlying,
        initialStatus = initialStatus,
      )
    for {
      _ <- store.markPruningStarted(5L)
      _ <- store.markPruningStarted(10L)
      statusCache0 <- store.pruningStatus()
      statusDb0 <- underlying.pruningStatus()
      _ <- store.markPruningDone(10L)
      statusCache1 <- store.pruningStatus()
      statusDb1 <- underlying.pruningStatus()
      _ <- store.markPruningDone(5L)
      statusCache2 <- store.pruningStatus()
      statusDb2 <- underlying.pruningStatus()
    } yield {
      statusCache0 shouldBe ParticipantPruningStatus(Some(10L), None)
      statusCache0 shouldBe statusDb0
      statusCache1 shouldBe ParticipantPruningStatus(Some(10L), Some(10L))
      statusCache1 shouldBe statusDb1
      statusCache2 shouldBe ParticipantPruningStatus(Some(10L), Some(10L))
      statusCache2 shouldBe statusDb2
    }
  }

}

class ParticipantPruningStoreCachedTestH2 extends DbParticipantPruningStoreCachedTest with H2Test
class ParticipantPruningStoreCachedTestPostgres
    extends DbParticipantPruningStoreCachedTest
    with PostgresTest
