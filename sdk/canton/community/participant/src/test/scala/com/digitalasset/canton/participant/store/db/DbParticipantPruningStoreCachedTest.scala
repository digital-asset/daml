// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.ParticipantPruningStore
import com.digitalasset.canton.participant.store.ParticipantPruningStore.ParticipantPruningStatus
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext

trait DbParticipantPruningStoreCachedTest extends DbParticipantPruningStoreTest { this: DbTest =>

  override def mk(): ParticipantPruningStore =
    mk(initialStatus = ParticipantPruningStore.ParticipantPruningStatus(None, None))

  def mk(initialStatus: ParticipantPruningStatus): ParticipantPruningStore =
    new DbParticipantPruningStoreCached(
      underlying = new DbParticipantPruningStore(name, storage, timeouts, loggerFactory),
      initialStatus = initialStatus,
      loggerFactory = loggerFactory,
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
        loggerFactory = loggerFactory,
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
        loggerFactory = loggerFactory,
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

  "refreshes cache from DB if exception is thrown after DB update for markPruningStarted and markPruningDone" in {
    val initialStatus = ParticipantPruningStatus(None, None)

    // Custom underlying store that throws after DB update
    class FailingAfterDbUpdateStore
        extends DbParticipantPruningStore(name, storage, timeouts, loggerFactory) {

      override def markPruningStarted(upToInclusive: Offset)(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Unit] =
        // first update the DB
        super
          .markPruningStarted(upToInclusive)
          .flatMap { _ =>
            // then throw an exception
            throw new RuntimeException("Simulated failure after DB update")
          }(ec)

      override def markPruningDone(upToInclusive: Offset)(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Unit] =
        // first update the DB
        super
          .markPruningDone(upToInclusive)
          .flatMap { _ =>
            // then throw an exception
            throw new RuntimeException("Simulated failure after DB update")
          }(ec)
    }
    val underlying = new FailingAfterDbUpdateStore
    val store = new DbParticipantPruningStoreCached(underlying, initialStatus, loggerFactory)
    val upTo = 42L
    for {
      ex0 <- store.markPruningStarted(upTo).failed
      cacheStatus0 <- store.pruningStatus()
      dbStatus0 <- underlying.pruningStatus()
      ex1 <- store.markPruningDone(upTo).failed
      cacheStatus1 <- store.pruningStatus()
      dbStatus1 <- underlying.pruningStatus()
    } yield {
      ex0.getMessage should include("Simulated failure after DB update")
      cacheStatus0 shouldBe dbStatus0
      dbStatus0 shouldBe ParticipantPruningStatus(Some(upTo), None)

      ex1.getMessage should include("Simulated failure after DB update")
      cacheStatus1 shouldBe dbStatus1
      dbStatus1 shouldBe ParticipantPruningStatus(Some(upTo), Some(upTo))
    }
  }

  "invalidates cache if pruningStatus from DB fails" in {
    val initialStatus = ParticipantPruningStatus(None, None)

    val throwInPruningStatus = new java.util.concurrent.atomic.AtomicBoolean(false)

    // Custom underlying store that throws after DB update and fails on pruningStatus
    class FailingStore extends DbParticipantPruningStore(name, storage, timeouts, loggerFactory) {

      override def markPruningDone(upToInclusive: Offset)(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Unit] =
        // first update the DB
        super
          .markPruningDone(upToInclusive)
          .flatMap { _ =>
            // then throw an exception
            throw new RuntimeException("Simulated failure after DB update")
          }(ec)

      override def pruningStatus()(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[ParticipantPruningStatus] =
        if (throwInPruningStatus.get())
          FutureUnlessShutdown.failed(new RuntimeException("Simulated failure in pruningStatus"))
        else super.pruningStatus()

    }
    val underlying = new FailingStore
    val store = new DbParticipantPruningStoreCached(underlying, initialStatus, loggerFactory)
    val upTo = 42L
    for {
      _ <- store.markPruningStarted(upTo)
      dbStatus0 <- underlying.pruningStatus()
      _ = dbStatus0 shouldBe ParticipantPruningStatus(Some(upTo), None)

      _ <- store.markPruningDone(upTo).failed
      cacheStatus1 <- store.pruningStatus()
      _ = cacheStatus1 shouldBe ParticipantPruningStatus(Some(upTo), Some(upTo))

      // now simulate failure also in pruningStatus
      _ = throwInPruningStatus.set(true)
      _ <- underlying.pruningStatus().failed
      ex2 <- store.markPruningDone(upTo + 1).failed
      _ = ex2.getMessage should include("Simulated failure after DB update")
      _ = store.pruningStatusCached() shouldBe None
      _ = throwInPruningStatus.set(false)
      cacheStatus2 <- store.pruningStatus()
      dbStatus2 <- underlying.pruningStatus()
      _ = cacheStatus2 shouldBe dbStatus2
      _ = dbStatus2 shouldBe ParticipantPruningStatus(Some(upTo), Some(upTo + 1))

      _ <- store.markPruningDone(upTo + 2).failed
      _ = store.pruningStatusCached() shouldBe Some(
        ParticipantPruningStatus(Some(upTo), Some(upTo + 2))
      )
      cacheStatus3 <- store.pruningStatus()
      dbStatus3 <- underlying.pruningStatus()
    } yield {

      cacheStatus3 shouldBe dbStatus3
      dbStatus3 shouldBe ParticipantPruningStatus(Some(upTo), Some(upTo + 2))
    }
  }

}

class ParticipantPruningStoreCachedTestH2 extends DbParticipantPruningStoreCachedTest with H2Test
class ParticipantPruningStoreCachedTestPostgres
    extends DbParticipantPruningStoreCachedTest
    with PostgresTest
