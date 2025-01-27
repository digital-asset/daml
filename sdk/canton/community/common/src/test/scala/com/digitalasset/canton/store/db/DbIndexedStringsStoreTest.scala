// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.{IndexedStringStore, IndexedStringType}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait DbIndexedStringsStoreTest
    extends AsyncWordSpec
    with BaseTest
    with BeforeAndAfterAll
    with HasExecutionContext {
  this: DbTest =>

  import com.digitalasset.canton.topology.DefaultTestIdentities.*

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    val query =
      sqlu"truncate table common_static_strings restart identity"
    storage.update(
      DBIO.seq(query),
      functionFullName,
    )
  }

  def staticStringsStore(mk: () => IndexedStringStore): Unit = {

    val synchronizer2 = SynchronizerId.tryFromString("other::synchronizer")

    def d2idx(store: IndexedStringStore, synchronizerId: SynchronizerId): Future[Int] =
      store
        .getOrCreateIndex(
          IndexedStringType.synchronizerId,
          synchronizerId.toLengthLimitedString.asString300,
        )
        .failOnShutdown

    def idx2d(store: IndexedStringStore, index: Int): Future[Option[SynchronizerId]] =
      store
        .getForIndex(IndexedStringType.synchronizerId, index)
        .map(_.map(str => SynchronizerId.tryFromString(str.unwrap)))
        .failOnShutdown

    "return the same index for a previously stored uid" in {
      val store = mk()
      for {
        idx <- d2idx(store, synchronizer2)
        idx2 <- d2idx(store, synchronizerId)
        idx3 <- d2idx(store, synchronizer2)
      } yield {
        idx shouldBe idx3
        idx2 should not be idx
      }
    }

    "return the correct index for the stored uid" in {
      val store = mk()
      for {
        in1 <- d2idx(store, synchronizer2)
        in2 <- d2idx(store, synchronizerId)
        in3 <- d2idx(store, synchronizer2)
        lk1 <- idx2d(store, in1)
        lk2 <- idx2d(store, in2)
        ompt1 <- idx2d(store, 0)
        ompt2 <- idx2d(store, Math.max(in1, in2) + 1)
      } yield {
        lk1.value shouldBe synchronizer2
        lk2.value shouldBe synchronizerId
        ompt1 shouldBe empty
        ompt2 shouldBe empty
      }

    }

    "concurrent insertion" in {

      val store = mk()
      val uidsF = Future.sequence(
        (1 to 500)
          .map(x => SynchronizerId.tryFromString(s"id$x::stinkynamespace"))
          .map(x =>
            d2idx(store, x).flatMap { idx =>
              idx2d(store, idx).map(res => (x, idx, res))
            }
          )
      )
      for {
        idxs <- uidsF
      } yield {
        forAll(idxs) { case (uid, _, resIdx) =>
          resIdx.value shouldBe uid
        }
        idxs.map(_._2).distinct should have length (idxs.length.toLong)
      }

    }

  }

  "DbStaticStringStore" should {
    behave like staticStringsStore(() => new DbIndexedStringStore(storage, timeouts, loggerFactory))
  }

}

class IndexedStringsStoreTestH2 extends DbIndexedStringsStoreTest with H2Test

class IndexedStringsStoreTestPostgres extends DbIndexedStringsStoreTest with PostgresTest
