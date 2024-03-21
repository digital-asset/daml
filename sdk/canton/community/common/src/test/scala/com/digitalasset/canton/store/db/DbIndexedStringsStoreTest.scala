// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.{IndexedStringStore, IndexedStringType}
import com.digitalasset.canton.topology.DomainId
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

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*
    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
        sqlu"truncate table static_strings restart identity"
      case _: DbStorage.Profile.Oracle =>
        sqlu"truncate table static_strings"
    }
    storage.update(
      DBIO.seq(query),
      functionFullName,
    )
  }

  def staticStringsStore(mk: () => IndexedStringStore): Unit = {

    val domain2 = DomainId.tryFromString("other::domain")

    def d2idx(store: IndexedStringStore, domainId: DomainId): Future[Int] =
      store.getOrCreateIndex(IndexedStringType.domainId, domainId.toLengthLimitedString.asString300)

    def idx2d(store: IndexedStringStore, index: Int): Future[Option[DomainId]] =
      store
        .getForIndex(IndexedStringType.domainId, index)
        .map(_.map(str => DomainId.tryFromString(str.unwrap)))

    "return the same index for a previously stored uid" in {
      val store = mk()
      for {
        idx <- d2idx(store, domain2)
        idx2 <- d2idx(store, domainId)
        idx3 <- d2idx(store, domain2)
      } yield {
        idx shouldBe idx3
        idx2 should not be idx
      }
    }

    "return the correct index for the stored uid" in {
      val store = mk()
      for {
        in1 <- d2idx(store, domain2)
        in2 <- d2idx(store, domainId)
        in3 <- d2idx(store, domain2)
        lk1 <- idx2d(store, in1)
        lk2 <- idx2d(store, in2)
        ompt1 <- idx2d(store, 0)
        ompt2 <- idx2d(store, Math.max(in1, in2) + 1)
      } yield {
        lk1.value shouldBe domain2
        lk2.value shouldBe domainId
        ompt1 shouldBe empty
        ompt2 shouldBe empty
      }

    }

    "concurrent insertion" in {

      val store = mk()
      val uidsF = Future.sequence(
        (1 to 500)
          .map(x => DomainId.tryFromString(s"id${x}::stinkynamespace"))
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
