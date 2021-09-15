// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection

import com.daml.logging.LoggingContext
import com.daml.platform.store.backend.DBLockStorageBackend.{Lock, LockId, LockMode}
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.Future
import scala.util.Try

private[platform] trait StorageBackendTestsDBLock extends Matchers with Eventually {
  this: AsyncFlatSpec =>

  protected def dbLock: DBLockStorageBackend
  protected def getConnection: Connection

  behavior of "DBLockStorageBackend"

  it should "allow to acquire the same shared lock many times" in dbLockTestCase(1) { c =>
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(1)) should not be empty
  }

  it should "allow to acquire the same exclusive lock many times" in dbLockTestCase(1) { c =>
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(1)) should not be empty
  }

  it should "allow shared locking" in dbLockTestCase(2) { c =>
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(2)) should not be empty
  }

  it should "allow shared locking many times" in dbLockTestCase(5) { c =>
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(2)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(3)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(4)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(5)) should not be empty
  }

  it should "not allow exclusive when locked by shared" in dbLockTestCase(2) { c =>
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
  }

  it should "not allow exclusive when locked by exclusive" in dbLockTestCase(2) { c =>
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
  }

  it should "not allow shared when locked by exclusive" in dbLockTestCase(2) { c =>
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(2)) shouldBe empty
  }

  it should "unlock successfully a shared lock" in dbLockTestCase(2) { c =>
    val lock = dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(1)).get
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
    dbLock.release(lock)(c(1)) shouldBe true
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(2)) should not be empty
  }

  it should "release successfully a shared lock if connection closed" in dbLockTestCase(2) { c =>
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(1)).get
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
    c(1).close()
    eventually(timeout)(
      dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(2)) should not be empty
    )
  }

  it should "unlock successfully an exclusive lock" in dbLockTestCase(2) { c =>
    val lock = dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(1)).get
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
    dbLock.release(lock)(c(1)) shouldBe true
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(2)) should not be empty
  }

  it should "release successfully an exclusive lock if connection closed" in dbLockTestCase(2) {
    c =>
      dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(1)).get
      dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
      c(1).close()
      eventually(timeout)(
        dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(2)) should not be empty
      )
  }

  it should "be able to lock exclusive, if all shared locks are released" in dbLockTestCase(4) {
    c =>
      val shared1 = dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(1)).get
      val shared2 = dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(2)).get
      val shared3 = dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(3)).get
      dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(4)) shouldBe empty

      dbLock.release(shared1)(c(1))
      dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(4)) shouldBe empty
      dbLock.release(shared2)(c(2))
      dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(4)) shouldBe empty
      dbLock.release(shared3)(c(3))
      dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(4)) should not be empty
  }

  it should "lock immediately, or fail immediately" in dbLockTestCase(2) { c =>
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(1)) should not be empty
    val start = System.currentTimeMillis()
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
    (System.currentTimeMillis() - start) should be < 500L
  }

  it should "fail to unlock lock held by others" in dbLockTestCase(2) { c =>
    val lock = dbLock.tryAcquire(dbLock.lock(1), LockMode.Shared)(c(1)).get
    dbLock.release(lock)(c(2)) shouldBe false
  }

  it should "fail to unlock lock which is not held by anyone" in dbLockTestCase(1) { c =>
    dbLock.release(Lock(dbLock.lock(1), LockMode.Shared))(c(1)) shouldBe false
  }

  it should "fail if attempt to use backend-foreign lock-id for locking" in dbLockTestCase(1) { c =>
    Try(dbLock.tryAcquire(new LockId {}, LockMode.Shared)(c(1))).isFailure shouldBe true
  }

  it should "fail if attempt to use backend-foreign lock-id for un-locking" in dbLockTestCase(1) {
    c =>
      Try(dbLock.release(Lock(new LockId {}, LockMode.Shared))(c(1))).isFailure shouldBe true
  }

  it should "lock successfully exclusive different locks" in dbLockTestCase(1) { c =>
    dbLock.tryAcquire(dbLock.lock(1), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(2), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(3), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(4), LockMode.Exclusive)(c(1)) should not be empty
  }

  private val timeout = Timeout(Span(10, Seconds))

  private def dbLockTestCase(
      numOfConnectionsNeeded: Int
  )(test: List[Connection] => Any): Future[Assertion] = {
    if (dbLock.dbLockSupported) {
      // prepending with null so we can refer to connections 1 based in tests
      val connections = null :: List.fill(numOfConnectionsNeeded)(getConnection)
      val result = Try(test(connections))
      connections.foreach(c => Try(c.close()))
      Future.fromTry(result).map(_ => 1 shouldBe 1)
    } else {
      info(
        s"This test makes sense only for StorageBackend which supports DB-Locks. For ${dbLock.getClass.getName} StorageBackend this test is disabled."
      )
      Future.successful(1 shouldBe 1)
    }
  }
}

trait StorageBackendTestsDBLockForSuite
    extends StorageBackendTestsDBLock
    with StorageBackendProvider {
  this: AsyncFlatSpec =>

  override def dbLock: DBLockStorageBackend = backend

  override def getConnection: Connection =
    backend.createDataSource(jdbcUrl)(LoggingContext.ForTesting).getConnection
}
