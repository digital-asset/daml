// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection

import com.daml.platform.store.backend.DBLockStorageBackend.{Lock, LockId, LockMode}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future
import scala.util.Try

private[backend] trait StorageBackendTestsDBLock extends Matchers with StorageBackendSpec {
  this: AsyncFlatSpec =>

  behavior of "DBLockStorageBackend"

  it should "allow to acquire the same shared lock many times" in dbLockTestCase(1) { c =>
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(1)) should not be empty
  }

  it should "allow to acquire the same exclusive lock many times" in dbLockTestCase(1) { c =>
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(1)) should not be empty
  }

  it should "allow shared locking" in dbLockTestCase(2) { c =>
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(2)) should not be empty
  }

  it should "allow shared locking many times" in dbLockTestCase(5) { c =>
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(2)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(3)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(4)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(5)) should not be empty
  }

  it should "not allow shared locked by exclusive" in dbLockTestCase(2) { c =>
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
  }

  it should "not allow exclusive locked by exclusive" in dbLockTestCase(2) { c =>
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
  }

  it should "not allow exclusive locked by shared" in dbLockTestCase(2) { c =>
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(2)) shouldBe empty
  }

  it should "unlock successfully a shared lock" in dbLockTestCase(2) { c =>
    val lock = backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(1)).get
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
    backend.release(lock)(c(1)) shouldBe true
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(2)) should not be empty
  }

  it should "release successfully a shared lock if connection closed" in dbLockTestCase(2) { c =>
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(1)).get
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
    c(1).close()
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(2)) should not be empty
  }

  it should "unlock successfully an exclusive lock" in dbLockTestCase(2) { c =>
    val lock = backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(1)).get
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
    backend.release(lock)(c(1)) shouldBe true
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(2)) should not be empty
  }

  it should "release successfully an exclusive lock if connection closed" in dbLockTestCase(2) {
    c =>
      backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(1)).get
      backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
      c(1).close()
      backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(2)) should not be empty
  }

  it should "be able to lock exclusive, if all shared locks are released" in dbLockTestCase(4) {
    c =>
      backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(1)) should not be empty
      backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(2)) should not be empty
      backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(3)) should not be empty
      backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(4)) shouldBe empty

      c(1).close()
      backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(4)) shouldBe empty
      c(2).close()
      backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(4)) shouldBe empty
      c(3).close()
      backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(4)) should not be empty
  }

  it should "lock immediately, or fail immediately" in dbLockTestCase(2) { c =>
    backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(1)) should not be empty
    val start = System.currentTimeMillis()
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(2)) shouldBe empty
    (System.currentTimeMillis() - start) should be < 100L
  }

  it should "fail to unlock lock held by others" in dbLockTestCase(2) { c =>
    val lock = backend.tryAcquire(backend.lock(1), LockMode.Shared)(c(1)).get
    backend.release(lock)(c(2)) shouldBe false
  }

  it should "fail to unlock lock which is not held by anyone" in dbLockTestCase(1) { c =>
    backend.release(Lock(backend.lock(1), LockMode.Shared))(c(1)) shouldBe false
  }

  it should "fail if attempt to use backend-foreign lock-id for locking" in dbLockTestCase(1) { c =>
    Try(backend.tryAcquire(new LockId {}, LockMode.Shared)(c(1))).isFailure shouldBe true
  }

  it should "fail if attempt to use backend-foreign lock-id for un-locking" in dbLockTestCase(1) {
    c =>
      Try(backend.release(Lock(new LockId {}, LockMode.Shared))(c(1))).isFailure shouldBe true
  }

  it should "lock successfully exclusive different locks" in dbLockTestCase(1) { c =>
    backend.tryAcquire(backend.lock(1), LockMode.Exclusive)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(2), LockMode.Exclusive)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(3), LockMode.Exclusive)(c(1)) should not be empty
    backend.tryAcquire(backend.lock(4), LockMode.Exclusive)(c(1)) should not be empty
  }

  private def dbLockTestCase(
      numOfConnectionsNeeded: Int
  )(test: List[Connection] => Any): Future[Assertion] = {
    val dataSource = backend.createDataSource(jdbcUrl)
    if (backend.dbLockSupported) {
      // prepending with null so we can refer to connections 1 based in tests
      val connections = null :: List.fill(numOfConnectionsNeeded)(dataSource.getConnection)
      val result = Try(test(connections))
      connections.foreach(c => Try(c.close()))
      Future.fromTry(result).map(_ => 1 shouldBe 1)
    } else {
      info(
        s"This test makes sense only for StorageBackend which supports DB-Locks. For ${backend.getClass.getName} StorageBackend this test is disabled."
      )
      Future.successful(1 shouldBe 1)
    }
  }
}
