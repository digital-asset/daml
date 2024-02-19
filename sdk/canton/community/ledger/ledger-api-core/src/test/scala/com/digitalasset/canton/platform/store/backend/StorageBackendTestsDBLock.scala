// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.platform.store.backend.DBLockStorageBackend.{Lock, LockId, LockMode}
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{Assertion, OptionValues}

import java.sql.Connection
import scala.util.Try

private[platform] trait StorageBackendTestsDBLock
    extends Matchers
    with Eventually
    with OptionValues {
  this: AnyFlatSpec =>

  protected def dbLock: DBLockStorageBackend
  protected def getConnection: Connection
  protected def lockIdSeed: Int

  behavior of "DBLockStorageBackend"

  it should "allow to acquire the same shared lock many times" in dbLockTestCase(1) { c =>
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(1)) should not be empty
  }

  it should "allow to acquire the same exclusive lock many times" in dbLockTestCase(1) { c =>
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(1)) should not be empty
  }

  it should "allow shared locking" in dbLockTestCase(2) { c =>
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(2)) should not be empty
  }

  it should "allow shared locking many times" in dbLockTestCase(5) { c =>
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(2)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(3)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(4)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(5)) should not be empty
  }

  it should "not allow exclusive when locked by shared" in dbLockTestCase(2) { c =>
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(2)) shouldBe empty
  }

  it should "not allow exclusive when locked by exclusive" in dbLockTestCase(2) { c =>
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(2)) shouldBe empty
  }

  it should "not allow shared when locked by exclusive" in dbLockTestCase(2) { c =>
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(2)) shouldBe empty
  }

  it should "unlock successfully a shared lock" in dbLockTestCase(2) { c =>
    val lock = dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(1)).value
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(2)) shouldBe empty
    dbLock.release(lock)(c(1)) shouldBe true
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(2)) should not be empty
  }

  it should "release successfully a shared lock if connection closed" in dbLockTestCase(2) { c =>
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(1)).value
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(2)) shouldBe empty
    c(1).close()
    eventually(timeout)(
      dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(2)) should not be empty
    )
  }

  it should "unlock successfully an exclusive lock" in dbLockTestCase(2) { c =>
    val lock = dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(1)).value
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(2)) shouldBe empty
    dbLock.release(lock)(c(1)) shouldBe true
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(2)) should not be empty
  }

  it should "release successfully an exclusive lock if connection closed" in dbLockTestCase(2) {
    c =>
      dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(1)).value
      dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(2)) shouldBe empty
      c(1).close()
      eventually(timeout)(
        dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(2)) should not be empty
      )
  }

  it should "be able to lock exclusive, if all shared locks are released" in dbLockTestCase(4) {
    c =>
      val shared1 = dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(1)).value
      val shared2 = dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(2)).value
      val shared3 = dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(3)).value
      dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(4)) shouldBe empty

      dbLock.release(shared1)(c(1))
      dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(4)) shouldBe empty
      dbLock.release(shared2)(c(2))
      dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(4)) shouldBe empty
      dbLock.release(shared3)(c(3))
      dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(4)) should not be empty
  }

  it should "lock immediately, or fail immediately" in dbLockTestCase(2) { c =>
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(1)) should not be empty
    val start = System.currentTimeMillis()
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(2)) shouldBe empty
    (System.currentTimeMillis() - start) should be < 500L
  }

  it should "fail to unlock lock held by others" in dbLockTestCase(2) { c =>
    val lock = dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Shared)(c(1)).value
    dbLock.release(lock)(c(2)) shouldBe false
  }

  it should "fail to unlock lock which is not held by anyone" in dbLockTestCase(1) { c =>
    dbLock.release(Lock(dbLock.lock(lockIdSeed), LockMode.Shared))(c(1)) shouldBe false
  }

  it should "fail if attempt to use backend-foreign lock-id for locking" in dbLockTestCase(1) { c =>
    Try(dbLock.tryAcquire(new LockId {}, LockMode.Shared)(c(1))).isFailure shouldBe true
  }

  it should "fail if attempt to use backend-foreign lock-id for un-locking" in dbLockTestCase(1) {
    c =>
      Try(dbLock.release(Lock(new LockId {}, LockMode.Shared))(c(1))).isFailure shouldBe true
  }

  it should "lock successfully exclusive different locks" in dbLockTestCase(1) { c =>
    dbLock.tryAcquire(dbLock.lock(lockIdSeed), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed + 1), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed + 2), LockMode.Exclusive)(c(1)) should not be empty
    dbLock.tryAcquire(dbLock.lock(lockIdSeed + 3), LockMode.Exclusive)(c(1)) should not be empty
  }

  private val timeout = Timeout(Span(10, Seconds))

  private def dbLockTestCase(
      numOfConnectionsNeeded: Int
  )(test: List[Connection] => Assertion): Assertion = {
    if (dbLock.dbLockSupported) {
      // prepending with null so we can refer to connections 1 based in tests
      val connections = null :: List.fill(numOfConnectionsNeeded)(getConnection)
      val result = Try(test(connections))
      connections.foreach(c => Try(c.close()))
      result.get
    } else {
      info(
        s"This test makes sense only for StorageBackend which supports DB-Locks. For ${dbLock.getClass.getName} StorageBackend this test is disabled."
      )
      succeed
    }
  }
}

trait StorageBackendTestsDBLockForSuite
    extends StorageBackendTestsDBLock
    with StorageBackendProvider
    with BaseTest {
  this: AnyFlatSpec =>

  override val dbLock: DBLockStorageBackend = backend.dbLock

  override def getConnection: Connection =
    backend.dataSource
      .createDataSource(DataSourceStorageBackend.DataSourceConfig(jdbcUrl), loggerFactory)
      .getConnection
}
