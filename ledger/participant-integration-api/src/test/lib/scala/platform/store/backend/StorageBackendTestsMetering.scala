// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.platform.store.backend.MeteringStorageBackend.TransactionMetering
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsMetering
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (metering)"

  import StorageBackendTestValues._

  it should "persist transaction metering" in {

    val now = someTime
    val toOffset = offset(5)

    val metering = TransactionMetering(
      someApplicationId,
      actionCount = 1,
      fromTimestamp = now.addMicros(2),
      toTimestamp = now.addMicros(3),
      fromLedgerOffset = offset(4),
      toLedgerOffset = toOffset,
    )

    val expected = metering
    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(Vector(dtoTransactionMetering(metering)), _))
    executeSql(updateLedgerEnd(toOffset, 5L))
    val Vector(actual) = executeSql(backend.metering.entries)
    actual shouldBe expected

  }

}
