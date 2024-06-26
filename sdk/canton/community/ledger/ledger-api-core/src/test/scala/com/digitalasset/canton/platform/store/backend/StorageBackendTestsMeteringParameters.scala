// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsMeteringParameters
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  {
    behavior of "StorageBackend (metering parameters)"

    val initLedgerMeteringEnd = LedgerMeteringEnd(Offset.beforeBegin, Timestamp.Epoch)
    val loggerFactory = SuppressingLogger(getClass)

    it should "fetch un-initialized ledger metering end" in {
      executeSql(backend.meteringParameter.ledgerMeteringEnd) shouldBe None
    }

    it should "initialized ledger metering end" in {
      val expected = LedgerMeteringEnd(Offset.beforeBegin, Timestamp.Epoch)
      executeSql(backend.meteringParameter.initializeLedgerMeteringEnd(expected, loggerFactory))
      executeSql(backend.meteringParameter.ledgerMeteringEnd) shouldBe Some(expected)
    }

    it should "update ledger metering end with `before begin` offset" in {
      executeSql(
        backend.meteringParameter.initializeLedgerMeteringEnd(initLedgerMeteringEnd, loggerFactory)
      )
      val expected = LedgerMeteringEnd(Offset.beforeBegin, Timestamp.now())
      executeSql(backend.meteringParameter.updateLedgerMeteringEnd(expected))
      executeSql(backend.meteringParameter.ledgerMeteringEnd) shouldBe Some(expected)
    }

    it should "update ledger metering end with valid offset" in {
      executeSql(
        backend.meteringParameter.initializeLedgerMeteringEnd(initLedgerMeteringEnd, loggerFactory)
      )
      val expected = LedgerMeteringEnd(
        Offset.fromHexString(Ref.HexString.assertFromString("07")),
        Timestamp.now(),
      )
      executeSql(backend.meteringParameter.updateLedgerMeteringEnd(expected))
      executeSql(backend.meteringParameter.ledgerMeteringEnd) shouldBe Some(expected)
    }

  }

}
