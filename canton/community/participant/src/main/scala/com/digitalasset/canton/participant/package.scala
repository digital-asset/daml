// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.lf.data.Time
import com.digitalasset.canton.data.{Counter, CounterCompanion}
import com.digitalasset.canton.ledger.offset

package object participant {

  // Sync event and offset used by participant state ReadService api
  type LedgerSyncOffset = offset.Offset
  val LedgerSyncOffset: offset.Offset.type = offset.Offset

  implicit class RichRequestCounter(val rc: RequestCounter) extends AnyVal {

    /** Use this method to indicate that unwrapping to use the request counter as
      * a local offset is fine.
      */
    def asLocalOffset: LocalOffset = LocalOffset(rc.unwrap)
  }

  type LocalOffsetDiscriminator
  type LocalOffset = Counter[LocalOffsetDiscriminator]
  val LocalOffset: CounterCompanion[LocalOffsetDiscriminator] =
    new CounterCompanion[LocalOffsetDiscriminator] {}

  implicit class RichLocalOffset(val offset: LocalOffset) extends AnyVal {
    def asRequestCounter: RequestCounter = RequestCounter(offset.unwrap)
  }

  // Ledger record time is "single-dimensional"
  type LedgerSyncRecordTime = Time.Timestamp
  val LedgerSyncRecordTime: Time.Timestamp.type = Time.Timestamp
}
