// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.lf.data.Time
import com.digitalasset.canton.ledger.offset

package object participant {

  // Sync event and offset used by participant state ReadService api
  type LedgerSyncOffset = offset.Offset
  val LedgerSyncOffset: offset.Offset.type = offset.Offset

  // Ledger record time is "single-dimensional"
  type LedgerSyncRecordTime = Time.Timestamp
  val LedgerSyncRecordTime: Time.Timestamp.type = Time.Timestamp
}
