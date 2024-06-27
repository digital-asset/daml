// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.data.Offset
import com.digitalasset.daml.lf.data.Time

package object participant {

  // Sync event and offset used by participant state ReadService api
  type LedgerSyncOffset = Offset
  val LedgerSyncOffset: Offset.type = data.Offset

  // Ledger record time is "single-dimensional"
  type LedgerSyncRecordTime = Time.Timestamp
  val LedgerSyncRecordTime: Time.Timestamp.type = Time.Timestamp
}
