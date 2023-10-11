// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.data.CantonTimestamp

object DefaultLedgerSyncEvent {
  def dummyStateUpdate(
      timestamp: CantonTimestamp = CantonTimestamp.Epoch
  ): LedgerSyncEvent =
    LedgerSyncEvent.PublicPackageUpload(List.empty, None, timestamp.toLf, None)
}
