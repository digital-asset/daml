// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.platform.store.backend.ResetStorageBackend

object MResetStorageBackend extends ResetStorageBackend {
  override def reset(connection: Connection): Unit = {
    val base = MStore(connection)
    base.synchronized {
      base.prunedUpToInclusive = null
      base.allDivulgedContractsPrunedUpToInclusive = null
      base.identityParams = null
      base.ledgerEnd = null
      base.commandSubmissions.clear()
      base.mData = MData(
        packages = base.mData.packages
      )
    }
  }

  override def resetAll(connection: Connection): Unit = {
    val base = MStore(connection)
    base.synchronized {
      base.prunedUpToInclusive = null
      base.allDivulgedContractsPrunedUpToInclusive = null
      base.identityParams = null
      base.ledgerEnd = null
      base.commandSubmissions.clear()
      base.mData = MData()
    }
  }
}
