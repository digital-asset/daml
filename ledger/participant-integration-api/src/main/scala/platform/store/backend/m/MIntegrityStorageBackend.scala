// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.platform.store.backend.IntegrityStorageBackend
import DbDtoUtils._

object MIntegrityStorageBackend extends IntegrityStorageBackend {
  override def verifyIntegrity()(connection: Connection): Unit = {
    val mStore = MStore(connection)
    val events = mStore.mData.events
    val ledgerEndEventSeqId = mStore.ledgerEnd.lastEventSeqId
    if (events.isEmpty) ()
    else
      events.takeWhile(_.eventSeqId <= ledgerEndEventSeqId).reduce { (prev, curr) =>
        if (prev.eventSeqId == curr.eventSeqId)
          throw new Exception(s"Found duplicate id: ${prev.eventSeqId}.")
        if (prev.eventSeqId + 1 != curr.eventSeqId)
          throw new Exception(
            s"Found non consecutive event ids: ${prev.eventSeqId} ${curr.eventSeqId} "
          )
        if (prev.offset >= curr.offset)
          throw new Exception("Found duplicate or non consecutive event offsets.")
        curr
      }
    ()
  }
}
