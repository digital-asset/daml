// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.platform.store.backend.IntegrityStorageBackend
import DbDtoUtils._

object MIntegrityStorageBackend extends IntegrityStorageBackend {
  override def verifyIntegrity()(connection: Connection): Unit = MStore(connection) { mStore =>
    val events = mStore.events
    val ledgerEndEventSeqId = mStore.ledgerEnd.lastEventSeqId
    if (events.nonEmpty)
      events.takeWhile(_.eventSeqId <= ledgerEndEventSeqId).reduce { (prev, curr) =>
        if (prev.eventSeqId == curr.eventSeqId)
          throw new RuntimeException(s"Found duplicate id: ${prev.eventSeqId}.")
        if (prev.eventSeqId + 1 != curr.eventSeqId)
          throw new RuntimeException(
            s"Found non consecutive event ids: ${prev.eventSeqId} ${curr.eventSeqId} "
          )
        if (prev.offset >= curr.offset)
          throw new RuntimeException("Found duplicate or non consecutive event offsets.")
        curr
      }
    ()
  }
}
