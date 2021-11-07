// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import java.sql.Connection

import com.daml.lf.data.Time
import com.daml.logging.LoggingContext
import com.daml.platform.store.backend.DeduplicationStorageBackend

object MDeduplicationStorageBackend extends DeduplicationStorageBackend {
  override def deduplicatedUntil(
      deduplicationKey: String
  )(connection: Connection): Time.Timestamp = {
    val commandSubmissions = MStore(connection).commandSubmissions
    commandSubmissions.synchronized {
      Time.Timestamp(commandSubmissions(deduplicationKey))
    }
  }

  override def upsertDeduplicationEntry(
      key: String,
      submittedAt: Time.Timestamp,
      deduplicateUntil: Time.Timestamp,
  )(connection: Connection)(implicit loggingContext: LoggingContext): Int = {
    val commandSubmissions = MStore(connection).commandSubmissions
    commandSubmissions.synchronized {
      commandSubmissions.get(key) match {
        case None =>
          commandSubmissions += key -> deduplicateUntil.micros
          1

        case Some(micros) if micros < submittedAt.micros =>
          commandSubmissions += key -> deduplicateUntil.micros
          1

        case _ =>
          0
      }
    }
  }

  override def removeExpiredDeduplicationData(
      currentTime: Time.Timestamp
  )(connection: Connection): Unit = {
    val commandSubmissions = MStore(connection).commandSubmissions
    commandSubmissions.synchronized {
      commandSubmissions.filterInPlace((_, micros) => micros >= currentTime.micros)
    }
    ()
  }

  override def stopDeduplicatingCommand(deduplicationKey: String)(connection: Connection): Unit = {
    val commandSubmissions = MStore(connection).commandSubmissions
    commandSubmissions.synchronized {
      commandSubmissions -= deduplicationKey
    }
    ()
  }
}
