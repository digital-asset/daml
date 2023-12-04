// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.participant.store.SerializableLedgerSyncEvent
import com.digitalasset.canton.participant.sync.LedgerSyncEvent
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, Traced}
import slick.jdbc.GetResult

object ParticipantStorageImplicits {

  // the reader and setting for the LedgerSyncEvent can throw DbSerializationException and DbDeserializationExceptions
  // which is currently permitted for storage operations as we have no practical alternative with the slick api
  private[db] implicit def getLedgerSyncEvent(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[LedgerSyncEvent] = {
    GetResult(r => bytesToEvent(r.<<[Array[Byte]]))
  }

  private def bytesToEvent(bytes: Array[Byte]): LedgerSyncEvent = {
    SerializableLedgerSyncEvent
      .fromByteArrayUnsafe(bytes)
      .fold(
        err =>
          throw new DbDeserializationException(
            s"LedgerSyncEvent protobuf deserialization error: $err"
          ),
        _.event,
      )
  }

  private[db] implicit def getTracedLedgerSyncEvent(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[Traced[LedgerSyncEvent]] =
    GetResult { r =>
      val event = GetResult[SerializableLedgerSyncEvent].apply(r).event
      implicit val traceContext: TraceContext = GetResult[SerializableTraceContext].apply(r).unwrap

      Traced(event)
    }
}
