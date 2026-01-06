// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{byteArray, int, str}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.backend.Conversions.{parties, timestampFromMicros}
import com.digitalasset.canton.platform.store.backend.RowDef.column
import com.digitalasset.canton.platform.store.backend.{Conversions, RowDef}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.{CommandId, Party}
import com.digitalasset.canton.protocol.UpdateId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Time.Timestamp

object CommonRowDefs {
  // basic column types
  def offset(offsetColumnName: String): RowDef[Offset] =
    column(offsetColumnName, Conversions.offset)
  def genSynchronizerId(columnName: String)(
      stringInterning: StringInterning
  ): RowDef[SynchronizerId] =
    column(columnName, int(_).map(stringInterning.synchronizerId.externalize))
  def synchronizerId(stringInterning: StringInterning): RowDef[SynchronizerId] =
    genSynchronizerId("synchronizer_id")(stringInterning)

  // update related
  val updateId: RowDef[UpdateId] = column("update_id", Conversions.updateId)
  val recordTime: RowDef[Timestamp] = column("record_time", timestampFromMicros)
  val traceContext: RowDef[Array[Byte]] = column("trace_context", byteArray(_))
  val commandId: RowDef[CommandId] = column("command_id", str).map(CommandId.assertFromString)
  def submitters(stringInterning: StringInterning): RowDef[Seq[Party]] =
    column("submitters", parties(stringInterning)(_))
  val publicationTime: RowDef[Timestamp] = column("publication_time", timestampFromMicros)
}
