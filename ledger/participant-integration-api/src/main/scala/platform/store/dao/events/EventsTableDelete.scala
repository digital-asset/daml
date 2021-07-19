// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, SimpleSql, SqlStringInterpolation}
import com.daml.ledger.offset.Offset
import com.daml.platform.store.Conversions.OffsetToStatement

object EventsTableDelete {

  /** Delete
    * - archive events before or at specified offset and
    * - create events that have been archived before or at the specific offset.
    */
  def prepareEventsDelete(endInclusive: Offset): SimpleSql[Row] =
    SQL"delete from participant_events where event_offset <= $endInclusive and (create_argument is null or create_consumed_at <= $endInclusive)"

}
