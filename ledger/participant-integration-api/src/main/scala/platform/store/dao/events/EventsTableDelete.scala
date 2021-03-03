// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import anorm.{Row, SimpleSql, SqlStringInterpolation}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.platform.store.Conversions.OffsetToStatement

object EventsTableDelete {

  /** Delete
    * - archive events before or at specified offset and
    * - create events that have been archived before or at the specific offset.
    */
  def prepareEventsDelete(endInclusive: Offset): SimpleSql[Row] =
    SQL"""
          delete from participant_events as delete_events
          where
            delete_events.event_offset <= $endInclusive and
            delete_events.event_kind in (10, 0) and   -- create and divulgence events
            exists (
              SELECT 1 FROM participant_events as archive_events
              WHERE
                archive_events.event_offset <= $endInclusive AND
                archive_events.event_kind = 20 AND -- consuming
                archive_events.contract_id = delete_events.contract_id
            );
          delete from participant_events as delete_events
          where
            delete_events.event_offset <= $endInclusive and
            delete_events.event_kind in (20, 25);  -- consuming and non-consuming exercise events
       """

}
