// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

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
          -- Divulgence events
          delete from participant_events_divulgence as delete_events
          where
            delete_events.event_offset <= $endInclusive and
            exists (
              SELECT 1 FROM participant_events_consuming_exercise as archive_events
              WHERE
                archive_events.event_offset <= $endInclusive AND
                archive_events.contract_id = delete_events.contract_id
            );
          -- Create events
          delete from participant_events_create as delete_events
          where
            delete_events.event_offset <= $endInclusive and
            exists (
              SELECT 1 FROM participant_events_consuming_exercise as archive_events
              WHERE
                archive_events.event_offset <= $endInclusive AND
                archive_events.contract_id = delete_events.contract_id
            );
          -- Exercise events
          delete from participant_events_consuming_exercise as delete_events
          where
            delete_events.event_offset <= $endInclusive and
          delete from participant_events_non_>consuming_exercise as delete_events
          where
            delete_events.event_offset <= $endInclusive and
       """

}
