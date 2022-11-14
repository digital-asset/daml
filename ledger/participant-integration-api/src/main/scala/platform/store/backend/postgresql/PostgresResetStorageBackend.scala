// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.Connection

import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.ResetStorageBackend

object PostgresResetStorageBackend extends ResetStorageBackend {

  override def resetAll(connection: Connection): Unit = {
    SQL"""
      truncate table configuration_entries cascade;
      truncate table packages cascade;
      truncate table package_entries cascade;
      truncate table parameters cascade;
      truncate table participant_command_completions cascade;
      truncate table participant_transaction_meta cascade;
      truncate table participant_events_divulgence cascade;
      truncate table participant_events_create cascade;
      truncate table participant_events_consuming_exercise cascade;
      truncate table participant_events_non_consuming_exercise cascade;
      truncate table party_entries cascade;
      truncate table participant_party_records cascade;
      truncate table participant_party_record_annotations cascade;
      truncate table string_interning cascade;
      truncate table participant_events_create_filter cascade;
      truncate table pe_create_filter_nonstakeholder_informees cascade;
      truncate table pe_consuming_exercise_filter_stakeholders cascade;
      truncate table pe_consuming_exercise_filter_nonstakeholder_informees cascade;
      truncate table pe_non_consuming_exercise_filter_informees cascade;
      truncate table participant_users cascade;
      truncate table participant_users cascade;
      truncate table participant_user_annotations cascade;
      truncate table participant_user_rights cascade;
      truncate table transaction_metering cascade;
      truncate table participant_metering cascade;
      truncate table metering_parameters cascade;
    """
      .execute()(connection)
    ()
  }
}
