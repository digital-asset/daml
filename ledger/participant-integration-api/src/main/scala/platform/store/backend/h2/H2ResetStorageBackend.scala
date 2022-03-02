// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import java.sql.Connection

import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.ResetStorageBackend

object H2ResetStorageBackend extends ResetStorageBackend {

  override def resetAll(connection: Connection): Unit = {
    SQL"""
      set referential_integrity false;
      truncate table configuration_entries;
      truncate table packages;
      truncate table package_entries;
      truncate table parameters;
      truncate table participant_command_completions;
      truncate table participant_events_divulgence;
      truncate table participant_events_create;
      truncate table participant_events_consuming_exercise;
      truncate table participant_events_non_consuming_exercise;
      truncate table party_entries;
      truncate table string_interning;
      truncate table participant_events_create_filter;
      truncate table participant_users;
      truncate table participant_user_rights;
      truncate table transaction_metering;
      truncate table participant_metering;
      truncate table metering_parameters;
      set referential_integrity true;
    """
      .execute()(connection)
    ()
  }

}
