// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import java.sql.Connection

import anorm.SQL
import com.daml.platform.store.backend.ResetStorageBackend

object H2ResetStorageBackend extends ResetStorageBackend {

  override def reset(connection: Connection): Unit = {
    SQL("""set referential_integrity false;
          |truncate table configuration_entries;
          |truncate table package_entries;
          |truncate table parameters;
          |truncate table participant_command_completions;
          |truncate table participant_command_submissions;
          |truncate table participant_events_divulgence;
          |truncate table participant_events_create;
          |truncate table participant_events_consuming_exercise;
          |truncate table participant_events_non_consuming_exercise;
          |truncate table party_entries;
          |truncate table string_interning;
          |truncate table participant_events_create_filter;
          |set referential_integrity true;""".stripMargin)
      .execute()(connection)
    ()
  }

  override def resetAll(connection: Connection): Unit = {
    SQL("""set referential_integrity false;
          |truncate table configuration_entries;
          |truncate table packages;
          |truncate table package_entries;
          |truncate table parameters;
          |truncate table participant_command_completions;
          |truncate table participant_command_submissions;
          |truncate table participant_events_divulgence;
          |truncate table participant_events_create;
          |truncate table participant_events_consuming_exercise;
          |truncate table participant_events_non_consuming_exercise;
          |truncate table party_entries;
          |truncate table string_interning;
          |truncate table participant_events_create_filter;
          |set referential_integrity true;""".stripMargin)
      .execute()(connection)
    ()
  }
}
