// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import java.sql.Connection

import anorm.SQL
import com.daml.platform.store.backend.ResetStorageBackend

object OracleResetStorageBackend extends ResetStorageBackend {
  override def reset(connection: Connection): Unit =
    List(
      "truncate table configuration_entries cascade",
      "truncate table package_entries cascade",
      "truncate table parameters cascade",
      "truncate table participant_command_completions cascade",
      "truncate table participant_command_submissions cascade",
      "truncate table participant_events_divulgence cascade",
      "truncate table participant_events_create cascade",
      "truncate table participant_events_consuming_exercise cascade",
      "truncate table participant_events_non_consuming_exercise cascade",
      "truncate table party_entries cascade",
      "truncate table string_interning cascade",
    ).map(SQL(_)).foreach(_.execute()(connection))

  override def resetAll(connection: Connection): Unit =
    List(
      "truncate table configuration_entries cascade",
      "truncate table packages cascade",
      "truncate table package_entries cascade",
      "truncate table parameters cascade",
      "truncate table participant_command_completions cascade",
      "truncate table participant_command_submissions cascade",
      "truncate table participant_events_divulgence cascade",
      "truncate table participant_events_create cascade",
      "truncate table participant_events_consuming_exercise cascade",
      "truncate table participant_events_non_consuming_exercise cascade",
      "truncate table party_entries cascade",
      "truncate table string_interning cascade",
    ).map(SQL(_)).foreach(_.execute()(connection))
}
