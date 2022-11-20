// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import java.sql.Connection

import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.daml.platform.store.backend.ResetStorageBackend

object OracleResetStorageBackend extends ResetStorageBackend {

  override def resetAll(connection: Connection): Unit =
    List(
      "configuration_entries",
      "packages",
      "package_entries",
      "parameters",
      "participant_command_completions",
      "participant_events_divulgence",
      "participant_events_create",
      "participant_events_consuming_exercise",
      "participant_events_non_consuming_exercise",
      "party_entries",
      "participant_party_records",
      "participant_party_record_annotations",
      "string_interning",
      "participant_events_create_filter",
      "participant_users",
      "participant_user_rights",
      "participant_user_annotations",
      "participant_identity_provider_config",
      "transaction_metering",
      "participant_metering",
      "metering_parameters",
    ) foreach { table =>
      SQL"delete from #$table".execute()(connection)
    }

}
