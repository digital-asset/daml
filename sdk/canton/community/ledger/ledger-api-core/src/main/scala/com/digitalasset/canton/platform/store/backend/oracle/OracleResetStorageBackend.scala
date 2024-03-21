// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.oracle

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.platform.store.backend.ResetStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation

import java.sql.Connection

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
      "participant_events_assign",
      "participant_events_unassign",
      "party_entries",
      "participant_party_records",
      "participant_party_record_annotations",
      "string_interning",
      "pe_create_id_filter_stakeholder",
      "pe_create_id_filter_non_stakeholder_informee",
      "pe_consuming_id_filter_stakeholder",
      "pe_consuming_id_filter_non_stakeholder_informee",
      "pe_non_consuming_id_filter_informee",
      "pe_assign_id_filter_stakeholder",
      "pe_unassign_id_filter_stakeholder",
      "participant_transaction_meta",
      "participant_users",
      "participant_user_rights",
      "participant_user_annotations",
      "participant_identity_provider_config",
      "transaction_metering",
      "participant_metering",
      "metering_parameters",
    ) foreach { table =>
      SQL"delete from #$table".execute()(connection).discard
    }

}
