// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.oracle

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.platform.store.backend.ResetStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation

import java.sql.Connection

object OracleResetStorageBackend extends ResetStorageBackend {

  override def resetAll(connection: Connection): Unit =
    List(
      "packages",
      "lapi_package_entries",
      "lapi_parameters",
      "lapi_command_completions",
      "lapi_events_create",
      "lapi_events_consuming_exercise",
      "lapi_events_non_consuming_exercise",
      "lapi_events_assign",
      "lapi_events_unassign",
      "lapi_party_entries",
      "lapi_party_records",
      "lapi_party_record_annotations",
      "lapi_string_interning",
      "lapi_pe_create_id_filter_stakeholder",
      "lapi_pe_create_id_filter_non_stakeholder_informee",
      "lapi_pe_consuming_id_filter_stakeholder",
      "lapi_pe_consuming_id_filter_non_stakeholder_informee",
      "lapi_pe_non_consuming_id_filter_informee",
      "lapi_pe_assign_id_filter_stakeholder",
      "lapi_pe_unassign_id_filter_stakeholder",
      "lapi_transaction_meta",
      "lapi_users",
      "lapi_user_rights",
      "lapi_user_annotations",
      "lapi_identity_provider_config",
      "lapi_transaction_metering",
      "lapi_participant_metering",
      "lapi_metering_parameters",
    ) foreach { table =>
      SQL"delete from #$table".execute()(connection).discard
    }

}
