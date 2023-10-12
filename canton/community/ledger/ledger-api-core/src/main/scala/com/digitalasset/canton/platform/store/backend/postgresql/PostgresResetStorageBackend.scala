// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.platform.store.backend.ResetStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation

import java.sql.Connection

object PostgresResetStorageBackend extends ResetStorageBackend {

  override def resetAll(connection: Connection): Unit = {
    SQL"""
      delete from configuration_entries cascade;
      delete from packages cascade;
      delete from package_entries cascade;
      delete from parameters cascade;
      delete from participant_command_completions cascade;
      delete from participant_events_divulgence cascade;
      delete from participant_events_create cascade;
      delete from participant_events_consuming_exercise cascade;
      delete from participant_events_non_consuming_exercise cascade;
      delete from participant_events_assign cascade;
      delete from participant_events_unassign cascade;
      delete from party_entries cascade;
      delete from participant_party_records cascade;
      delete from participant_party_record_annotations cascade;
      delete from string_interning cascade;
      delete from pe_create_id_filter_stakeholder cascade;
      delete from pe_create_id_filter_non_stakeholder_informee cascade;
      delete from pe_consuming_id_filter_stakeholder cascade;
      delete from pe_consuming_id_filter_non_stakeholder_informee cascade;
      delete from pe_non_consuming_id_filter_informee cascade;
      delete from pe_assign_id_filter_stakeholder cascade;
      delete from pe_unassign_id_filter_stakeholder cascade;
      delete from participant_transaction_meta cascade;
      delete from participant_users cascade;
      delete from participant_user_annotations cascade;
      delete from participant_user_rights cascade;
      delete from participant_identity_provider_config cascade;
      delete from transaction_metering cascade;
      delete from participant_metering cascade;
      delete from metering_parameters cascade;
    """
      .execute()(connection)
      .discard
  }
}
