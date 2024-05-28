// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.store.backend.ResetStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation

import java.sql.Connection

object PostgresResetStorageBackend extends ResetStorageBackend {

  override def resetAll(connection: Connection): Unit = {
    SQL"""
      delete from lapi_parameters cascade;
      delete from lapi_command_completions cascade;
      delete from lapi_events_create cascade;
      delete from lapi_events_consuming_exercise cascade;
      delete from lapi_events_non_consuming_exercise cascade;
      delete from lapi_events_assign cascade;
      delete from lapi_events_unassign cascade;
      delete from lapi_party_entries cascade;
      delete from lapi_party_records cascade;
      delete from lapi_party_record_annotations cascade;
      delete from lapi_string_interning cascade;
      delete from lapi_pe_create_id_filter_stakeholder cascade;
      delete from lapi_pe_create_id_filter_non_stakeholder_informee cascade;
      delete from lapi_pe_consuming_id_filter_stakeholder cascade;
      delete from lapi_pe_consuming_id_filter_non_stakeholder_informee cascade;
      delete from lapi_pe_non_consuming_id_filter_informee cascade;
      delete from lapi_pe_assign_id_filter_stakeholder cascade;
      delete from lapi_pe_unassign_id_filter_stakeholder cascade;
      delete from lapi_transaction_meta cascade;
      delete from lapi_users cascade;
      delete from lapi_user_annotations cascade;
      delete from lapi_user_rights cascade;
      delete from lapi_identity_provider_config cascade;
      delete from lapi_transaction_metering cascade;
      delete from lapi_participant_metering cascade;
      delete from lapi_metering_parameters cascade;
    """
      .execute()(connection)
      .discard
  }
}
