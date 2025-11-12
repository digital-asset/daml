// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.store.backend.ResetStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation

import java.sql.Connection

object PostgresResetStorageBackend extends ResetStorageBackend {

  override def resetAll(connection: Connection): Unit =
    SQL"""
      delete from lapi_parameters cascade;
      delete from lapi_ledger_end_synchronizer_index cascade;
      delete from lapi_command_completions cascade;
      delete from lapi_events_activate_contract cascade;
      delete from lapi_filter_activate_stakeholder cascade;
      delete from lapi_filter_activate_witness cascade;
      delete from lapi_events_deactivate_contract cascade;
      delete from lapi_filter_deactivate_stakeholder cascade;
      delete from lapi_filter_deactivate_witness cascade;
      delete from lapi_events_various_witnessed cascade;
      delete from lapi_filter_various_witness cascade;
      delete from lapi_party_entries cascade;
      delete from lapi_party_records cascade;
      delete from lapi_party_record_annotations cascade;
      delete from lapi_events_party_to_participant cascade;
      delete from lapi_string_interning cascade;
      delete from lapi_update_meta cascade;
      delete from lapi_users cascade;
      delete from lapi_user_annotations cascade;
      delete from lapi_user_rights cascade;
      delete from lapi_identity_provider_config cascade;
    """
      .execute()(connection)
      .discard
}
