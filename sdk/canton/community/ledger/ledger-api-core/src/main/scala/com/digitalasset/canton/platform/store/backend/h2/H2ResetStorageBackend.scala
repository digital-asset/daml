// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.h2

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.store.backend.ResetStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation

import java.sql.Connection

object H2ResetStorageBackend extends ResetStorageBackend {

  override def resetAll(connection: Connection): Unit = {
    SQL"""
      set referential_integrity false;
      truncate table lapi_parameters;
      truncate table lapi_ledger_end_synchronizer_index;
      truncate table lapi_command_completions;
      truncate table lapi_events_activate_contract;
      truncate table lapi_filter_activate_stakeholder;
      truncate table lapi_filter_activate_witness;
      truncate table lapi_events_deactivate_contract;
      truncate table lapi_filter_deactivate_stakeholder;
      truncate table lapi_filter_deactivate_witness;
      truncate table lapi_events_various_witnessed;
      truncate table lapi_filter_various_witness;
      truncate table lapi_party_entries;
      truncate table lapi_party_records;
      truncate table lapi_party_record_annotations;
      truncate table lapi_events_party_to_participant;
      truncate table lapi_string_interning;
      truncate table lapi_update_meta;
      truncate table lapi_users;
      truncate table lapi_user_rights;
      truncate table lapi_user_annotations;
      truncate table lapi_identity_provider_config;
      truncate table par_pruning_operation;
      truncate table par_contracts;
      set referential_integrity true;
    """
      .execute()(connection)
      .discard
    ()
  }

}
