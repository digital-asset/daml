// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.h2

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.platform.store.backend.ResetStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation

import java.sql.Connection

object H2ResetStorageBackend extends ResetStorageBackend {

  override def resetAll(connection: Connection): Unit = {
    SQL"""
      set referential_integrity false;
      truncate table lapi_packages;
      truncate table lapi_package_entries;
      truncate table lapi_parameters;
      truncate table lapi_command_completions;
      truncate table lapi_events_create;
      truncate table lapi_events_consuming_exercise;
      truncate table lapi_events_non_consuming_exercise;
      truncate table lapi_events_assign;
      truncate table lapi_events_unassign;
      truncate table lapi_party_entries;
      truncate table lapi_party_records;
      truncate table lapi_party_record_annotations;
      truncate table lapi_string_interning;
      truncate table lapi_pe_create_id_filter_stakeholder;
      truncate table lapi_pe_create_id_filter_non_stakeholder_informee;
      truncate table lapi_pe_consuming_id_filter_stakeholder;
      truncate table lapi_pe_consuming_id_filter_non_stakeholder_informee;
      truncate table lapi_pe_non_consuming_id_filter_informee;
      truncate table lapi_pe_assign_id_filter_stakeholder;
      truncate table lapi_pe_unassign_id_filter_stakeholder;
      truncate table lapi_transaction_meta;
      truncate table lapi_users;
      truncate table lapi_user_rights;
      truncate table lapi_user_annotations;
      truncate table lapi_identity_provider_config;
      truncate table lapi_transaction_metering;
      truncate table lapi_participant_metering;
      truncate table lapi_metering_parameters;
      set referential_integrity true;
    """
      .execute()(connection)
      .discard
    ()
  }

}
