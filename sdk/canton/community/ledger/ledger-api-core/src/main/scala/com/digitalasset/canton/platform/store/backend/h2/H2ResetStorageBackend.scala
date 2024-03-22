// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
      truncate table configuration_entries;
      truncate table packages;
      truncate table package_entries;
      truncate table parameters;
      truncate table participant_command_completions;
      truncate table participant_events_divulgence;
      truncate table participant_events_create;
      truncate table participant_events_consuming_exercise;
      truncate table participant_events_non_consuming_exercise;
      truncate table participant_events_assign;
      truncate table participant_events_unassign;
      truncate table party_entries;
      truncate table participant_party_records;
      truncate table participant_party_record_annotations;
      truncate table string_interning;
      truncate table pe_create_id_filter_stakeholder;
      truncate table pe_create_id_filter_non_stakeholder_informee;
      truncate table pe_consuming_id_filter_stakeholder;
      truncate table pe_consuming_id_filter_non_stakeholder_informee;
      truncate table pe_non_consuming_id_filter_informee;
      truncate table pe_assign_id_filter_stakeholder;
      truncate table pe_unassign_id_filter_stakeholder;
      truncate table participant_transaction_meta;
      truncate table participant_users;
      truncate table participant_user_rights;
      truncate table participant_user_annotations;
      truncate table participant_identity_provider_config;
      truncate table transaction_metering;
      truncate table participant_metering;
      truncate table metering_parameters;
      set referential_integrity true;
    """
      .execute()(connection)
      .discard
    ()
  }

}
