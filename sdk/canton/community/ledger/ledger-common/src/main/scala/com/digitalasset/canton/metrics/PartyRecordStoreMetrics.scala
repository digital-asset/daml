// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.DatabaseMetrics
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName

class PartyRecordStoreMetrics(
    prefix: MetricName,
    labeledMetricsFactory: LabeledMetricsFactory,
) extends DatabaseMetricsFactory(prefix, labeledMetricsFactory) {

  val getPartyRecord: DatabaseMetrics = createDbMetrics("get_party_record")
  val partiesExist: DatabaseMetrics = createDbMetrics("parties_exist")
  val createPartyRecord: DatabaseMetrics = createDbMetrics("create_party_record")
  val updatePartyRecord: DatabaseMetrics = createDbMetrics("update_party_record")
  val updatePartyRecordIdp: DatabaseMetrics = createDbMetrics("update_party_record_idp")

}
