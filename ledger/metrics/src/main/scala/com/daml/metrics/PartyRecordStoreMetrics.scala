// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.api.{MetricDoc, MetricName}

@MetricDoc.GroupTag(
  representative = "daml.party_record_store.<operation>",
  groupableClass = classOf[DatabaseMetrics],
)
class PartyRecordStoreMetrics(
    prefix: MetricName,
    factory: Factory,
) extends DatabaseMetricsFactory(prefix, factory) {

  val getPartyRecord: DatabaseMetrics = createDbMetrics("get_party_record")
  val createPartyRecord: DatabaseMetrics = createDbMetrics("create_party_record")
  val updatePartyRecord: DatabaseMetrics = createDbMetrics("update_party_record")

}
