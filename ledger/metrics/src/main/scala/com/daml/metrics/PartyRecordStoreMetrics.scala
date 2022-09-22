// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry

class PartyRecordStoreMetrics(
    override val prefix: MetricName,
    override val registry: MetricRegistry,
) extends MetricHandle.FactoryWithDBMetrics {

  val getPartyRecord: DatabaseMetrics = createDbMetrics("get_party_record")
  val createPartyRecord: DatabaseMetrics = createDbMetrics("create_party_record")
  val updatePartyRecord: DatabaseMetrics = createDbMetrics("update_party_record")
}
