// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import com.codahale.metrics.MetricRegistry

private[app] object Metrics {

  val IndexServicePrefix: String = MetricRegistry.name("daml", "services", "index")
  val ReadServicePrefix: String = MetricRegistry.name("daml", "services", "read")
  val WriteServicePrefix: String = MetricRegistry.name("daml", "services", "write")

}
