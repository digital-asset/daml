// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import com.daml.ledger.participant.state.metrics.MetricName

private[app] object Metrics {

  private val ServicePrefix = MetricName.DAML :+ "services"

  val IndexServicePrefix: MetricName = ServicePrefix :+ "index"
  val ReadServicePrefix: MetricName = ServicePrefix :+ "read"
  val WriteServicePrefix: MetricName = ServicePrefix :+ "write"

}
