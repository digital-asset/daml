// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Time

/** Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService]]
  */
trait TimeService {
  def getLedgerRecordTimeStream(): Source[Time.Timestamp, NotUsed]
}
