// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.lf.data.Time
import com.daml.logging.LoggingContext

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService]]
  */
trait IndexTimeService {
  def getLedgerRecordTimeStream()(implicit
      loggingContext: LoggingContext
  ): Source[Time.Timestamp, NotUsed]
}
