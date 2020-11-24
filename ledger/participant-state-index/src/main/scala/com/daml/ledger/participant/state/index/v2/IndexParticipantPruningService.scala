// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.participant.state.v1.Offset
import com.daml.logging.LoggingContext

import scala.concurrent.Future

/**
  * Serves as a backend to implement
  * ParticipantPruningService.
  */
trait IndexParticipantPruningService {

  def prune(pruneUpToInclusive: Offset)(implicit loggingContext: LoggingContext): Future[Unit]

}
