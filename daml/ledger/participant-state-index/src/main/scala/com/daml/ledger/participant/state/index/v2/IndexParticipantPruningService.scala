// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext

import scala.concurrent.Future

/** Serves as a backend to implement
  * ParticipantPruningService.
  */
trait IndexParticipantPruningService {
  def prune(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      loggingContext: LoggingContext
  ): Future[Unit]

}
