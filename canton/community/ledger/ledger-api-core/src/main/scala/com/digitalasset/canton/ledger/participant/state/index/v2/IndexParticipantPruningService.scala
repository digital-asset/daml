// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

/** Serves as a backend to implement
  * ParticipantPruningService.
  */
trait IndexParticipantPruningService {
  def prune(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit]

}
