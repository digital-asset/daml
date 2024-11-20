// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.canton.data.AbsoluteOffset
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

/** Serves as a backend to implement
  * ParticipantPruningService.
  */
trait IndexParticipantPruningService {
  def prune(
      pruneUpToInclusive: AbsoluteOffset,
      pruneAllDivulgedContracts: Boolean,
      incompletReassignmentOffsets: Vector[AbsoluteOffset],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit]

}
