// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.LoggingContextWithTrace

import scala.concurrent.Future

/** Serves as a backend to implement ParticipantPruningService.
  */
trait IndexParticipantPruningService {
  def prune(
      previousPruneUpToInclusive: Option[Offset],
      previousIncompleteReassignmentOffsets: Vector[Offset],
      pruneUpToInclusive: Offset,
      incompletReassignmentOffsets: Vector[Offset],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit]

  def indexDbPrunedUpTo(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Offset]]
}
