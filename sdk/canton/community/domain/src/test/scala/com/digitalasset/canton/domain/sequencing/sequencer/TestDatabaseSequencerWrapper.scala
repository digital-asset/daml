// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.RegisterError
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Small wrapper around [[DatabaseSequencer]] to expose internal methods for testing
  */
final case class TestDatabaseSequencerWrapper(
    sequencer: DatabaseSequencer
) {
  def registerMemberInternal(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, RegisterError, Unit] =
    sequencer.registerMemberInternal(member, timestamp)
}
