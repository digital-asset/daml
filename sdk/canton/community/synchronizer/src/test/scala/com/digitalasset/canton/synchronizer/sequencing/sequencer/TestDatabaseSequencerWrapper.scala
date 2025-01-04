// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import Sequencer.RegisterError

/** Small wrapper around [[DatabaseSequencer]] to expose internal methods for testing
  */
final case class TestDatabaseSequencerWrapper(
    sequencer: DatabaseSequencer
) {
  def registerMemberInternal(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, RegisterError, Unit] =
    sequencer.registerMemberInternal(member, timestamp)
}
