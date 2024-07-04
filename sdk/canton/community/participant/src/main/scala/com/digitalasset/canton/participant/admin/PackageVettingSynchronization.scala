// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.daml.lf.data.Ref.PackageId

// TODO(#15087) remove this synchronization logic once topology events are published on the ledger api
trait PackageVettingSynchronization {
  def sync(packages: Set[PackageId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit]
}

object PackageVettingSynchronization {
  object NoSync extends PackageVettingSynchronization {
    override def sync(packages: Set[PackageId])(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] = EitherTUtil.unitUS
  }
}
