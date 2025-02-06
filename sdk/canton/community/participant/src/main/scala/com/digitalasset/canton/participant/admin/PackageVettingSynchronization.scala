// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.Future

// TODO(i21341) remove this synchronization logic once topology events are published on the ledger api
trait PackageVettingSynchronization {
  def sync(packages: Set[PackageId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, ParticipantTopologyManagerError, Unit]
}

object PackageVettingSynchronization {
  object NoSync extends PackageVettingSynchronization {
    override def sync(packages: Set[PackageId])(implicit
        traceContext: TraceContext
    ): EitherT[Future, ParticipantTopologyManagerError, Unit] = EitherTUtil.unit
  }
}
