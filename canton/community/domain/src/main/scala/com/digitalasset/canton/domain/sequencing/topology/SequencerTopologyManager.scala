// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.topology

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyChangeOp}
import com.digitalasset.canton.topology.{TopologyManager, TopologyManagerError}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class SequencerTopologyManager(
    clock: Clock,
    override val store: TopologyStore[TopologyStoreId.AuthorizedStore],
    crypto: Crypto,
    override protected val timeouts: ProcessingTimeout,
    protocolVersion: ProtocolVersion,
    loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext)
    extends TopologyManager[TopologyManagerError](
      clock,
      crypto,
      store,
      timeouts,
      protocolVersion,
      loggerFactory,
      futureSupervisor,
    )(
      ec
    ) {
  override protected def checkNewTransaction(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      force: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyManagerError, Unit] =
    EitherT.rightT[Future, TopologyManagerError](())

  override protected def notifyObservers(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Future[Unit] = Future.unit

  override protected def wrapError(error: TopologyManagerError)(implicit
      traceContext: TraceContext
  ): TopologyManagerError = error
}
