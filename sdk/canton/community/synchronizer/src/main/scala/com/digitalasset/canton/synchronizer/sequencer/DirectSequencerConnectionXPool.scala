// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port, PositiveInt}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.ConnectionX.ConnectionXConfig
import com.digitalasset.canton.sequencing.SequencerConnectionXPool.{
  SequencerConnectionXPoolConfig,
  SequencerConnectionXPoolError,
  SequencerConnectionXPoolHealth,
}
import com.digitalasset.canton.sequencing.{SequencerConnectionX, SequencerConnectionXPool}
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}

import scala.concurrent.ExecutionContextExecutor

/** This connection pool is meant to be used to create a sequencer client that connects directly to
  * an in-process sequencer. Needed for cases when the sequencer node itself needs to listen to
  * specific events such as identity events.
  */
class DirectSequencerConnectionXPool(
    sequencer: Sequencer,
    mySynchronizerId: PhysicalSynchronizerId,
    sequencerId: SequencerId,
    staticParameters: StaticSynchronizerParameters,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends SequencerConnectionXPool {
  import DirectSequencerConnectionXPool.*

  private val directConnection = new DirectSequencerConnectionX(
    directConnectionDummyConfig,
    sequencer,
    mySynchronizerId,
    sequencerId,
    staticParameters,
    timeouts,
    loggerFactory,
  )

  override def physicalSynchronizerIdO: Option[PhysicalSynchronizerId] = Some(mySynchronizerId)

  override def staticSynchronizerParametersO: Option[StaticSynchronizerParameters] = Some(
    staticParameters
  )

  override def start()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXPoolError, Unit] =
    EitherTUtil.unitUS

  override val config: SequencerConnectionXPoolConfig = directPoolConfig

  override def updateConfig(newConfig: SequencerConnectionXPoolConfig)(implicit
      traceContext: TraceContext
  ): Either[SequencerConnectionXPoolError, Unit] =
    ErrorUtil.internalError(
      new UnsupportedOperationException(
        s"$functionFullName is not implemented for DirectSequencerConnectionXPool"
      )
    )

  override val health: SequencerConnectionXPoolHealth =
    new SequencerConnectionXPoolHealth.AlwaysHealthy("direct-pool-health", logger)

  override def nbSequencers: NonNegativeInt = NonNegativeInt.one

  override def nbConnections: NonNegativeInt = NonNegativeInt.one

  override def getConnections(requester: String, nb: PositiveInt, exclusions: Set[SequencerId])(
      implicit traceContext: TraceContext
  ): Set[SequencerConnectionX] = Set(directConnection)

  override def getOneConnectionPerSequencer(requester: String)(implicit
      traceContext: TraceContext
  ): Map[SequencerId, SequencerConnectionX] = Map(sequencerId -> directConnection)

  override def getAllConnections()(implicit traceContext: TraceContext): Seq[SequencerConnectionX] =
    Seq(directConnection)

  override val contents: Map[SequencerId, Set[SequencerConnectionX]] = Map(
    sequencerId -> Set(directConnection)
  )

  override def isThresholdStillReachable(
      threshold: PositiveInt,
      ignored: Set[ConnectionXConfig],
      extraUndecided: NonNegativeInt,
  )(implicit traceContext: TraceContext): Boolean = true
}

object DirectSequencerConnectionXPool {
  private val directConnectionDummyConfig = ConnectionXConfig(
    name = "direct-connection",
    endpoint = Endpoint("dummy-endpoint-direct-connection", Port.tryCreate(0)),
    transportSecurity = false,
    customTrustCertificates = None,
    expectedSequencerIdO = None,
    tracePropagation = TracingConfig.Propagation.Disabled,
  )

  private val directPoolConfig = SequencerConnectionXPoolConfig(
    connections = NonEmpty(Seq, directConnectionDummyConfig),
    trustThreshold = PositiveInt.one,
  )
}
