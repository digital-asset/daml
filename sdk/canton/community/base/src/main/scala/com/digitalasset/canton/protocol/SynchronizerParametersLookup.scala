// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

/** This class allows to query synchronizer parameters easily.
  * Type parameter `P` is the type of the returned value.
  */
class DynamicSynchronizerParametersLookup[P](
    projector: DynamicSynchronizerParameters => P,
    topologyClient: SynchronizerTopologyClient,
    protocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Return one value, valid at the specified timestamp
    *
    * @param warnOnUsingDefaults Log a warning if dynamic synchronizer parameters are not set
    *                            and default value is used.
    */
  def get(validAt: CantonTimestamp, warnOnUsingDefaults: Boolean = true)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[P] = topologyClient
    .awaitSnapshotUSSupervised(s"Querying for synchronizer parameters valid at $validAt")(validAt)
    .flatMap(snapshot =>
      snapshot.findDynamicSynchronizerParametersOrDefault(protocolVersion, warnOnUsingDefaults)
    )
    .map(projector)

  /** Return the value of the topology snapshot approximation
    * or the default value.
    */
  def getApproximateOrDefaultValue(warnOnUsingDefaults: Boolean = true)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[P] =
    topologyClient.currentSnapshotApproximation
      .findDynamicSynchronizerParametersOrDefault(protocolVersion, warnOnUsingDefaults)
      .map(projector)

  /** Return the value of the topology snapshot approximation.
    */
  def getApproximate()(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[P]] =
    topologyClient.currentSnapshotApproximation
      .findDynamicSynchronizerParameters()
      .map(_.map(p => projector(p.parameters)).toOption)

  /** Return the approximate latest validity/freshness.
    * Returned value is the approximate timestamp of the `TopologyClient`.
    */
  def approximateTimestamp: CantonTimestamp = topologyClient.approximateTimestamp
}

object SynchronizerParametersLookup {
  def forSequencerSynchronizerParameters(
      staticSynchronizerParameters: StaticSynchronizerParameters,
      overrideMaxRequestSize: Option[NonNegativeInt],
      topologyClient: SynchronizerTopologyClient,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): DynamicSynchronizerParametersLookup[SequencerSynchronizerParameters] =
    new DynamicSynchronizerParametersLookup(
      params =>
        SequencerSynchronizerParameters(
          params.confirmationRequestsMaxRate,
          overrideMaxRequestSize.map(MaxRequestSize.apply).getOrElse(params.maxRequestSize),
        ),
      topologyClient,
      staticSynchronizerParameters.protocolVersion,
      loggerFactory,
    )

  def forAcsCommitmentSynchronizerParameters(
      pv: ProtocolVersion,
      topologyClient: SynchronizerTopologyClient,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): DynamicSynchronizerParametersLookup[AcsCommitmentSynchronizerParameters] =
    new DynamicSynchronizerParametersLookup(
      params =>
        AcsCommitmentSynchronizerParameters(
          params.reconciliationInterval,
          params.acsCommitmentsCatchUp,
        ),
      topologyClient,
      pv,
      loggerFactory,
    )

  final case class SequencerSynchronizerParameters(
      confirmationRequestsMaxRate: NonNegativeInt,
      maxRequestSize: MaxRequestSize,
  )

  final case class AcsCommitmentSynchronizerParameters(
      reconciliationInterval: PositiveSeconds,
      acsCommitmentsCatchUp: Option[AcsCommitmentsCatchUpParameters],
  )
}
