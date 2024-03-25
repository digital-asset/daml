// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** This class allows to query domain parameters easily.
  * Type parameter `P` is the type of the returned value.
  */
class DynamicDomainParametersLookup[P](
    projector: DynamicDomainParameters => P,
    topologyClient: DomainTopologyClient,
    protocolVersion: ProtocolVersion,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Return one value, valid at the specified timestamp
    *
    * @param warnOnUsingDefaults Log a warning if dynamic domain parameters are not set
    *                            and default value is used.
    */
  def get(validAt: CantonTimestamp, warnOnUsingDefaults: Boolean = true)(implicit
      traceContext: TraceContext
  ): Future[P] = futureSupervisor
    .supervised(s"Querying for domain parameters valid at $validAt") {
      topologyClient.awaitSnapshot(validAt)
    }
    .flatMap(_.findDynamicDomainParametersOrDefault(protocolVersion, warnOnUsingDefaults))
    .map(projector)

  /** Return the value of the topology snapshot approximation
    * or the default value.
    */
  def getApproximateOrDefaultValue(warnOnUsingDefaults: Boolean = true)(implicit
      traceContext: TraceContext
  ): Future[P] =
    topologyClient.currentSnapshotApproximation
      .findDynamicDomainParametersOrDefault(protocolVersion, warnOnUsingDefaults)
      .map(projector)

  /** Return the value of the topology snapshot approximation.
    */
  def getApproximate()(implicit traceContext: TraceContext): Future[Option[P]] =
    topologyClient.currentSnapshotApproximation
      .findDynamicDomainParameters()
      .map(_.map(p => projector(p.parameters)).toOption)

  /** Return a list of parameters, together with their validity interval,
    *
    * @param warnOnUsingDefaults Log a warning if dynamic domain parameters are not set
    *                            and default value is used.
    */
  def getAll(validAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[DomainParameters.WithValidity[P]]] =
    futureSupervisor
      .supervised(s"Querying for list of domain parameters changes valid at $validAt") {
        topologyClient.awaitSnapshot(validAt)
      }
      .flatMap(_.listDynamicDomainParametersChanges())
      .map { domainParametersChanges =>
        domainParametersChanges.map(_.map(projector))
      }

  /** Return the approximate latest validity/freshness.
    * Returned value is the approximate timestamp of the `TopologyClient`.
    */
  def approximateTimestamp: CantonTimestamp = topologyClient.approximateTimestamp
}

object DomainParametersLookup {
  def forSequencerDomainParameters(
      staticDomainParameters: StaticDomainParameters,
      overrideMaxRequestSize: Option[NonNegativeInt],
      topologyClient: DomainTopologyClient,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): DynamicDomainParametersLookup[SequencerDomainParameters] = {
    new DynamicDomainParametersLookup(
      params =>
        SequencerDomainParameters(
          params.confirmationRequestsMaxRate,
          overrideMaxRequestSize.map(MaxRequestSize).getOrElse(params.maxRequestSize),
        ),
      topologyClient,
      staticDomainParameters.protocolVersion,
      futureSupervisor,
      loggerFactory,
    )
  }

  final case class SequencerDomainParameters(
      confirmationRequestsMaxRate: NonNegativeInt,
      maxRequestSize: MaxRequestSize,
  )
}
