// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.protocol.MessageDispatcher.{
  ParticipantTopologyProcessor,
  RequestProcessors,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionDomainTracker
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.sequencing.traffic.TrafficControlProcessor
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContext

object ParallelMessageDispatcherFactory
    extends MessageDispatcher.Factory[ParallelMessageDispatcher] {
  // Process only transactions asynchronously
  private def processAsynchronously(viewType: ViewType): Boolean = viewType == TransactionViewType

  override def create(
      protocolVersion: ProtocolVersion,
      domainId: DomainId,
      participantId: ParticipantId,
      requestTracker: RequestTracker,
      requestProcessors: RequestProcessors,
      topologyProcessor: ParticipantTopologyProcessor,
      trafficProcessor: TrafficControlProcessor,
      acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
      requestCounterAllocator: RequestCounterAllocator,
      recordOrderPublisher: RecordOrderPublisher,
      badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
      repairProcessor: RepairProcessor,
      inFlightSubmissionDomainTracker: InFlightSubmissionDomainTracker,
      loggerFactory: NamedLoggerFactory,
      metrics: SyncDomainMetrics,
  )(implicit ec: ExecutionContext, tracer: Tracer): ParallelMessageDispatcher =
    new ParallelMessageDispatcher(
      protocolVersion,
      domainId,
      participantId,
      requestTracker,
      requestProcessors,
      topologyProcessor,
      trafficProcessor,
      acsCommitmentProcessor,
      requestCounterAllocator,
      recordOrderPublisher,
      badRootHashMessagesRequestProcessor,
      repairProcessor,
      inFlightSubmissionDomainTracker,
      processAsynchronously,
      loggerFactory,
      metrics,
    )
}
