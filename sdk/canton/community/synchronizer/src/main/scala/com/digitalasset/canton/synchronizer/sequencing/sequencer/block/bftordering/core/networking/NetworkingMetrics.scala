// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.networking

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1.BftOrderingMessageBody
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1.BftOrderingMessageBody.Message
import com.digitalasset.canton.topology.SequencerId

private[networking] object NetworkingMetrics {

  def emitConnectedCount(
      metrics: BftOrderingMetrics,
      knownPeers: KnownPeers,
  )(implicit
      mc: MetricsContext
  ): Unit =
    metrics.p2p.connections.connected.updateValue(knownPeers.getEndpoints.size)

  def emitAuthenticatedCount(
      metrics: BftOrderingMetrics,
      knownPeers: KnownPeers,
  )(implicit
      mc: MetricsContext
  ): Unit =
    metrics.p2p.connections.authenticated.updateValue(knownPeers.authenticatedCount)

  def emitSendStats(
      metrics: BftOrderingMetrics,
      message: BftOrderingMessageBody,
  )(implicit
      mc: MetricsContext
  ): Unit = {
    metrics.p2p.send.sentBytes.mark(message.serializedSize.toLong)
    metrics.p2p.send.sentMessages.mark(1L)
  }

  def sendMetricsContext(
      metrics: BftOrderingMetrics,
      message: BftOrderingMessageBody,
      to: SequencerId,
      droppedAsUnauthenticated: Boolean,
  )(implicit mc: MetricsContext): MetricsContext = {
    type TargetModule = metrics.p2p.send.labels.targetModule.values.TargetModuleValue
    val targetModule: Option[TargetModule] = // Help type inference
      message.message match {
        case Message.Empty => None
        case Message.AvailabilityMessage(_) =>
          Some(metrics.p2p.send.labels.targetModule.values.Availability)
        case Message.ConsensusMessage(_) =>
          Some(metrics.p2p.send.labels.targetModule.values.Consensus)
        case Message.RetransmissionMessage(_) =>
          Some(metrics.p2p.send.labels.targetModule.values.Consensus)
        case Message.StateTransferMessage(_) =>
          Some(metrics.p2p.send.labels.targetModule.values.Consensus)
      }
    val mc1 = mc.withExtraLabels(
      metrics.p2p.send.labels.TargetSequencer -> to.toProtoPrimitive,
      metrics.p2p.send.labels.DroppedAsUnauthenticated -> droppedAsUnauthenticated.toString,
    )
    targetModule
      .map(targetModule =>
        mc1.withExtraLabels(metrics.p2p.send.labels.targetModule.Key -> targetModule)
      )
      .getOrElse(mc1)
  }

  def emitReceiveStats(
      metrics: BftOrderingMetrics,
      size: Long,
  )(implicit
      mc: MetricsContext
  ): Unit = {
    metrics.p2p.receive.receivedBytes.mark(size)
    metrics.p2p.receive.receivedMessages.mark(1L)
  }

  def receiveMetricsContext(
      metrics: BftOrderingMetrics
  )(
      source: metrics.p2p.receive.labels.source.values.SourceValue
  )(implicit mc: MetricsContext): MetricsContext = {
    val sourceSequencerId = source match {
      case metrics.p2p.receive.labels.source.values.SourceParsingFailed => None
      case metrics.p2p.receive.labels.source.values.Empty(from) => Some(from)
      case metrics.p2p.receive.labels.source.values.Availability(from) => Some(from)
      case metrics.p2p.receive.labels.source.values.Consensus(from) => Some(from)
      case metrics.p2p.receive.labels.source.values.Retransmissions(from) => Some(from)
      case metrics.p2p.receive.labels.source.values.StateTransfer(from) => Some(from)
    }
    val mc1 = mc.withExtraLabels(
      metrics.p2p.receive.labels.source.Key -> source
    )
    val mc2 = sourceSequencerId
      .map(from =>
        mc1.withExtraLabels(metrics.p2p.receive.labels.SourceSequencer -> from.toProtoPrimitive)
      )
      .getOrElse(mc)
    mc2
  }

  def emitIdentityEquivocation(
      metrics: BftOrderingMetrics,
      fromEndpoint: Endpoint,
      fromSequencerId: SequencerId,
  )(implicit
      mc: MetricsContext
  ): Unit =
    metrics.security.noncompliant.behavior.mark()(
      mc.withExtraLabels(
        metrics.security.noncompliant.labels.Endpoint -> fromEndpoint.toString,
        metrics.security.noncompliant.labels.Sequencer -> fromSequencerId.toProtoPrimitive,
        metrics.security.noncompliant.labels.violationType.Key -> metrics.security.noncompliant.labels.violationType.values.AuthIdentityEquivocation,
      )
    )
}
