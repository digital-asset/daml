// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  FailingCryptoProvider,
  FakeIgnoringModuleRef,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  CancellableEvent,
  Env,
  ModuleRef,
}
import com.digitalasset.canton.topology.{SequencerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches}
import org.scalatest.Assertions.fail

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable

package object bftordering {

  private[bftordering] def emptySource[X](): Source[X, KillSwitch] =
    Source.empty.viaMat(KillSwitches.single)(Keep.right)

  private[bftordering] def endpointToTestBftNodeId(endpoint: P2PEndpoint): BftNodeId =
    // Must be parseable as a valid sequencer ID, else the network output module will crash
    //  when generating peer statuses.
    SequencerNodeId.toBftNodeId(endpointToTestSequencerId(endpoint))

  private[bftordering] def endpointToTestSequencerId(endpoint: P2PEndpoint): SequencerId =
    SequencerId(UniqueIdentifier.tryCreate("ns", s"${endpoint.address}_${endpoint.port}"))

  private[bftordering] def fakeIgnoringModule[MessageT]: ModuleRef[MessageT] =
    new FakeIgnoringModuleRef()

  private[bftordering] def fakeCellModule[ModuleMessageT, CellMessageT <: ModuleMessageT: Manifest](
      cell: AtomicReference[Option[CellMessageT]]
  ): ModuleRef[ModuleMessageT] = new ModuleRef[ModuleMessageT] {
    override def asyncSend(
        msg: ModuleMessageT
    )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
      msg match {
        case cellMsg: CellMessageT => cell.set(Some(cellMsg))
        case other => fail(s"Unexpected message $other")
      }
  }

  private[bftordering] def fakeRecordingModule[MessageT](
      buffer: mutable.ArrayBuffer[MessageT]
  ): ModuleRef[MessageT] = new ModuleRef[MessageT] {
    override def asyncSend(
        msg: MessageT
    )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
      buffer += msg
  }

  private[bftordering] def fakeModuleExpectingSilence[MessageT]: ModuleRef[MessageT] =
    new ModuleRef[MessageT] {
      override def asyncSend(
          msg: MessageT
      )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
        fail(s"Module should not receive any requests but received $msg")
    }

  private[bftordering] def fakeCancellableEventExpectingSilence: CancellableEvent =
    new FakeCancellableEvent(() => fail("Module should not cancel delayed event"))

  private[bftordering] def failingCryptoProvider[E <: Env[E]]: CryptoProvider[E] =
    new FailingCryptoProvider()
}
