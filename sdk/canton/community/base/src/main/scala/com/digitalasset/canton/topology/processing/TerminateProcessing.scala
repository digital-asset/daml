// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** An implementation of this trait allows to schedule code to be executed at the end of the
  * processing of a batch of topology transactions.
  * On the participant, this *must* tick the record order publisher before returning.
  */
trait TerminateProcessing {

  /** Changes to the topology stores need to be persisted before this method is called.
    */
  def terminate(
      sc: SequencerCounter,
      sequencedTime: SequencedTime,
      effectiveTime: EffectiveTime,
  )(implicit traceContext: TraceContext, executionContext: ExecutionContext): Future[Unit]
}

object TerminateProcessing {

  /** On the participant, [[TerminateProcessing.terminate]] should tick the record order publisher when processing
    * is finished. Hence, this no-op terminate processing should be used only in domain nodes.
    */

  private[canton] object NoOpTerminateTopologyProcessing extends TerminateProcessing {
    override def terminate(
        sc: SequencerCounter,
        sequencedTime: SequencedTime,
        effectiveTime: EffectiveTime,
    )(implicit traceContext: TraceContext, executionContext: ExecutionContext): Future[Unit] =
      Future.unit
  }
}
