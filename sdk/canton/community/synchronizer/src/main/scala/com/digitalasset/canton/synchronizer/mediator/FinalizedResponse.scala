// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggingContext
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResponse,
  MediatorConfirmationRequest,
  Verdict,
}
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

final case class FinalizedResponse(
    override val requestId: RequestId,
    override val request: MediatorConfirmationRequest,
    finalizationTime: CantonTimestamp,
    verdict: Verdict,
)(val requestTraceContext: TraceContext)
    extends ResponseAggregator {

  override def version: CantonTimestamp = finalizationTime
  override def isFinalized: Boolean = true

  /** Merely validates the request and raises alarms. But there is nothing to progress any more */
  override protected[synchronizer] def validateAndProgressInternal(
      responseTimestamp: CantonTimestamp,
      response: ConfirmationResponse,
      rootHash: RootHash,
      sender: ParticipantId,
      topologySnapshot: TopologySnapshot,
  )(implicit
      loggingContext: NamedLoggingContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[ResponseAggregation[VKey]]] = {
    val ConfirmationResponse(
      viewPositionO,
      localVerdict,
      confirmingParties,
    ) = response

    def go[VKEY: ViewKey](viewKeyO: Option[VKEY]): FutureUnlessShutdown[Option[Nothing]] =
      (for {
        _ <- validateResponse(
          viewKeyO,
          rootHash,
          responseTimestamp,
          sender,
          localVerdict,
          topologySnapshot,
          confirmingParties,
        )
      } yield {
        loggingContext.debug(
          s"Request ${requestId.unwrap} has already been finalized with verdict $verdict before response $responseTimestamp from $sender with $localVerdict for view $viewKeyO arrives"
        )
        None
      }).value.map(_.flatten)

    go(viewPositionO)
  }
}
