// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggingContext
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.{MediatorRequest, MediatorResponse, Verdict}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.Ordered.orderingToOrdered
import scala.concurrent.{ExecutionContext, Future}

final case class FinalizedResponse(
    override val requestId: RequestId,
    override val request: MediatorRequest,
    override val version: CantonTimestamp,
    verdict: Verdict,
)(val requestTraceContext: TraceContext)
    extends ResponseAggregator {

  override def isFinalized: Boolean = true

  /** Merely validates the request and raises alarms. But there is nothing to progress any more */
  override def validateAndProgress(
      responseTimestamp: CantonTimestamp,
      response: MediatorResponse,
      topologySnapshot: TopologySnapshot,
  )(implicit
      loggingContext: NamedLoggingContext,
      ec: ExecutionContext,
  ): Future[Option[ResponseAggregation[VKey]]] = {
    val MediatorResponse(
      _requestId,
      sender,
      viewHashO,
      viewPositionO,
      localVerdict,
      rootHashO,
      confirmingParties,
      _domainId,
    ) = response

    def go[VKEY: ViewKey](viewKeyO: Option[VKEY]): Future[Option[Nothing]] = {
      (for {
        _ <- validateResponse(
          viewKeyO,
          rootHashO,
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
    }

    if (
      verdict.representativeProtocolVersion >=
        Verdict.protocolVersionRepresentativeFor(ProtocolVersion.v5)
    ) go(viewPositionO)
    else go(viewHashO)
  }
}
