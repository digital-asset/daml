// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext
import scala.util.Success

/** Utilities shared between checks in phase 1 and phase 3
  */
private[transfer] object TransferOutValidationUtil {

  def confirmingAdminParticipants(
      topologySnapshot: TopologySnapshot,
      adminParty: LfPartyId,
      logger: TracedLogger,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]] =
    FutureUnlessShutdown.outcomeF(
      topologySnapshot
        .activeParticipantsOf(adminParty)
        .map(_.filter { case (_, attributes) => attributes.permission.canConfirm })
        .andThen {
          case Success(adminParticipants) if adminParticipants.sizeCompare(1) != 0 =>
            val participants = adminParticipants.keySet
            logger.warn(
              s"Admin party $adminParty hosted with confirmation permission on $participants."
            )
        }
    )

}
