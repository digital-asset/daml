// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.shortType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.BlockTransferRequest
import com.digitalasset.canton.tracing.TraceContext

import scala.util.{Failure, Success}

object StateTransferMessageValidator {

  def validateBlockTransferRequest(
      request: BlockTransferRequest,
      activeMembership: Membership,
  ): Either[String, Unit] = {
    val from = request.from
    val nodes = activeMembership.sortedNodes

    for {
      _ <- Either.cond(
        nodes.contains(from),
        (),
        s"'$from' is requesting state transfer while not being active, active nodes are: $nodes",
      )
      _ <- Either.cond(
        request.epoch > Genesis.GenesisEpochNumber,
        (),
        s"state transfer is supported only after genesis, but start epoch ${request.epoch} received",
      )
    } yield ()
  }

  def verifyStateTransferMessage[E <: Env[E]](
      unverifiedMessage: SignedMessage[Consensus.StateTransferMessage.StateTransferNetworkMessage],
      activeMembership: Membership,
      activeCryptoProvider: CryptoProvider[E],
      loggerFactory: NamedLoggerFactory,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val logger = loggerFactory.getLogger(getClass)
    if (activeMembership.orderingTopology.nodes.contains(unverifiedMessage.from)) {
      context.pipeToSelf(
        activeCryptoProvider
          .verifySignedMessage(
            unverifiedMessage,
            AuthenticatedMessageType.BftSignedStateTransferMessage,
          )
      ) {
        case Failure(exception) =>
          logger.error(
            s"Message $unverifiedMessage from ${unverifiedMessage.from} could not be verified, dropping",
            exception,
          )
          None
        case Success(Left(errors)) =>
          logger.warn(
            s"Message $unverifiedMessage from ${unverifiedMessage.from} failed verified, dropping: $errors"
          )
          None
        case Success(Right(())) =>
          Some(
            Consensus.StateTransferMessage.VerifiedStateTransferMessage(unverifiedMessage.message)
          )
      }
    } else {
      logger.info(
        s"Got ${shortType(unverifiedMessage.message)} message from ${unverifiedMessage.from} which is not in active membership, dropping"
      )
    }
  }
}
