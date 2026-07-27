// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.error.MediatorError.{
  DuplicateConfirmationRequest,
  InvalidMessage,
  MalformedMessage,
  Timeout,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.{NonPositiveLocalVerdict, Verdict}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.version.ProtocolVersion
import pprint.Tree

sealed trait MediatorVerdict extends Product with Serializable with PrettyPrinting {
  def toVerdict(protocolVersion: ProtocolVersion): Verdict
}

object MediatorVerdict {
  case object MediatorApprove extends MediatorVerdict {
    override def toVerdict(protocolVersion: ProtocolVersion): Verdict =
      Verdict.Approve(protocolVersion)

    override protected def pretty: Pretty[MediatorApprove] = prettyOfObject[MediatorApprove]
  }
  type MediatorApprove = MediatorApprove.type

  final case class ParticipantReject(
      reasons: NonEmpty[List[(Set[LfPartyId], ParticipantId, NonPositiveLocalVerdict)]]
  ) extends MediatorVerdict {
    override def toVerdict(protocolVersion: ProtocolVersion): Verdict =
      Verdict.ParticipantReject(reasons, protocolVersion)

    override protected def pretty: Pretty[ParticipantReject] = {
      import Pretty.PrettyOps

      prettyOfClass(
        unnamedParam(
          _.reasons.map { case (parties, participantId, reason) =>
            Tree.Infix(reason.toTree, s"- reported by $participantId for:", parties.toTree)
          }
        )
      )
    }
  }

  final case class MediatorReject(reason: MediatorError) extends MediatorVerdict {
    override def toVerdict(protocolVersion: ProtocolVersion): Verdict.MediatorReject = {
      val error = reason match {
        case timeout: Timeout.Reject => timeout
        case invalid: InvalidMessage.Reject => invalid
        case malformed: MalformedMessage.Reject => malformed
        case duplicate: DuplicateConfirmationRequest.Reject => duplicate
      }

      Verdict.MediatorReject.tryCreate(
        error.rpcStatusWithoutLoggingContext(),
        reason.isMalformed,
        protocolVersion,
      )
    }

    override protected def pretty: Pretty[MediatorReject] = prettyOfClass(
      param("reason", _.reason)
    )
  }
}
