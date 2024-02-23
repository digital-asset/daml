// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset

object ApiConversions {

  def toV1(participantOffset: ParticipantOffset): LedgerOffset =
    participantOffset.value match {
      case ParticipantOffset.Value.Empty =>
        LedgerOffset.of(LedgerOffset.Value.Empty)
      case ParticipantOffset.Value.Absolute(absoluteString) =>
        LedgerOffset.of(LedgerOffset.Value.Absolute(absoluteString))
      case ParticipantOffset.Value.Boundary(
            ParticipantOffset.ParticipantBoundary.PARTICIPANT_BEGIN
          ) =>
        LedgerOffset.of(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN))
      case ParticipantOffset.Value.Boundary(
            ParticipantOffset.ParticipantBoundary.PARTICIPANT_END
          ) =>
        LedgerOffset.of(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))
      case ParticipantOffset.Value.Boundary(
            ParticipantOffset.ParticipantBoundary.Unrecognized(value)
          ) =>
        LedgerOffset.of(
          LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.Unrecognized(value))
        )
    }

  def toV2(ledgerOffset: LedgerOffset): ParticipantOffset = ledgerOffset.value match {
    case LedgerOffset.Value.Empty => ParticipantOffset.of(ParticipantOffset.Value.Empty)
    case LedgerOffset.Value.Absolute(value) =>
      ParticipantOffset.of(ParticipantOffset.Value.Absolute(value))
    case LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN) =>
      ParticipantOffset.of(
        ParticipantOffset.Value.Boundary(ParticipantOffset.ParticipantBoundary.PARTICIPANT_BEGIN)
      )
    case LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END) =>
      ParticipantOffset.of(
        ParticipantOffset.Value.Boundary(ParticipantOffset.ParticipantBoundary.PARTICIPANT_END)
      )
    case LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.Unrecognized(value)) => {
      ParticipantOffset.of(
        ParticipantOffset.Value.Boundary(
          ParticipantOffset.ParticipantBoundary.Unrecognized(value)
        )
      )
    }
  }

}
