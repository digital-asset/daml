// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pretty

import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset.ParticipantBoundary
import com.daml.ledger.javaapi.data.codegen.ContractId
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.v2.ChangeId
import com.digitalasset.canton.logging.pretty.Pretty
import pprint.Tree

object Implicits {
  import com.digitalasset.canton.logging.pretty.Pretty.*

  implicit val prettyReadServiceOffset: Pretty[Offset] = prettyOfString(
    // Do not use `toReadableHash` because this is not a hash but a hex-encoded string
    // whose end contains the most important information
    _.toHexString
  )

  implicit val prettyLedgerBoundary: Pretty[ParticipantBoundary] = {
    case ParticipantBoundary.PARTICIPANT_BOUNDARY_BEGIN =>
      Tree.Literal("PARTICIPANT_BOUNDARY_BEGIN")
    case ParticipantBoundary.PARTICIPANT_BOUNDARY_END => Tree.Literal("PARTICIPANT_BOUNDARY_END")
    case ParticipantBoundary.Unrecognized(value) => Tree.Literal(s"Unrecognized($value)")
  }

  implicit val prettyLedgerOffset: Pretty[ParticipantOffset] = {
    case ParticipantOffset(ParticipantOffset.Value.Absolute(absolute)) =>
      Tree.Apply("AbsoluteOffset", Iterator(Tree.Literal(absolute)))
    case ParticipantOffset(ParticipantOffset.Value.Boundary(boundary)) =>
      Tree.Apply("Boundary", Iterator(boundary.toTree))
    case ParticipantOffset(ParticipantOffset.Value.Empty) => Tree.Literal("Empty")
  }

  implicit def prettyChangeId: Pretty[ChangeId] = prettyOfClass(
    param("application Id", _.applicationId),
    param("command Id", _.commandId),
    param("act as", _.actAs),
  )

  implicit def prettyContractId: Pretty[ContractId[_]] = prettyOfString { coid =>
    val coidStr = coid.contractId
    val tokens = coidStr.split(':')
    if (tokens.lengthCompare(2) == 0) {
      tokens(0).readableHash.toString + ":" + tokens(1).readableHash.toString
    } else {
      // Don't abbreviate anything for unusual contract ids
      coidStr
    }
  }
}
