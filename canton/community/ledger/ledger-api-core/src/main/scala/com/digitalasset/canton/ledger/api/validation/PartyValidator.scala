// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.lf.data.Ref.Party
import io.grpc.StatusRuntimeException

class PartyValidator(
    partyNameChecker: PartyNameChecker
) {
  type Result[X] = Either[StatusRuntimeException, X]

  import ValidationErrors.invalidArgument
  import FieldValidator.requireParties

  def requireKnownParties(
      parties: Iterable[String]
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Result[Set[Party]] =
    for {
      ps <- requireParties(parties.toSet)
      knownParties <- requireKnownParties(ps)
    } yield knownParties

  private def requireKnownParties(
      partiesInRequest: Set[Party]
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Result[Set[Party]] = {
    val unknownParties = partiesInRequest.filterNot(partyNameChecker.isKnownParty)
    if (unknownParties.nonEmpty)
      Left(invalidArgument(s"Unknown parties: ${unknownParties.mkString("[", ", ", "]")}"))
    else Right(partiesInRequest)
  }
}
