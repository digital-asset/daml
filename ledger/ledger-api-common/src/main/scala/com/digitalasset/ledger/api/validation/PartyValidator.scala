// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.lf.data.Ref.Party
import com.daml.platform.server.api.validation.ErrorFactories.invalidArgument
import com.daml.platform.server.api.validation.FieldValidations.requireParties
import io.grpc.StatusRuntimeException

class PartyValidator(partyNameChecker: PartyNameChecker) {
  type Result[X] = Either[StatusRuntimeException, X]

  def requireKnownParties(parties: Traversable[String]): Result[Set[Party]] =
    for {
      ps <- requireParties(parties.toSet)
      knownParties <- requireKnownParties(ps)
    } yield (knownParties)

  private def requireKnownParties(partiesInRequest: Set[Party]): Result[Set[Party]] = {
    val unknownParties = partiesInRequest.filterNot(partyNameChecker.isKnownParty)
    if (unknownParties.nonEmpty)
      Left(invalidArgument(s"Unknown parties: ${unknownParties.mkString("[", ", ", "]")}"))
    else Right(partiesInRequest)
  }
}
