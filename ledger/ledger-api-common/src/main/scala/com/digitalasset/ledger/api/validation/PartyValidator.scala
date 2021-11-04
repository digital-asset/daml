// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.lf.data.Ref.Party
import com.daml.platform.server.api.validation.{ErrorFactories, FieldValidations}
import io.grpc.StatusRuntimeException

class PartyValidator(
    partyNameChecker: PartyNameChecker,
    errorFactories: ErrorFactories,
    fieldValidations: FieldValidations,
) {
  type Result[X] = Either[StatusRuntimeException, X]

  import errorFactories.invalidArgument
  import fieldValidations.requireParties

  // CommandCompletionService:
  //  - completionStreamSource
  // TransactionService:
  //  - getTransactionTreesSource
  //  - getTransactionsSource
  //  - getTransactionById
  //  - getFlatTransactionById
  //  - getFlatTransactionByEventId
  //  - getTransactionByEventId
  //  - getTransactionTreesSource
  def requireKnownParties(
      parties: Iterable[String]
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Result[Set[Party]] =
    for {
      ps <- requireParties(parties.toSet)
      knownParties <- requireKnownParties(ps)
    } yield knownParties

  // CommandCompletionService:
  //  - completionStreamSource
  // TransactionService:
  //  - getTransactionTreesSource
  //  - getTransactionsSource
  //  - getTransactionById
  //  - getFlatTransactionById
  //  - getFlatTransactionByEventId
  //  - getTransactionByEventId
  //  - getTransactionTreesSource
  private def requireKnownParties(
      partiesInRequest: Set[Party]
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Result[Set[Party]] = {
    val unknownParties = partiesInRequest.filterNot(partyNameChecker.isKnownParty)
    if (unknownParties.nonEmpty)
      Left(invalidArgument(None)(s"Unknown parties: ${unknownParties.mkString("[", ", ", "]")}"))
    else Right(partiesInRequest)
  }
}
