// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.ledger

import com.daml.lf.data.Ref.{ChoiceName, Identifier, Location, Party}

sealed trait Authorization {
  def fold[A](ifDontAuthorize: A)(ifAuthorize: Set[Party] => A): A =
    this match {
      case DontAuthorize => ifDontAuthorize
      case Authorize(authorizers) => ifAuthorize(authorizers)
    }

  def map(f: Set[Party] => Set[Party]): Authorization = this match {
    case DontAuthorize => DontAuthorize
    case Authorize(parties) => Authorize(f(parties))
  }
}

/** Do not authorize the transaction. If this is passed in, failedAuthorizations is guaranteed to be empty. */
case object DontAuthorize extends Authorization

/** Authorize the transaction using the provided parties as initial authorizers for the dynamic authorization. */
final case class Authorize(authorizers: Set[Party]) extends Authorization

sealed trait FailedAuthorization

object FailedAuthorization {

  final case class CreateMissingAuthorization(
      templateId: Identifier,
      optLocation: Option[Location],
      authorizingParties: Set[Party],
      requiredParties: Set[Party],
  ) extends FailedAuthorization

  final case class MaintainersNotSubsetOfSignatories(
      templateId: Identifier,
      optLocation: Option[Location],
      signatories: Set[Party],
      maintainers: Set[Party],
  ) extends FailedAuthorization

  final case class FetchMissingAuthorization(
      templateId: Identifier,
      optLocation: Option[Location],
      stakeholders: Set[Party],
      authorizingParties: Set[Party],
  ) extends FailedAuthorization

  final case class ExerciseMissingAuthorization(
      templateId: Identifier,
      choiceId: ChoiceName,
      optLocation: Option[Location],
      authorizingParties: Set[Party],
      requiredParties: Set[Party],
  ) extends FailedAuthorization

  final case class ActorMismatch(
      templateId: Identifier,
      choiceId: ChoiceName,
      optLocation: Option[Location],
      givenActors: Set[Party],
  ) extends FailedAuthorization

  final case class NoSignatories(
      templateId: Identifier,
      optLocation: Option[Location],
  ) extends FailedAuthorization

  final case class NoControllers(
      templateId: Identifier,
      choiceid: ChoiceName,
      optLocation: Option[Location],
  ) extends FailedAuthorization

  final case class LookupByKeyMissingAuthorization(
      templateId: Identifier,
      optLocation: Option[Location],
      maintainers: Set[Party],
      authorizingParties: Set[Party],
  ) extends FailedAuthorization

}
