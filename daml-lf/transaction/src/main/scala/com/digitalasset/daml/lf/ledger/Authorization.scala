// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.ledger

import com.daml.lf.data.Ref.{ChoiceName, Identifier, Location, Party}

/** Authorize the transaction using the provided parties as initial authorizers for the dynamic authorization. */
final case class Authorize(authParties: Set[Party])

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
