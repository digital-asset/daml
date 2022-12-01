// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref.Location
import com.daml.lf.ledger.Authorize
import com.daml.lf.ledger.FailedAuthorization
import com.daml.lf.transaction.Node

trait AuthorizationChecker {

  private[lf] def authorizeCreate(
      optLocation: Option[Location],
      create: Node.Create,
  )(
      auth: Authorize
  ): List[FailedAuthorization]

  private[lf] def authorizeFetch(
      optLocation: Option[Location],
      fetch: Node.Fetch,
  )(
      auth: Authorize
  ): List[FailedAuthorization]

  private[lf] def authorizeLookupByKey(
      optLocation: Option[Location],
      lbk: Node.LookupByKey,
  )(
      auth: Authorize
  ): List[FailedAuthorization]

  private[lf] def authorizeExercise(
      optLocation: Option[Location],
      ex: Node.Exercise,
  )(
      auth: Authorize
  ): List[FailedAuthorization]
}

/** Default AuthorizationChecker intended for production usage. */
private[lf] object DefaultAuthorizationChecker extends AuthorizationChecker {

  @inline
  private[this] def authorize(
      passIf: Boolean,
      failWith: => FailedAuthorization,
  ): List[FailedAuthorization] = {
    if (passIf)
      List()
    else
      List(failWith)
  }

  override private[lf] def authorizeCreate(
      optLocation: Option[Location],
      create: Node.Create,
  )(
      auth: Authorize
  ): List[FailedAuthorization] = {
    authorize(
      passIf = create.signatories subsetOf auth.authParties,
      failWith = FailedAuthorization.CreateMissingAuthorization(
        templateId = create.templateId,
        optLocation = optLocation,
        authorizingParties = auth.authParties,
        requiredParties = create.signatories,
      ),
    ) ++
      authorize(
        passIf = create.signatories.nonEmpty,
        failWith = FailedAuthorization.NoSignatories(create.templateId, optLocation),
      ) ++
      (create.key match {
        case None => List()
        case Some(key) =>
          val maintainers = key.maintainers
          authorize(
            passIf = maintainers subsetOf create.signatories,
            failWith = FailedAuthorization.MaintainersNotSubsetOfSignatories(
              templateId = create.templateId,
              optLocation = optLocation,
              signatories = create.signatories,
              maintainers = maintainers,
            ),
          )
      })
  }

  override private[lf] def authorizeFetch(
      optLocation: Option[Location],
      fetch: Node.Fetch,
  )(
      auth: Authorize
  ): List[FailedAuthorization] = {
    authorize(
      passIf = fetch.stakeholders.intersect(auth.authParties).nonEmpty,
      failWith = FailedAuthorization.FetchMissingAuthorization(
        templateId = fetch.templateId,
        optLocation = optLocation,
        stakeholders = fetch.stakeholders,
        authorizingParties = auth.authParties,
      ),
    )
  }

  override private[lf] def authorizeLookupByKey(
      optLocation: Option[Location],
      lbk: Node.LookupByKey,
  )(
      auth: Authorize
  ): List[FailedAuthorization] = {
    authorize(
      passIf = lbk.key.maintainers subsetOf auth.authParties,
      failWith = FailedAuthorization.LookupByKeyMissingAuthorization(
        lbk.templateId,
        optLocation,
        lbk.key.maintainers,
        auth.authParties,
      ),
    )
  }

  override private[lf] def authorizeExercise(
      optLocation: Option[Location],
      ex: Node.Exercise,
  )(
      auth: Authorize
  ): List[FailedAuthorization] = {
    authorize(
      passIf = ex.actingParties.nonEmpty,
      failWith = FailedAuthorization.NoControllers(ex.templateId, ex.choiceId, optLocation),
    ) ++
      authorize(
        passIf = ex.actingParties subsetOf auth.authParties,
        failWith = FailedAuthorization.ExerciseMissingAuthorization(
          templateId = ex.templateId,
          choiceId = ex.choiceId,
          optLocation = optLocation,
          authorizingParties = auth.authParties,
          requiredParties = ex.actingParties,
        ),
      )
  }

}

/** Dummy authorization checker that does not check anything. Should only be used for testing. */
object NoopAuthorizationChecker extends AuthorizationChecker {
  override private[lf] def authorizeCreate(optLocation: Option[Location], create: Node.Create)(
      auth: Authorize
  ): List[FailedAuthorization] = Nil

  override private[lf] def authorizeFetch(optLocation: Option[Location], fetch: Node.Fetch)(
      auth: Authorize
  ): List[FailedAuthorization] = Nil

  override private[lf] def authorizeLookupByKey(
      optLocation: Option[Location],
      lbk: Node.LookupByKey,
  )(auth: Authorize): List[FailedAuthorization] = Nil

  override private[lf] def authorizeExercise(optLocation: Option[Location], ex: Node.Exercise)(
      auth: Authorize
  ): List[FailedAuthorization] = Nil
}
