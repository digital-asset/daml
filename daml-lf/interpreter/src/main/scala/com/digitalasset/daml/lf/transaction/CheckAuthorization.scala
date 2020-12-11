// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.ledger.Authorize
import com.daml.lf.ledger.FailedAuthorization
import com.daml.lf.transaction.Node.{NodeCreate, NodeFetch, NodeLookupByKey}

import PartialTransaction.ExercisesContext

private[lf] object CheckAuthorization {

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

  private[lf] def authorizeCreate(create: NodeCreate[_])(
      auth: Authorize,
  ): List[FailedAuthorization] = {
    authorize(
      passIf = create.signatories subsetOf auth.authParties,
      failWith = FailedAuthorization.CreateMissingAuthorization(
        templateId = create.coinst.template,
        optLocation = create.optLocation,
        authorizingParties = auth.authParties,
        requiredParties = create.signatories,
      ),
    ) ++
      authorize(
        passIf = create.signatories.nonEmpty,
        failWith = FailedAuthorization.NoSignatories(create.coinst.template, create.optLocation),
      ) ++
      (create.key match {
        case None => List()
        case Some(key) =>
          val maintainers = key.maintainers
          authorize(
            passIf = maintainers subsetOf create.signatories,
            failWith = FailedAuthorization.MaintainersNotSubsetOfSignatories(
              templateId = create.coinst.template,
              optLocation = create.optLocation,
              signatories = create.signatories,
              maintainers = maintainers,
            ),
          )
      })
  }

  private[lf] def authorizeFetch(fetch: NodeFetch[_])(
      auth: Authorize
  ): List[FailedAuthorization] = {
    authorize(
      passIf = fetch.stakeholders.intersect(auth.authParties).nonEmpty,
      failWith = FailedAuthorization.FetchMissingAuthorization(
        templateId = fetch.templateId,
        optLocation = fetch.optLocation,
        stakeholders = fetch.stakeholders,
        authorizingParties = auth.authParties,
      )
    )
  }

  private[lf] def authorizeLookupByKey(lbk: NodeLookupByKey[_])(
      auth: Authorize,
  ): List[FailedAuthorization] = {
    authorize(
      passIf = lbk.key.maintainers subsetOf auth.authParties,
      failWith = FailedAuthorization.LookupByKeyMissingAuthorization(
        lbk.templateId,
        lbk.optLocation,
        lbk.key.maintainers,
        auth.authParties,
      )
    )
  }

  private[lf] def authorizeExercise(ex: ExercisesContext)(
      auth: Authorize,
  ): List[FailedAuthorization] = {
    authorize(
      passIf = ex.actingParties.nonEmpty,
      failWith = FailedAuthorization.NoControllers(ex.templateId, ex.choiceId, ex.optLocation),
    ) ++
      authorize(
        passIf = ex.actingParties subsetOf auth.authParties,
        failWith = FailedAuthorization.ExerciseMissingAuthorization(
          templateId = ex.templateId,
          choiceId = ex.choiceId,
          optLocation = ex.optLocation,
          authorizingParties = auth.authParties,
          requiredParties = ex.actingParties,
        ),
      )
  }

}
