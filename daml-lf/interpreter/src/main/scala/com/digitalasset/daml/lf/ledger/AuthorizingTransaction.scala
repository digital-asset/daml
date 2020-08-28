// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.ledger

import com.daml.lf.transaction.Node.{NodeCreate, NodeExercises, NodeFetch, NodeLookupByKey}
import com.daml.lf.transaction.{NodeId, Transaction => Tx}
import com.daml.lf.value.Value.ContractId

object AuthorizingTransaction {

  private object CheckState {
    def Empty =
      CheckState(Map.empty)
  }

  /** State to use whilst checking for failed authorizations. */
  private final case class CheckState(
      failedAuthorizations: FailedAuthorizations,
  ) {

    def authorize(
        nodeId: NodeId,
        passIf: Boolean,
        failWith: FailedAuthorization,
    ): CheckState =
      if (passIf ||
        failedAuthorizations.contains(nodeId) /* already failed? keep the first one */
        )
        this
      else
        copy(failedAuthorizations = failedAuthorizations + (nodeId -> failWith))

    def authorizeCreate(
        nodeId: NodeId,
        create: NodeCreate.WithTxValue[ContractId],
        auth: Authorize,
    ): CheckState = {
      val state = this
        .authorize(
          nodeId = nodeId,
          passIf = create.signatories subsetOf auth.authParties,
          failWith = FailedAuthorization.CreateMissingAuthorization(
            templateId = create.coinst.template,
            optLocation = create.optLocation,
            authorizingParties = auth.authParties,
            requiredParties = create.signatories,
          ),
        )
        .authorize(
          nodeId = nodeId,
          passIf = create.signatories.nonEmpty,
          failWith = FailedAuthorization.NoSignatories(create.coinst.template, create.optLocation),
        )
      create.key match {
        case None => state
        case Some(key) =>
          val maintainers = key.maintainers
          state.authorize(
            nodeId = nodeId,
            passIf = maintainers subsetOf create.signatories,
            failWith = FailedAuthorization.MaintainersNotSubsetOfSignatories(
              templateId = create.coinst.template,
              optLocation = create.optLocation,
              signatories = create.signatories,
              maintainers = maintainers,
            ),
          )
      }

    }

    def authorizeExercise(
        nodeId: NodeId,
        ex: NodeExercises.WithTxValue[NodeId, ContractId],
        auth: Authorize,
    ): CheckState =
      this
        .authorize(
          nodeId = nodeId,
          passIf = ex.actingParties.nonEmpty,
          failWith = FailedAuthorization.NoControllers(ex.templateId, ex.choiceId, ex.optLocation),
        )
        .authorize(
          nodeId = nodeId,
          passIf = !ex.controllersDifferFromActors,
          failWith = FailedAuthorization.ActorMismatch(
            templateId = ex.templateId,
            choiceId = ex.choiceId,
            optLocation = ex.optLocation,
            givenActors = ex.actingParties,
          ),
        )
        .authorize(
          nodeId = nodeId,
          passIf = ex.actingParties subsetOf auth.authParties,
          failWith = FailedAuthorization.ExerciseMissingAuthorization(
            templateId = ex.templateId,
            choiceId = ex.choiceId,
            optLocation = ex.optLocation,
            authorizingParties = auth.authParties,
            requiredParties = ex.actingParties,
          ),
        )

    def authorizeFetch(
        nodeId: NodeId,
        fetch: NodeFetch.WithTxValue[ContractId],
        auth: Authorize,
    ): CheckState =
      this.authorize(
        nodeId = nodeId,
        passIf = fetch.stakeholders.intersect(auth.authParties).nonEmpty,
        failWith = FailedAuthorization.FetchMissingAuthorization(
          templateId = fetch.templateId,
          optLocation = fetch.optLocation,
          stakeholders = fetch.stakeholders,
          authorizingParties = auth.authParties,
        )
      )

    def authorizeLookupByKey(
        nodeId: NodeId,
        lbk: NodeLookupByKey.WithTxValue[ContractId],
        auth: Authorize,
    ): CheckState =
      this.authorize(
        nodeId = nodeId,
        passIf = lbk.key.maintainers subsetOf auth.authParties,
        failWith = FailedAuthorization.LookupByKeyMissingAuthorization(
          lbk.templateId,
          lbk.optLocation,
          lbk.key.maintainers,
          auth.authParties,
        )
      )
  }

  /** Check a transaction for failed authorizations */
  def checkAuthFailures(
      auth: Authorize,
      tx: Tx.Transaction,
  ): FailedAuthorizations = {

    def enrichNode(
        state: CheckState,
        auth: Authorize,
        nodeId: NodeId,
    ): CheckState = {
      val node =
        tx.nodes
          .getOrElse(
            nodeId,
            throw new IllegalArgumentException(
              s"enrichNode - precondition violated: node $nodeId not present"))
      node match {
        case create: NodeCreate.WithTxValue[ContractId] =>
          state
            .authorizeCreate(nodeId, create, auth)

        case fetch: NodeFetch.WithTxValue[ContractId] =>
          state
            .authorizeFetch(nodeId, fetch, auth)

        case ex: NodeExercises.WithTxValue[NodeId, ContractId] =>
          val state1 =
            state
              .authorizeExercise(nodeId, ex, auth)

          ex.children.foldLeft(state1) { (s, childNodeId) =>
            enrichNode(
              s,
              Authorize(ex.actingParties union ex.signatories),
              childNodeId,
            )
          }

        case nlbk: NodeLookupByKey.WithTxValue[ContractId] =>
          state
            .authorizeLookupByKey(nodeId, nlbk, auth)
      }
    }

    val finalState =
      tx.transaction.roots.foldLeft(CheckState.Empty) { (s, nodeId) =>
        enrichNode(s, auth, nodeId)
      }

    finalState.failedAuthorizations
  }

}
