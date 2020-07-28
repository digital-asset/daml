// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.ledger

import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation.Relation
import com.daml.lf.transaction.Node.{NodeCreate, NodeExercises, NodeFetch, NodeLookupByKey}
import com.daml.lf.transaction.{NodeId, Transaction => Tx}
import com.daml.lf.value.Value.ContractId

object EnrichedTransaction {

  private object EnrichState {
    def Empty =
      EnrichState(Map.empty, Map.empty, Map.empty)
  }

  /** State to use during enriching a transaction with disclosure information. */
  private final case class EnrichState(
      disclosures: Relation[NodeId, Party],
      divulgences: Relation[ContractId, Party],
      failedAuthorizations: Map[NodeId, FailedAuthorization],
  ) {

    def discloseNode(
        parentWitnesses: Set[Party],
        nid: NodeId,
        node: Tx.Node,
    ): (Set[Party], EnrichState) = {
      val witnesses = parentWitnesses union node.informeesOfNode
      witnesses ->
        copy(
          disclosures = disclosures
            .updated(nid, witnesses union disclosures.getOrElse(nid, Set.empty)),
        )
    }

    def divulgeCoidTo(witnesses: Set[Party], acoid: ContractId): EnrichState = {
      copy(
        divulgences = divulgences
          .updated(acoid, witnesses union divulgences.getOrElse(acoid, Set.empty)),
      )
    }

    def authorize(
        nodeId: NodeId,
        passIf: Boolean,
        failWith: FailedAuthorization,
    ): EnrichState =
      if (passIf ||
        failedAuthorizations.contains(nodeId) /* already failed? keep the first one */
        )
        this
      else
        copy(failedAuthorizations = failedAuthorizations + (nodeId -> failWith))

    /**
      *
      * @param mbMaintainers If the create has a key, these are the maintainers
      */
    def authorizeCreate(
        nodeId: NodeId,
        create: NodeCreate.WithTxValue[ContractId],
        signatories: Set[Party],
        authorization: Authorization,
        mbMaintainers: Option[Set[Party]],
    ): EnrichState =
      authorization.fold(this)(
        authParties => {
          val auth = this
            .authorize(
              nodeId = nodeId,
              passIf = signatories subsetOf authParties,
              failWith = FailedAuthorization.CreateMissingAuthorization(
                templateId = create.coinst.template,
                optLocation = create.optLocation,
                authorizingParties = authParties,
                requiredParties = signatories,
              ),
            )
            .authorize(
              nodeId = nodeId,
              passIf = signatories.nonEmpty,
              failWith =
                FailedAuthorization.NoSignatories(create.coinst.template, create.optLocation),
            )
          mbMaintainers match {
            case None => auth
            case Some(maintainers) =>
              auth.authorize(
                nodeId = nodeId,
                passIf = maintainers subsetOf signatories,
                failWith = FailedAuthorization.MaintainersNotSubsetOfSignatories(
                  templateId = create.coinst.template,
                  optLocation = create.optLocation,
                  signatories = signatories,
                  maintainers = maintainers,
                ),
              )
          }
        })

    def authorizeExercise(
        nodeId: NodeId,
        ex: NodeExercises.WithTxValue[NodeId, ContractId],
        actingParties: Set[Party],
        authorization: Authorization,
        controllersDifferFromActors: Boolean,
    ): EnrichState = {
      // well-authorized by A : actors == controllers(c)
      //                        && actors subsetOf A
      //                        && childrenActions well-authorized by
      //                           (signatories(c) union controllers(c))

      authorization.fold(this)(
        authParties =>
          this
            .authorize(
              nodeId = nodeId,
              passIf = actingParties.nonEmpty,
              failWith =
                FailedAuthorization.NoControllers(ex.templateId, ex.choiceId, ex.optLocation),
            )
            .authorize(
              nodeId = nodeId,
              passIf = !controllersDifferFromActors,
              failWith = FailedAuthorization.ActorMismatch(
                templateId = ex.templateId,
                choiceId = ex.choiceId,
                optLocation = ex.optLocation,
                givenActors = actingParties,
              ),
            )
            .authorize(
              nodeId = nodeId,
              passIf = actingParties subsetOf authParties,
              failWith = FailedAuthorization.ExerciseMissingAuthorization(
                templateId = ex.templateId,
                choiceId = ex.choiceId,
                optLocation = ex.optLocation,
                authorizingParties = authParties,
                requiredParties = actingParties,
              ),
          ))
    }

    def authorizeFetch(
        nodeId: NodeId,
        fetch: NodeFetch.WithTxValue[ContractId],
        stakeholders: Set[Party],
        authorization: Authorization,
    ): EnrichState = {
      authorization.fold(this)(
        authParties =>
          this.authorize(
            nodeId = nodeId,
            passIf = stakeholders.intersect(authParties).nonEmpty,
            failWith = FailedAuthorization.FetchMissingAuthorization(
              templateId = fetch.templateId,
              optLocation = fetch.optLocation,
              stakeholders = stakeholders,
              authorizingParties = authParties,
            )
        ))
    }

    /*
      If we have `authorizers` and lookup node with maintainers
      `maintainers`, we have three options:

      1. Not authorize at all (always accept the lookup node);

         - Not good because it allows you to guess what keys exist, and thus
           leaks information about what contract ids are active to
           non-stakeholders.

      2. `authorizers ∩ maintainers ≠ ∅`, at least one.

         - This is a stricter condition compared to fetches, because with
           fetches we check that `authorizers ∩ stakeholders ≠ ∅`, and we
           know that `maintainers ⊆ stakeholders`, since `maintainers ⊆
           signatories ⊆ stakeholders`. In other words, you won't be able
           to look up a contract by key if you're an observer but not a
           signatory.

         - However, this is problematic since lookups will induce work for *all*
           maintainers even if only a subset of the maintainers have
           authorized it, violating the tenet that nobody can be forced to
           perform work.

           To make this a bit more concrete, consider the case where a
           negative lookup is the only thing that induces a validation
           request to a maintainer who would have received the transaction
           to validate otherwise.

      3. `authorizers ⊇ maintainers`, all of them.

         - This seems to be the only safe choice for lookups, *but* note
           that for fetches which fail if the key cannot be found we can use
           the same authorization rule we use for fetch, which is much more
           lenient. The way we achieve this is that we have two DAML-LF
           primitives, `fetchByKey` and `lookupByKey`, with the former
           emitting a normal fetch node, and the latter emitting a lookup
           node.

           The reason why we have a full-blown lookup node rather than a
           simple "key does not exist" node is so that the transaction
           structure is stable with what regards wrong results coming from
           the key oracle, which will happen when the user requests a key
           for a contract that is not available locally but is available
           elsewhere.

           From a more high level perspective, we want to make the
           authorization checks orthogonal to DAML-LF interpretation, which
           would not be the case if we added a "key does not exist" node as
           described above.

         - Observation by Andreas: don't we end up in the same situation if
           we have a malicious submitter node that omits the authorization
           check? For example, it could craft transactions which involve
           arbitrary parties which then will have to perform work in
           re-interpreting the transaction.

           Francesco: yes, but there is a key difference: the above scenario
           requires a malicious (or at the very least negligent / defective) *participant*,
           while in this case we are talking about malicious *code* being
           able to induce work. So the "threat model" is quite different.

      To be able to make a statement of non-existence of a key, it's clear
      that we must authorize against the maintainers, and not the
      stakeholders, since there are no observers to speak of.

      On the other hand, when making a positive statement, we can use the
      same authorization rule that we use for fetch -- that is, we check
      that `authorizers ∩ stakeholders ≠ ∅`.
     */
    def authorizeLookupByKey(
        nodeId: NodeId,
        lbk: NodeLookupByKey.WithTxValue[ContractId],
        authorization: Authorization,
    ): EnrichState = {
      authorization.fold(this) { authorizers =>
        this.authorize(
          nodeId = nodeId,
          passIf = lbk.key.maintainers subsetOf authorizers,
          failWith = FailedAuthorization.LookupByKeyMissingAuthorization(
            lbk.templateId,
            lbk.optLocation,
            lbk.key.maintainers,
            authorizers,
          ),
        )
      }
    }
  }

  /** Enrich a transaction with disclosure and authorization information.
    *
    * PRE: The transaction must create contract-instances before
    * consuming them.
    *
    * @param authorization the authorization mode
    * @param tx            transaction resulting from executing the update
    *                      expression at the given effective time.
    */
  def apply(
      authorization: Authorization,
      tx: Tx.Transaction,
  ): EnrichedTransaction = {

    // Before we traversed through an exercise node the exercise witnesses
    // contain only the initial authorizers.
    val initialParentExerciseWitnesses: Set[Party] =
      authorization match {
        case DontAuthorize => Set.empty
        case Authorize(authorizers) => authorizers
      }

    def enrichNode(
        state: EnrichState,
        parentExerciseWitnesses: Set[Party],
        authorization: Authorization,
        nodeId: NodeId,
    ): EnrichState = {
      val node =
        tx.nodes
          .getOrElse(
            nodeId,
            throw new IllegalArgumentException(
              s"enrichNode - precondition violated: node $nodeId not present"))
      node match {
        case create: NodeCreate.WithTxValue[ContractId] =>
          // ------------------------------------------------------------------
          // witnesses            : stakeholders union witnesses of parent exercise
          //                        node
          // divulge              : Nothing
          // well-authorized by A : signatories subsetOf A && non-empty signatories
          // ------------------------------------------------------------------
          state
            .authorizeCreate(
              nodeId,
              create,
              signatories = create.signatories,
              authorization = authorization,
              mbMaintainers = create.key.map(_.maintainers),
            )
            .discloseNode(parentExerciseWitnesses, nodeId, create)
            ._2

        case fetch: NodeFetch.WithTxValue[ContractId] =>
          // ------------------------------------------------------------------
          // witnesses            : parent exercise witnesses
          // divulge              : referenced contract to witnesses of parent exercise node
          // well-authorized by A : A `intersect` stakeholders(fetched contract id) = non-empty
          // ------------------------------------------------------------------
          state
            .divulgeCoidTo(parentExerciseWitnesses -- fetch.stakeholders, fetch.coid)
            .discloseNode(parentExerciseWitnesses, nodeId, fetch)
            ._2
            .authorizeFetch(
              nodeId,
              fetch,
              stakeholders = fetch.stakeholders,
              authorization = authorization,
            )

        case ex: NodeExercises.WithTxValue[NodeId, ContractId] =>
          // ------------------------------------------------------------------
          // witnesses:
          //    | consuming  -> stakeholders(targetId) union witnesses of parent exercise node
          //    | non-consuming ->  signatories(targetId) union actors
          //                        union witnesses of parent exercise
          // divulge: target contract id to parent exercise witnesses.
          // well-authorized by A : actors == controllers(c)
          //                        && actors subsetOf A
          //                        && childrenActions well-authorized by
          //                           (signatories(c) union controllers(c))
          //                        && controllers non-empty
          // ------------------------------------------------------------------

          // Authorize the exercise
          val state0 =
            state.authorizeExercise(
              nodeId,
              ex,
              actingParties = ex.actingParties,
              authorization = authorization,
              controllersDifferFromActors = ex.controllersDifferFromActors,
            )

          // Then enrich and authorize the children.
          val (witnesses, state1) = state0.discloseNode(parentExerciseWitnesses, nodeId, ex)
          val state2 =
            state1.divulgeCoidTo(parentExerciseWitnesses -- ex.stakeholders, ex.targetCoid)
          ex.children.foldLeft(state2) { (s, childNodeId) =>
            enrichNode(
              s,
              witnesses,
              authorization.map(_ => ex.actingParties union ex.signatories),
              childNodeId,
            )
          }

        case nlbk: NodeLookupByKey.WithTxValue[ContractId] =>
          // ------------------------------------------------------------------
          // witnesses: parent exercise witnesses
          //
          // divulge: nothing
          //
          // well-authorized by A: maintainers subsetOf A.
          // ------------------------------------------------------------------
          state
            .authorizeLookupByKey(nodeId, nlbk, authorization)
            .discloseNode(parentExerciseWitnesses, nodeId, nlbk)
            ._2

      }
    }

    val finalState =
      tx.transaction.roots.foldLeft(EnrichState.Empty) { (s, nodeId) =>
        enrichNode(s, initialParentExerciseWitnesses, authorization, nodeId)
      }

    new EnrichedTransaction(
      tx,
      explicitDisclosure = finalState.disclosures,
      implicitDisclosure = finalState.divulgences,
      failedAuthorizations = finalState.failedAuthorizations,
    )
  }

}

final case class EnrichedTransaction(
    tx: Tx.Transaction,
    // A relation between a node id and the parties to which this node gets explicitly disclosed.
    explicitDisclosure: Relation[NodeId, Party],
    // A relation between contract id and the parties to which the contract id gets
    // explicitly disclosed.
    implicitDisclosure: Relation[ContractId, Party],
    // A map from node ids to authorizations that failed for them.
    failedAuthorizations: FailedAuthorizations,
)
